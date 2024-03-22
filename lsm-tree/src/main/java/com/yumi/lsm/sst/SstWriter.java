package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.util.AllUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对应于 lsm tree 中的一个 sstable. 这是写入流程的视角
 */
public class SstWriter {
    // 配置文件
    private Config config;
    // sstable 对应的磁盘文件
    private File dest;
    // 数据块缓冲区 key -> val
    //private List<ByteBuffer> dataBuf;
    // 过滤器块缓冲区 prev block offset -> filter bit map
    private List<ByteBuffer> filterBuf;
    // 索引块缓冲区 index key -> prev block offset, prev block size
    private List<ByteBuffer> indexBuf;
    // prev block offset -> filter bit map
    private Map<Integer, BitsArray> blockToFilter;
    // index key -> prev block offset, prev block size
    private Index[] index;

    private FileChannel channel;


    // 数据块
    private Block dataBlock;
    // 过滤器块
    private Block filterBlock;
    // 索引块
    private Block indexBlock;

    // 前一笔数据的 key
    private byte[] prevKey;

    // 前一个数据块的起始偏移位置
    private int prevBlockOffset;
    // 前一个数据块的大小
    private int prevBlockSize;
    private ByteBuffer footBuffer;
    private ByteBuffer indexValueBuffer;
    private ByteBuffer filterKeyBuffer;


    public SstWriter(String file, Config config) {
        File dest = new File(config.getDir() + File.separator + file);

        this.config = config;
        this.dest = dest;
        try {
            this.channel = new RandomAccessFile(dest, "rw").getChannel();
        } catch (Exception e) {
            throw new RuntimeException("创建文件失败");
        }
        //this.dataBuf = new ArrayList<>();
        this.filterBuf = new ArrayList<>();
        this.indexBuf = new ArrayList<>();
        this.blockToFilter = new HashMap<>();
        this.dataBlock = new Block(config, true);
        this.filterBlock = new Block(config, false);
        this.indexBlock = new Block(config,false);
        this.prevKey = new byte[0];
        this.footBuffer = ByteBuffer.allocateDirect(config.getSstFooterSize());
        this.indexValueBuffer = ByteBuffer.allocateDirect(Integer.BYTES * 2);
        this.filterKeyBuffer = ByteBuffer.allocateDirect(Integer.BYTES);
    }

    // 完成 sstable 的全部处理流程，包括将其中的数据溢写到磁盘，并返回信息供上层的 lsm 获取缓存
    public FinishRes finish() throws IOException{
        this.refreshBlock();// 完成最后一个块的处理
        this.insertIndex(this.prevKey);// 补齐最后一个 index

        this.filterBlock.flushTo(this.filterBuf);
        this.indexBlock.flushTo(this.indexBuf);

        // 处理 footer，记录布隆过滤器块起始、大小、索引块起始、大小
        ByteBuffer byteBuffer = this.footBuffer;
        byteBuffer.clear();


        FileChannel channel = this.channel;
        int size = 0;
        try {
            int dataPosition = (int) channel.position();
            byteBuffer.putInt(dataPosition);
            for (ByteBuffer buffer : filterBuf) {
                channel.write(buffer);
                this.config.getBlockBufferPool().returnBuffer(buffer);
            }
            int filterPosition = (int) this.channel.position();

            byteBuffer.putInt(filterPosition - dataPosition);
            for (ByteBuffer buffer : indexBuf) {
                channel.write(buffer);
                this.config.getBlockBufferPool().returnBuffer(buffer);
            }
            int indexPosition = (int) this.channel.position();
            size = indexPosition;
            byteBuffer.putInt(filterPosition);
            byteBuffer.putInt(indexPosition - filterPosition);
            byteBuffer.flip();

            channel.write(byteBuffer);
            channel.force(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new FinishRes(size, this.blockToFilter, this.index);
    }


    public void append(byte[] key, byte[] value) throws IOException{
        boolean res = this.dataBlock.append(key, value);
        if (!res) {
            this.refreshBlock();
            this.insertIndex(key);
            res = this.dataBlock.append(key, value);
            if (!res) {
                throw new RuntimeException("bug");
            }
        }
        this.config.getFilter().add(key);
        this.prevKey = key;
    }

    public int size() {
        try {
            return (int) this.channel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            this.channel.close();
            this.indexBuf.clear();
            this.filterBuf.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertIndex(byte[] key) {
        byte[] indexKey = AllUtils.getSeparatorBetween(this.prevKey, key);
        ByteBuffer buffer = this.indexValueBuffer;
        buffer.clear();
        buffer.putInt(this.prevBlockOffset);
        buffer.putInt(this.prevBlockSize);
        buffer.flip();
        byte[] indexVal = new byte[Integer.BYTES * 2];
        buffer.get(indexVal);
        this.indexBlock.append(indexKey, indexVal);
        if (null == this.index) {
            this.index = new Index[1];
        } else {
            Index[] tem = this.index;
            this.index = new Index[tem.length + 1];
            System.arraycopy(tem, 0, this.index, 0, tem.length);
        }
        Index localIndex = new Index();
        localIndex.setKey(indexKey);
        localIndex.setPrevBlockOffset(this.prevBlockOffset);
        localIndex.setPrevBlockSize(this.prevBlockSize);
        this.index[this.index.length - 1] = localIndex;
    }

    private void refreshBlock() throws IOException {
        if (this.config.getFilter().keyLen() == 0) {
            return;
        }

        this.prevBlockOffset = (int) this.channel.position();
        BitsArray filterBitmap = this.config.getFilter().hash();
        this.blockToFilter.put(this.prevBlockOffset, filterBitmap);

        ByteBuffer buffer = this.filterKeyBuffer;
        buffer.clear();
        buffer.putInt(this.prevBlockOffset);
        buffer.flip();
        byte[] filterKey = new byte[Integer.BYTES];
        buffer.get(filterKey);
        this.filterBlock.append(filterKey, filterBitmap.bytes());

        //重置布隆过滤器
        this.config.getFilter().reset();
        // 将 block 的数据添加到缓冲区
        this.prevBlockSize = this.dataBlock.flushTo(this.channel);
    }


    public static class FinishRes {
        private int size;
        private Map<Integer, BitsArray> blockToFilter;
        private Index[] index;

        public FinishRes(int size, Map<Integer, BitsArray> blockToFilter, Index[] index) {
            this.size = size;
            this.blockToFilter = blockToFilter;
            this.index = index;
        }

        public int getSize() {
            return size;
        }


        public Map<Integer, BitsArray> getBlockToFilter() {
            return blockToFilter;
        }

        public Index[] getIndex() {
            return index;
        }

    }
}
