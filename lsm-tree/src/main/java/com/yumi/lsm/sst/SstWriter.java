package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.util.AllUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
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
    private ByteArrayOutputStream dataBuf;
    // 过滤器块缓冲区 prev block offset -> filter bit map
    private ByteArrayOutputStream filterBuf;
    // 索引块缓冲区 index key -> prev block offset, prev block size
    private ByteArrayOutputStream indexBuf;
    // prev block offset -> filter bit map
    private Map<Integer, BitsArray> blockToFilter;
    // index key -> prev block offset, prev block size
    private Index[] index;


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

    private int blockRefreshCnt = 0;


    public SstWriter(String file, Config config) {
        File dest = new File(config.getDir() + File.separator + file);

        this.config = config;
        this.dest = dest;
        this.dataBuf = new ByteArrayOutputStream();
        this.filterBuf = new ByteArrayOutputStream();
        this.indexBuf = new ByteArrayOutputStream();
        this.blockToFilter = new HashMap<>();
        this.dataBlock = new Block(config);
        this.filterBlock = new Block(config);
        this.indexBlock = new Block(config);
        this.prevKey = new byte[0];
    }

    // 完成 sstable 的全部处理流程，包括将其中的数据溢写到磁盘，并返回信息供上层的 lsm 获取缓存
    public FinishRes finish() {
        this.refreshBlock();// 完成最后一个块的处理
        this.insertIndex(this.prevKey);// 补齐最后一个 index

        this.filterBlock.flushTo(this.filterBuf);
        this.indexBlock.flushTo(this.indexBuf);

        // 处理 footer，记录布隆过滤器块起始、大小、索引块起始、大小
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.config.getSstFooterSize());
        byteBuffer.putInt(this.dataBuf.size());
        byteBuffer.putInt(this.filterBuf.size());
        int size = this.dataBuf.size() + this.filterBuf.size();
        byteBuffer.putInt(size);
        byteBuffer.putInt(this.indexBuf.size());
        BufferedOutputStream bfo = null;
        try {
            bfo = new BufferedOutputStream(Files.newOutputStream(this.dest.toPath()));

            bfo.write(this.dataBuf.toByteArray());
            bfo.write(this.filterBuf.toByteArray());
            bfo.write(this.indexBuf.toByteArray());
            bfo.write(byteBuffer.array());

            bfo.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != bfo) {
                try {
                    bfo.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return new FinishRes(size, this.blockToFilter, this.index);
    }


    public void append(byte[] key, byte[] value) {
        if (this.dataBlock.getEntriesCnt() == 0 && this.blockRefreshCnt > 0) {
            this.insertIndex(key);
        }
        this.dataBlock.append(key, value);
        this.config.getFilter().add(key);
        this.prevKey = key;

        if (this.dataBlock.size() >= this.config.getSstDataBlockSize()) {
            this.refreshBlock();
        }
    }

    public int size() {
        return this.dataBuf.size();
    }

    public void close() {
        try {
            this.dataBuf.close();
            this.indexBuf.close();
            this.filterBuf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertIndex(byte[] key) {
        byte[] indexKey = AllUtils.getSeparatorBetween(this.prevKey, key);
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * 2);
        buffer.putInt(this.prevBlockOffset);
        buffer.putInt(this.prevBlockSize);
        this.indexBlock.append(indexKey, buffer.array());
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

    private void refreshBlock() {
        if (this.config.getFilter().keyLen() == 0) {
            return;
        }

        this.prevBlockOffset = this.dataBuf.size();
        BitsArray filterBitmap = this.config.getFilter().hash();
        this.blockToFilter.put(this.prevBlockOffset, filterBitmap);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(this.prevBlockOffset);
        this.filterBlock.append(buffer.array(), filterBitmap.bytes());

        //重置布隆过滤器
        this.config.getFilter().reset();
        // 将 block 的数据添加到缓冲区
        this.prevBlockSize = this.dataBlock.flushTo(this.dataBuf);
        this.blockRefreshCnt++;
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
