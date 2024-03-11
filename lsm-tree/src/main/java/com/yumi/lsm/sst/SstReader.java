package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.bloom.BitsArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SstReader {
    // 配置文件
    private Config conf;
    // 对应的文件
    private RandomAccessFile src;
    // 读取文件的 reader
    private MappedByteBuffer reader;
    // 过滤器块起始位置在 sstable 的 offset
    private int filterOffset;
    // 过滤器块的大小，单位 byte
    private int filterSize;
    // 索引块起始位置在 sstable 的 offset
    private int indexOffset;
    // 索引块的大小，单位 byte
    private int indexSize;

    public SstReader (String file, Config conf) {
        File jFile = new File(conf.getDir() + File.separator + file);
        if (!jFile.exists()) {
            throw new IllegalStateException("文件不存在 " + file);
        }

        this.conf = conf;
        try {
            this.src = new RandomAccessFile(jFile, "r");
            this.reader = this.src.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, this.src.length());
        } catch (FileNotFoundException e) {
            IllegalStateException illegalStateException = new IllegalStateException("文件不存在");
            illegalStateException.addSuppressed(e);
            throw illegalStateException;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int size() {
        if (this.indexOffset == 0) {
            this.readFooter();
        }
        return this.indexOffset + this.indexSize;
    }

    public void close() {
        try {
            this.src.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void readFooter() {
        ByteBuffer slice = this.reader.slice();
        try {
            long length = this.src.length();
            slice.position((int)length - this.conf.getSstFooterSize());
            slice.limit((int)length);
            this.filterOffset = slice.getInt();
            this.filterSize = slice.getInt();
            this.indexOffset = slice.getInt();
            this.indexSize = slice.getInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Integer, BitsArray> readFilter() {
        if (this.filterOffset == 0 || this.filterSize == 0) {
            this.readFooter();
        }
        ByteBuffer byteBuffer = this.readBlock(this.filterOffset, this.filterSize);
        return bufferToFilter(byteBuffer);
    }

    private Map<Integer, BitsArray> bufferToFilter(ByteBuffer buffer) {
        Map<Integer, BitsArray> blockToFilter = new HashMap<>();
        byte[] prevKey = new byte[0];
        while (buffer.hasRemaining()) {
            Kv kv = this.readRecord(prevKey, buffer);
            Integer key = ByteBuffer.wrap(kv.getKey()).getInt();
            blockToFilter.put(key, BitsArray.create(kv.getValue()));
            prevKey = kv.getKey();
        }
        return blockToFilter;
    }

    public Index[] readIndex() {
        if (this.indexOffset == 0 || this.indexSize == 0) {
            this.readFooter();
        }
        ByteBuffer byteBuffer = this.readBlock(this.indexOffset, this.indexSize);
        return bufferToIndex(byteBuffer);
    }

    private Index[] bufferToIndex(ByteBuffer buffer) {
        List<Index> indexLst = new ArrayList<>();
        byte[] prevKey = new byte[0];
        while (buffer.hasRemaining()) {
            Kv kv = this.readRecord(prevKey, buffer);
            ByteBuffer buf = ByteBuffer.wrap(kv.getValue());
            int blockOffset = buf.getInt();
            int blockSize = buf.getInt();
            Index i = new Index();
            i.setKey(kv.getKey());
            i.setPrevBlockOffset(blockOffset);
            i.setPrevBlockSize(blockSize);
            indexLst.add(i);
            prevKey = kv.getKey();
        }
        return indexLst.toArray(new Index[0]);
    }

    public Kv[] readData() {
        if (this.indexOffset == 0 || this.indexSize == 0 || this.filterOffset == 0 || this.filterSize == 0) {
            this.readFooter();
        }
        ByteBuffer byteBuffer = this.readBlock(0, filterOffset);
        return readBlockData(byteBuffer);
    }

    public Kv[] readBlockData(ByteBuffer buffer) {
        byte[] prevKey = new byte[0];
        List<Kv> kvList = new ArrayList<>();
        while (buffer.hasRemaining()) {
            Kv kv = this.readRecord(prevKey, buffer);
            kvList.add(kv);
            prevKey = kv.getKey();
        }
        return kvList.toArray(new Kv[0]);
    }

    public Kv readRecord(byte[] prevKey, ByteBuffer buffer) {
        int sharedPreFixLen = buffer.getInt();
        int remainKeyLen = buffer.getInt();
        int valLen = buffer.getInt();

        byte[] remainKey = new byte[remainKeyLen];
        buffer.get(remainKey);

        byte[] value = new byte[valLen];
        buffer.get(value);

        byte[] key = new byte[sharedPreFixLen + remainKeyLen];
        System.arraycopy(prevKey, 0, key, 0, sharedPreFixLen);
        System.arraycopy(remainKey, 0, key, sharedPreFixLen, remainKeyLen);
        return new Kv(key, value);
    }


    public ByteBuffer readBlock(int offset, int size) {
        ByteBuffer slice = this.reader.slice();
        slice.position(offset);
        slice.limit( offset + size);
        return slice;
    }


    public int getFilterOffset() {
        return filterOffset;
    }

    public int getFilterSize() {
        return filterSize;
    }

    public int getIndexOffset() {
        return indexOffset;
    }

    public int getIndexSize() {
        return indexSize;
    }
}
