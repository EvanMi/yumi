package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.util.AllUtils;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * sst 文件中的数据块，和索引、过滤器为一一对应关系
 */
public class Block {
    // lsm tree 配置文件
    private Config config;
    // 用于复制溢写数据的缓冲区
    private List<ByteBuffer> record;
    // kv 对数量
    private int entriesCnt;
    // 最晚一笔写入的数据的 key
    private byte[] prevKey = new byte[0];
    //data 类型的block不会进行扩容
    private boolean isData;


    public Block(Config config, boolean isData) {
        this.config = config;
        this.isData = isData;
        this.record = new ArrayList<>();
        this.record.add(this.config.getBlockBufferPool().borrowBuffer());
    }

    public int size() {
        return this.record.stream().mapToInt(Buffer::position).sum();
    }

    public boolean append(byte[] key, byte[] value) {
        ByteBuffer lastBuffer = this.record.get(this.record.size() - 1);
        int remainingBytes = lastBuffer.limit() - lastBuffer.position();
        int curBytes = Integer.BYTES * 3 + key.length + value.length;
        if (this.isData && curBytes > remainingBytes) {
            //数据模块不扩容，写不下返回false
            return false;
        }
        if (curBytes > remainingBytes) {
            //其他类型的模块自动扩容
            lastBuffer = this.config.getBlockBufferPool().borrowBuffer();
            this.record.add(lastBuffer);
        }

        int sharedPrefixLen = AllUtils.sharedPrefixLen(key, this.prevKey);
        //写入长度
        lastBuffer.putInt(sharedPrefixLen);
        lastBuffer.putInt(key.length - sharedPrefixLen);
        lastBuffer.putInt(value.length);

        //写入key和value
        lastBuffer.put(key, sharedPrefixLen, key.length - sharedPrefixLen);
        lastBuffer.put(value, 0, value.length);

        this.prevKey = key;
        this.entriesCnt++;
        return true;
    }

    public int flushTo(List<ByteBuffer> dest) {
        int size = this.size();
        try {
            for (ByteBuffer byteBuffer : this.record) {
                byteBuffer.flip();
                dest.add(byteBuffer);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            this.clear();
        }
    }

    public int flushTo(FileChannel channel) {
        int size = this.size();
        try {
            for (ByteBuffer byteBuffer : this.record) {
                byteBuffer.flip();
                channel.write(byteBuffer);
                this.config.getBlockBufferPool().returnBuffer(byteBuffer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.clear();
        }
        return size;
    }

    public void clear() {
        this.entriesCnt = 0;
        this.record.clear();
        this.prevKey = new byte[0];
        this.record.add(this.config.getBlockBufferPool().borrowBuffer());
    }


    public Config getConfig() {
        return config;
    }


    public int getEntriesCnt() {
        return entriesCnt;
    }

    public byte[] getPrevKey() {
        return prevKey;
    }
}
