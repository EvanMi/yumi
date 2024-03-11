package com.yumi.lsm.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.util.AllUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * sst 文件中的数据块，和索引、过滤器为一一对应关系
 */
public class Block {
    // lsm tree 配置文件
    private Config config;
    // 用于复制溢写数据的缓冲区
    private ByteArrayOutputStream record;
    // kv 对数量
    private int entriesCnt;
    // 最晚一笔写入的数据的 key
    private byte[] prevKey = new byte[0];


    public Block(Config config) {
        this.config = config;
        this.record = new ByteArrayOutputStream();
    }

    public int size() {
        return this.record.size();
    }

    public void append(byte[] key, byte[] value) {
        int sharedPrefixLen = AllUtils.sharedPrefixLen(key, this.prevKey);

        ByteBuffer tmpBuffer = ByteBuffer.allocate(Integer.BYTES * 3);
        //写入长度
        tmpBuffer.putInt(sharedPrefixLen);
        tmpBuffer.putInt(key.length - sharedPrefixLen);
        tmpBuffer.putInt(value.length);
        this.record.write(tmpBuffer.array(), 0, Integer.BYTES * 3);

        //写入key和value
        this.record.write(key, sharedPrefixLen, key.length - sharedPrefixLen);
        this.record.write(value, 0, value.length);

        this.prevKey = key;
        this.entriesCnt++;
    }

    public int flushTo(OutputStream dest) {
        try {
            dest.write(this.toBytes());
            dest.flush();
            return this.record.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.clear();
        }
    }

    public void clear() {
        this.entriesCnt = 0;
        try {
            this.record.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.prevKey = new byte[0];
        this.record = new ByteArrayOutputStream();

    }

    public byte[] toBytes() {
        return this.record.toByteArray();
    }


    public Config getConfig() {
        return config;
    }

    public ByteArrayOutputStream getRecord() {
        return record;
    }

    public int getEntriesCnt() {
        return entriesCnt;
    }

    public byte[] getPrevKey() {
        return prevKey;
    }
}
