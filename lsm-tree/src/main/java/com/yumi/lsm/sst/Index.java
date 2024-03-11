package com.yumi.lsm.sst;

import java.util.Arrays;
import java.util.Objects;

/**
 * sstable 中用于快速检索 block 的索引
 */
public class Index {
    // 索引的 key. 保证其 >= 前一个 block 最大 key； < 后一个 block 的最小 key
    private byte[] key;
    // 索引前一个 block 起始位置在 sstable 中对应的 offset
    private int prevBlockOffset;
    // 索引前一个 block 的大小，单位 byte
    private int prevBlockSize;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public int getPrevBlockOffset() {
        return prevBlockOffset;
    }

    public void setPrevBlockOffset(int prevBlockOffset) {
        this.prevBlockOffset = prevBlockOffset;
    }

    public int getPrevBlockSize() {
        return prevBlockSize;
    }

    public void setPrevBlockSize(int prevBlockSize) {
        this.prevBlockSize = prevBlockSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return prevBlockOffset == index.prevBlockOffset
                && prevBlockSize == index.prevBlockSize
                && Arrays.equals(key, index.key);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(prevBlockOffset, prevBlockSize);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
