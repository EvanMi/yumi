package com.yumi.lsm;

import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.sst.Index;
import com.yumi.lsm.sst.Kv;
import com.yumi.lsm.sst.SstReader;
import com.yumi.lsm.util.AllUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * lsm tree 中的一个节点. 对应一个 sstable
 */
public class Node {
    // 配置文件
    private Config config;
    // sstable 对应的文件名，不含目录路径
    private String file;
    // sstable 所在 level 层级
    private int level;
    // sstable 的 seq 序列号. 对应为文件名中的 level_seq.sst 中的 seq
    private int seq;
    // sstable 的大小，单位 byte
    private int size;
    // 各 block 对应的 filter bitmap
    private Map<Integer, BitsArray> blockToFilter;
    // 各 block 对应的索引
    private Index[] index;
    // sstable 中最小的 key
    private byte[] startKey;
    // sstable 中最大的 key
    private byte[] endKey;
    // 读取 sst 文件的 reader 入口
    private SstReader sstReader;


    public Node(Config conf, String file, SstReader sstReader, int level,
                int seq, int size, Map<Integer, BitsArray> blockToFilter, Index[] index) {
        this.config = conf;
        this.file = file;
        this.sstReader = sstReader;
        this.level = level;
        this.seq = seq;
        this.size = size;
        this.blockToFilter = blockToFilter;
        this.index = index;
        this.startKey = index[0].getKey();
        this.endKey = index[index.length - 1].getKey();
    }

    public Kv[] getAll() {
        return this.sstReader.readData();
    }

    public Optional<byte[]> get(byte[] key) {
        Optional<Index> indexOptional = binarySearchIndex(key, 0, this.index.length - 1);
        if (!indexOptional.isPresent()) {
            return Optional.empty();
        }
        Index localIndex = indexOptional.get();
        BitsArray bitsArray = this.blockToFilter.get(localIndex.getPrevBlockOffset());
        if (null == bitsArray) {
            return Optional.empty();
        }
        if (!this.config.getFilter().exist(bitsArray, key)) {
            return Optional.empty();
        }
        ByteBuffer byteBuffer = this.sstReader.readBlock(localIndex.getPrevBlockOffset(), localIndex.getPrevBlockSize());
        Kv[] kvs = this.sstReader.readBlockData(byteBuffer);
        for (Kv kv : kvs) {
            if (Arrays.equals(kv.getKey(), key)) {
                return Optional.of(kv.getValue());
            }
        }
        return Optional.empty();
    }

    public int size() {
        return size;
    }

    public byte[] start() {
        return this.startKey;
    }

    public byte[] end() {
        return this.endKey;
    }

    public int[] index() {
        return new int[] {this.level, this.seq};
    }

    public void destroy() {
        this.sstReader.close();
        new File(this.config.getDir() + File.separator + this.file).delete();
    }

    public void close() {
        this.sstReader.close();
    }


    private Optional<Index> binarySearchIndex(byte[] key, int start, int end) {
        if (start == end) {
            if (AllUtils.compare(this.index[start].getKey(), key) >= 0) {
                return Optional.of(index[start]);
            } else {
                return Optional.empty();
            }
        }

        int mid = start + ((end - start) >> 1);
        if (AllUtils.compare(this.index[mid].getKey(), key) < 0) {
            return this.binarySearchIndex(key, mid + 1, end);
        }
        return this.binarySearchIndex(key, start, mid);
    }
}
