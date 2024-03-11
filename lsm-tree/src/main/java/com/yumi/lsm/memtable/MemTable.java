package com.yumi.lsm.memtable;

import java.util.List;
import java.util.Optional;

public interface MemTable {
    /**
     * 添加元素
     */
    void put(byte[] key, byte[] value);

    /**
     * 获取元素
     */
    Optional<byte[]> get(byte[] key);

    /**
     * @return 有序表内数据大小，单位 byte
     */
    int size();

    /**
     * @return kv 对数量
     */
    int entriesCnt();

    /**
     * 获取所有的k-v对
     */
    List<Kv> all();

    class Kv {
        private byte[] key;
        private byte[] value;

        public Kv(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }

    }

}
