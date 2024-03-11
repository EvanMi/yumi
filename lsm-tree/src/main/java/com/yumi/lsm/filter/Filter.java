package com.yumi.lsm.filter;

import com.yumi.lsm.filter.bloom.BitsArray;

public interface Filter {
    // 添加 key 到过滤器
    void add(byte[] key);
    // 是否存在 key
    boolean exist(BitsArray bitmap, byte[] key);
    // 生成过滤器对应的 bitmap
    BitsArray hash();
    // 重置过滤器
    void reset();
    // 存在多少个 key
    int keyLen();

}
