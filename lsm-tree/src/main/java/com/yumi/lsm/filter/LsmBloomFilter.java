package com.yumi.lsm.filter;

import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.filter.bloom.BloomFilter;

public class LsmBloomFilter implements Filter {
    private final BloomFilter bloomFilter = BloomFilter.createByFn(20, 400);
    private BitsArray bitsArray = BitsArray.create(bloomFilter.getM());
    private int keyCnt = 0;

    @Override
    public void add(byte[] key) {
        this.keyCnt++;
        bloomFilter.hashTo(bloomFilter.calcBitPositions(key), this.bitsArray);
    }

    @Override
    public boolean exist(BitsArray bitmap, byte[] key) {
        return bloomFilter.isHit(bloomFilter.calcBitPositions(key), bitmap);
    }

    @Override
    public BitsArray hash() {
        return this.bitsArray.clone();
    }

    @Override
    public void reset() {
        this.bitsArray = BitsArray.create(this.bloomFilter.getM());
        this.keyCnt = 0;
    }

    @Override
    public int keyLen() {
        return this.keyCnt;
    }
}
