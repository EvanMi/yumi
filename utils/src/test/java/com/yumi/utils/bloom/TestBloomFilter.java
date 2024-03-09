package com.yumi.utils.bloom;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBloomFilter {

    @Test
    public void test() {

        BloomFilter filter = BloomFilter.createByFn(20, 400);
        BitsArray bitsArray = BitsArray.create(filter.getM());
        System.out.println(filter.getM());
        filter.hashTo("yumi1", bitsArray);
        filter.hashTo("yumi2", bitsArray);

        assertTrue(filter.isHit("yumi1", bitsArray));
        assertTrue(filter.isHit("yumi2", bitsArray));
        assertFalse(filter.isHit("yumi3", bitsArray));

    }
}
