package com.yumi.lsm.memtable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SkipListMemTableTest {


    @Test
    public void testSingleValuePut() {
        MemTable memTable = new SkipListMemTable();
        memTable.put("key".getBytes(), "v1".getBytes());
        assertEquals(memTable.entriesCnt(), 1);
        assertEquals(memTable.size(), 2);
        assertEquals(memTable.all().size(), 1);
    }

    @Test
    public void testSingleValueRePutLess() {
        MemTable memTable = new SkipListMemTable();
        memTable.put("key".getBytes(), "v1".getBytes());
        memTable.put("key".getBytes(), "v".getBytes());
        assertEquals(memTable.entriesCnt(), 1);
        assertEquals(memTable.size(), 1);
        assertEquals(memTable.all().size(), 1);
    }

    @Test
    public void testSingleValueRePutMore() {
        MemTable memTable = new SkipListMemTable();
        memTable.put("key".getBytes(), "v1".getBytes());
        memTable.put("key".getBytes(), "very".getBytes());
        assertEquals(memTable.entriesCnt(), 1);
        assertEquals(memTable.size(), 4);
        assertEquals(memTable.all().size(), 1);
    }
}
