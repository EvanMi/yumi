package com.yumi.lsm.memtable.wal;

import com.yumi.lsm.wal.WalWriter;
import org.junit.jupiter.api.Test;

public class WalWriterTest {

    @Test
    public void testMap() {
        WalWriter walWriter = new WalWriter("/tmp/tree/yumi.txt", 128);
        walWriter.write("yumi".getBytes(), "bilibili.com".getBytes());
        walWriter.write("yumi1".getBytes(), "bilibili.com".getBytes());
        walWriter.close();
    }
}
