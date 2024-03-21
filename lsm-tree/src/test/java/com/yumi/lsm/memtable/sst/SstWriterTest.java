package com.yumi.lsm.memtable.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.sst.SstWriter;
import org.junit.jupiter.api.Test;

public class SstWriterTest {

    @Test
    public void testSmallFinish() throws Exception{
        Config config = Config.newConfig("/tmp/yumi");
        SstWriter sstWriter = new SstWriter("test", config);

        sstWriter.append("first".getBytes(), "second".getBytes());
        sstWriter.finish();
    }
}
