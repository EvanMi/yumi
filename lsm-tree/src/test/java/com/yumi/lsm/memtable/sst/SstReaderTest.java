package com.yumi.lsm.memtable.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.sst.Index;
import com.yumi.lsm.sst.Kv;
import com.yumi.lsm.sst.SstReader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class SstReaderTest {


    @Test
    public void testReadAll() {
        Config config = Config.newConfig("/tmp/tree");
        SstReader sstReader = new SstReader("0_1.sst", config);
        sstReader.readFooter();
        System.out.println(sstReader.getFilterOffset());
        System.out.println(sstReader.getFilterSize());
        System.out.println(sstReader.getIndexOffset());
        System.out.println(sstReader.getIndexSize());

        System.out.println("index--------------------");
        Index[] indices = sstReader.readIndex();
        for (Index index : indices) {
            System.out.println(new String(index.getKey()));
            System.out.println(index.getPrevBlockOffset());
            System.out.println(index.getPrevBlockSize());
            System.out.println("=====");
        }

        System.out.println("data--------------------");

        Kv[] kvs = sstReader.readData();
        System.out.println(new String(kvs[0].getKey()));
        System.out.println(new String(kvs[0].getValue()));

        ByteBuffer byteBuffer = sstReader.readBlock(147871, 16433);
        Kv[] kvs1 = sstReader.readBlockData(byteBuffer);
        System.out.println("data///////");
        for (Kv kv : kvs1) {
            System.out.println(new String(kv.getKey()));
        }
        System.out.println("data///////");

        System.out.println("filter--------------------");
        Map<Integer, BitsArray> intMap = sstReader.readFilter();
        for (Integer aInt : intMap.keySet()) {
            System.out.println(aInt);
            System.out.println(config.getFilter().exist(intMap.get(aInt), kvs[0].getKey()));
        }

    }
}
