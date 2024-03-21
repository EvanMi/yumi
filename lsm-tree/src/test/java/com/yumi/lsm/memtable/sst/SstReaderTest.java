package com.yumi.lsm.memtable.sst;

import com.yumi.lsm.Config;
import com.yumi.lsm.Node;
import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.sst.Index;
import com.yumi.lsm.sst.Kv;
import com.yumi.lsm.sst.SstReader;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SstReaderTest {


    @Test
    public void testReadAll() {
         for (int x = 344; x <= 344; x++) {
             Config config = Config.newConfig("/tmp/tree");
             SstReader sstReader = new SstReader("0_" + x + ".sst", config);
             sstReader.readFooter();
//             System.out.println(sstReader.getFilterOffset());
//             System.out.println(sstReader.getFilterSize());
//             System.out.println(sstReader.getIndexOffset());
//             System.out.println(sstReader.getIndexSize());


//             System.out.println("index--------------------");
//             Index[] indices = sstReader.readIndex();
//             for (int i = 0; i < indices.length; i++) {
//                 if (i == 0 || i == indices.length - 1) {
//                     Index index = indices[i];
//                     System.out.println(new String(index.getKey()));
//                     System.out.println(index.getPrevBlockOffset());
//                     System.out.println(index.getPrevBlockSize());
//                     System.out.println("=====");
//
//                 }
//             }

             System.out.println("data--------------------");

             Kv[] kvs = sstReader.readData();
             System.out.println(new String(kvs[0].getKey()));
             System.out.println(new String(kvs[kvs.length - 1].getKey()));
         }



//        System.out.println("filter--------------------");
//        Map<Integer, BitsArray> intMap = sstReader.readFilter();
//        for (Integer aInt : intMap.keySet()) {
//            System.out.println(aInt);
//            System.out.println(config.getFilter().exist(intMap.get(aInt), kvs[0].getKey()));
//        }

    }


    @Test
    public void test() throws Exception{
        Config config = Config.newConfig("/tmp/tree/");
        SstReader sstReader = new SstReader("1_2.sst", config);
        sstReader.readFooter();

        System.out.println(sstReader.getFilterOffset());
        System.out.println(sstReader.getFilterSize());
        System.out.println(sstReader.getIndexOffset());
        System.out.println(sstReader.getIndexSize());
    }
}
