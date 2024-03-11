package com.yumi.lsm.memtable;

import com.yumi.lsm.Config;
import com.yumi.lsm.Tree;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class TreeTest {
    public static void main(String[] args) {
        String ww = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        Tree tree = new Tree(Config.newConfig("/tmp/tree"));
        new Thread(() -> {
           for (int i = 0; i < 1000000; i++) {
               try {
                   TimeUnit.MILLISECONDS.sleep(10);
                   tree.put(("testKey" + i).getBytes(), (i + ww).getBytes());
                   if (i % 1000 == 0) {
                       System.out.println("testKey" + i);
                   }
               } catch (InterruptedException e) {
                   throw new RuntimeException(e);
               }
           }
           tree.close();
        }).start();

        for (int i = 0; i < 1000; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
                System.out.println("key ----------:      " + "testKey" + i);
                Optional<byte[]> bytes = tree.get(("testKey" + i).getBytes());
                if (bytes.isPresent()) {
                    System.out.println(new String(bytes.get()));
                } else {
                    i--;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tree.close();
    }
}
