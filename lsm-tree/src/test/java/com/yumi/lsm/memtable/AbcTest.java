package com.yumi.lsm.memtable;

import com.yumi.lsm.Config;
import com.yumi.lsm.Tree;
import com.yumi.lsm.util.AllUtils;

import java.io.File;

public class AbcTest {

    public static void main(String[] args) {
//        Tree tree = new Tree(Config.newConfig("/tmp/tree"));
//        System.out.println(new String(tree.get("testKey27".getBytes()).get()));

        new File("/tmp/tree/0_1.sst").delete();
    }
}
