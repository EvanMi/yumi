package com.yumi.utils.collections;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class CollectionsUtilsTest {

    @Test
    public void test() {
        List<String> list = getCopyOnWriteArrayList();
        CollectionUtils.removeMatched(list, item -> item.equals("c"));
        System.out.println(list);
    }

    private static List<String> getArrayList() {
        List<String> res = new ArrayList<>();
        fillList(res);
        return res;
    }

    private static List<String> getCopyOnWriteArrayList() {
        List<String> res = new CopyOnWriteArrayList<>();
        fillList(res);
        return res;
    }


    private static void fillList(List<String> res) {
        res.add("a");
        res.add("b");
        res.add("c");
        res.add("d");
    }
}
