package com.yumi.utils.skip;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SkipListTest {

    @Test
    public void test() {
        SkipList<Integer> integerSkipList = new SkipList<>();

        integerSkipList.insert(2);
        integerSkipList.insert(5);
        integerSkipList.insert(1);
        integerSkipList.insert(20);
        integerSkipList.insert(5);

        Assertions.assertEquals(4, integerSkipList.size());
        Node<Integer> integerNode = integerSkipList.find(5);
        Assertions.assertNotNull(integerNode);
        Assertions.assertEquals(5, integerNode.getData());
        integerSkipList.delete(5);
        Node<Integer> integerNode1 = integerSkipList.find(5);
        Assertions.assertNull(integerNode1);
        Assertions.assertNotNull(integerSkipList.find(20));
    }
}
