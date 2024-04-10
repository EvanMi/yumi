package com.yumi.utils.sugar;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleTest {

    static class Yumi {
        @Override
        public String toString() {
            return "yumi";
        }
    }

    static class Nice {
        @Override
        public String toString() {
            return "nice";
        }
    }

    static class Bili {
        @Override
        public String toString() {
            return "2233";
        }
    }

    static Tuple getTuple() {
        return  Tuple.of(new Yumi(), new Nice(), new Bili());
    }

    @Test
    public void testGetIndex() {
        Tuple tuple = getTuple();
        Yumi yumi = tuple.getObj(0);
        assertEquals("yumi", yumi.toString());
        Nice nice = tuple.getObj(1);
        assertEquals( "nice", nice.toString());
        Bili bili = tuple.getObj(2);
        assertEquals( "2233", bili.toString());
    }

    @Test
    public void testGetFirst() {
        Tuple tuple = getTuple();
        Yumi yumi = tuple.first();
        assertEquals("yumi", yumi.toString());
    }

    @Test
    public void testGetSecond() {
        Tuple tuple = getTuple();
        Nice nice = tuple.second();
        assertEquals( "nice", nice.toString());
    }

    @Test
    public void testGetThird() {
        Tuple tuple = getTuple();
        Bili bili = tuple.third();
        assertEquals( "2233", bili.toString());
    }
}
