package com.yumi.utils.sugar;

public class Tuple {
    private final Object[] objects;

    private Tuple(Object ... objects) {
        this.objects = objects;
    }

    @SuppressWarnings("unchecked")
    public <T> T getObj(int index) {
        if (index >= this.objects.length) {
            return null;
        }
        return (T) this.objects[index];
    }

    public <T> T first() {
        return getObj(0);
    }

    public <T> T second() {
        return getObj(1);
    }

    public <T> T third() {
        return getObj(2);
    }

    public <T> T fourth() {
        return getObj(3);
    }

    public <T> T fifth() {
        return getObj(4);
    }

    public <T> T sixth() {
        return getObj(5);
    }

    public <T> T seventh() {
        return getObj(6);
    }

    public <T> T eighth() {
        return getObj(7);
    }

    public <T> T ninth() {
        return getObj(8);
    }

    public static Tuple of(Object ... objects) {
        return new Tuple(objects);
    }
}
