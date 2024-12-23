package com.yumi.collapsed.profiler.info.analyst.quota.entry;

public class PathEntry extends Entry {
    @Override
    public String toString() {
        return key + " " + count;
    }
}