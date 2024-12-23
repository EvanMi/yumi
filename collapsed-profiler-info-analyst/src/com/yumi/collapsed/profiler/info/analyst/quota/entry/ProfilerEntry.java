package com.yumi.collapsed.profiler.info.analyst.quota.entry;

import java.util.TreeSet;

public class ProfilerEntry extends Entry {

    private final TreeSet<Long> depths = new TreeSet<>();

    public void addDepth(Long depth) {
        depths.add(depth);
    }

    public void addAll(TreeSet<Long> another) {
        this.depths.addAll(another);
    }

    public TreeSet<Long> getDepths() {
        return depths;
    }

    @Override
    public String toString() {
        return key + " " + count + " " + depths;
    }

}
