package com.yumi.collapsed.profiler.info.analyst.quota.printer;

import com.yumi.collapsed.profiler.info.analyst.quota.entry.Entry;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractPrinter <T extends Entry> {

    protected final String fileName;
    protected final List<String> pakgeList;

    public AbstractPrinter(String fileName, String packages) {
        this.fileName = fileName;
        this.pakgeList = Arrays.asList(packages.replaceAll("\\.", "/").split(";"));
    }

    abstract List<T> getPrintList() throws Exception;
    public abstract void print() throws Exception;
}
