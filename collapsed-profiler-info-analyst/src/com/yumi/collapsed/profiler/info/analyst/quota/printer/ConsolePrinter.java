package com.yumi.collapsed.profiler.info.analyst.quota.printer;

import com.yumi.collapsed.profiler.info.analyst.quota.entry.Entry;

import java.util.List;

public abstract class ConsolePrinter<T extends Entry> extends AbstractPrinter<T>{

    public ConsolePrinter(String fileName, String packages) {
        super(fileName, packages);
    }

    @Override
    public void print() throws Exception {
        getPrintList().forEach(System.out::println);
    }
}
