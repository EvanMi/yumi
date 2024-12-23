package com.yumi.collapsed.profiler.info.analyst;

import com.yumi.collapsed.profiler.info.analyst.quota.printer.PrinterFactory;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: printer file-path package;package");
            return;
        }
        PrinterFactory.createPrinter(args[0], args[1], args[2]).print();
    }
}
