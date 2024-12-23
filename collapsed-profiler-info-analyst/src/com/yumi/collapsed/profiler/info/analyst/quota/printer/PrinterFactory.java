package com.yumi.collapsed.profiler.info.analyst.quota.printer;

public class PrinterFactory {

    private PrinterFactory() {
    }

    public static AbstractPrinter<?> createPrinter(String printerName, String fileName, String packages) {
        if ("hot-point".equalsIgnoreCase(printerName)) {
            return new HotPointPrinter(fileName, packages);
        } else if ("hot-path".equalsIgnoreCase(printerName)) {
            return new HotPathPrinter(fileName, packages);
        }
        throw new IllegalStateException("no printer found!");
    }
}
