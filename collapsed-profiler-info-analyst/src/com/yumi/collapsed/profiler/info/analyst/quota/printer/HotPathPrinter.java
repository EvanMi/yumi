package com.yumi.collapsed.profiler.info.analyst.quota.printer;

import com.yumi.collapsed.profiler.info.analyst.quota.entry.PathEntry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class HotPathPrinter extends ConsolePrinter<PathEntry> {

    public HotPathPrinter(String fileName, String packages) {
        super(fileName, packages);
    }

    @Override
    List<PathEntry> getPrintList() throws Exception {
        return Files.readAllLines(Path.of(fileName))
                .stream()
                .filter(line -> {
                    for (String p : pakgeList) {
                        if (line.contains(p)) {
                            return true;
                        }
                    }
                    return false;
                })
                .map(line -> {
                    int lastSpaceIndex = line.lastIndexOf(" ");
                    long cnt = Long.parseLong(line.substring(lastSpaceIndex + 1));
                    String stacks = line.substring(0, lastSpaceIndex);
                    String[] stackArr = stacks.split(";");
                    StringBuilder sb = new StringBuilder();
                    int headerIndex = -1, tailIndex = -1;
                    for (int i = 0; i < stackArr.length; i++) {
                        for (String p : pakgeList) {
                            if (stackArr[i].contains(p)) {
                                if (headerIndex == -1) {
                                    headerIndex = i;
                                }
                                tailIndex = i;
                                break;
                            }

                        }
                    }
                    for (int i = headerIndex; i <= tailIndex; i++) {
                        sb.append(stackArr[i]).append("->");
                    }
                    PathEntry pathEntry = new PathEntry();
                    pathEntry.setCount(cnt);
                    pathEntry.setKey(sb.toString());
                    return pathEntry;
                }).collect(Collectors.groupingBy(PathEntry::getKey, Collectors.reducing(null, (x, y) -> {
                    if (x == null) {
                        return y;
                    }
                    if (y == null) {
                        return x;
                    }
                    x.setCount(x.getCount() + y.getCount());

                    return x;
                })))
                .values()
                .stream()
                .sorted(Comparator.comparing(PathEntry::getCount).reversed())
                .toList();
    }
}
