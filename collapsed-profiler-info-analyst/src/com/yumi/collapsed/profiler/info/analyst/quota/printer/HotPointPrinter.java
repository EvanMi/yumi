package com.yumi.collapsed.profiler.info.analyst.quota.printer;

import com.yumi.collapsed.profiler.info.analyst.quota.entry.ProfilerEntry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HotPointPrinter extends ConsolePrinter<ProfilerEntry> {

    public HotPointPrinter(String fileName, String packages) {
        super(fileName, packages);
    }

    @Override
    List<ProfilerEntry> getPrintList() throws Exception {
        return Files.readAllLines(Path.of(fileName))
                .stream()
                .flatMap(line -> {
                    int lastSpaceIndex = line.lastIndexOf(" ");
                    long cnt = Long.parseLong(line.substring(lastSpaceIndex + 1));
                    String stacks = line.substring(0, lastSpaceIndex);
                    String[] stackArr = stacks.split(";");
                    ProfilerEntry[] result = new ProfilerEntry[stackArr.length];
                    for (int i = 0; i < stackArr.length; i++) {
                        ProfilerEntry profilerEntry = new ProfilerEntry();
                        profilerEntry.setKey(stackArr[i]);
                        profilerEntry.setCount(cnt);
                        profilerEntry.addDepth((long) (i + 1));
                        result[i] = profilerEntry;
                    }
                    return Stream.of(result);
                })
                .filter(entry -> {
                    for (String p : pakgeList) {
                        if (entry.getKey().startsWith(p)) {
                            return true;
                        }
                    }
                    return false;
                })
                .collect(Collectors.groupingBy(ProfilerEntry::getKey, Collectors.reducing(null, (x, y) -> {
                    if (x == null) {
                        return y;
                    }
                    if (y == null) {
                        return x;
                    }
                    x.addAll(y.getDepths());
                    x.setCount(x.getCount() + y.getCount());

                    return x;
                })))
                .values()
                .stream()
                .sorted(Comparator.comparing(ProfilerEntry::getCount).reversed())
                .toList();
    }
}
