package com.yumi.lsm.memtable;

import com.yumi.lsm.util.AllUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 并发情况下保证最终一致性，主要问题出现在put操作的时候分两步去更行了k-v和bytes的大小。<br/>
 * 所以在get 和 size 两个方法获取状态的时候可能会出现不一致的问题。但是最终会达到一致。<br/>
 * 可以通过外部加锁的方式来保证并发情况下的绝对原子性。
 */
public class SkipListMemTable implements MemTable {
    private ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(new Comparator<byte[]>() {
        @Override
        public int compare(byte[] b1, byte[] b2) {
            return AllUtils.compare(b1, b2);
        }
    });
    private AtomicInteger bytes = new AtomicInteger(0);

    @Override
    public void put(byte[] key, byte[] value) {
        byte[] old = map.put(key, value);
        int oldSize = null == old ? 0 : old.length;
        bytes.addAndGet(value.length - oldSize);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public int size() {
        return bytes.get();
    }

    @Override
    public int entriesCnt() {
        return map.size();
    }

    @Override
    public List<Kv> all() {
        return map.entrySet()
                .stream()
                .map((entry) -> new Kv(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }
}
