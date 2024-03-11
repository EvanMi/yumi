package com.yumi.lsm;

import com.yumi.lsm.filter.bloom.BitsArray;
import com.yumi.lsm.memtable.MemTable;
import com.yumi.lsm.sst.Index;
import com.yumi.lsm.sst.Kv;
import com.yumi.lsm.sst.SstReader;
import com.yumi.lsm.sst.SstWriter;
import com.yumi.lsm.util.AllUtils;
import com.yumi.lsm.wal.WalReader;
import com.yumi.lsm.wal.WalWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Tree {
    private final ExecutorService compactPool = Executors.newFixedThreadPool(8);
    private Config config;
    // 读写数据时使用的锁
    private ReentrantReadWriteLock dataLock;
    // 每层 node 节点使用的读写锁
    private ReentrantReadWriteLock[] levelLocks;
    // 读写 memtable
    private MemTable memTable;
    // 只读 memtable
    private List<MemTableCompactItem> rOnlyMemTable;
    private WalWriter walWriter;
    private List<List<Node>> nodes;

    private final ArrayBlockingQueue<MemTableCompactItem> memCompactQueue = new ArrayBlockingQueue<>(10);

    private final ArrayBlockingQueue<Integer> levelCompactQueue = new ArrayBlockingQueue<>(10);
    private final AtomicBoolean stopC = new AtomicBoolean(false);

    // memtable index，需要与 wal 文件一一对应
    private int memTableIndex;
    // 各层 sstable 文件 seq. sstable 文件命名为 level_seq.sst
    private AtomicInteger[] levelToSeq;

    public Tree(Config config) {
        this.config = config;
        this.levelToSeq = new AtomicInteger[config.getMaxLevel()];
        for (int i = 0; i < levelToSeq.length; i++) {
            levelToSeq[i] = new AtomicInteger(0);
        }
        this.nodes = new ArrayList<>();
        for (int i = 0; i < config.getMaxLevel(); i++) {
            nodes.add(new ArrayList<>());
        }
        this.dataLock = new ReentrantReadWriteLock();
        this.levelLocks = new ReentrantReadWriteLock[config.getMaxLevel()];
        for (int i = 0; i < levelLocks.length; i++) {
            levelLocks[i] = new ReentrantReadWriteLock();
        }
        this.rOnlyMemTable = new ArrayList<>();

        this.constructTree();

        this.compactPool.submit(() -> {
            while (!this.stopC.get()) {
                try {
                    MemTableCompactItem item = this.memCompactQueue.poll(10, TimeUnit.SECONDS);
                    if (null != item) {
                        this.compactMemTable(item);
                    }
                    Integer level = this.levelCompactQueue.poll(10, TimeUnit.SECONDS);
                    if (null != level) {
                        this.compactLevel(level);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        this.constructMemTable();
    }

    public void close() {
        if (this.stopC.compareAndSet(false, true)) {
            for (List<Node> nodeLst : this.nodes) {
                for (Node node : nodeLst) {
                    node.close();
                }
            }
        }
        this.compactPool.shutdown();
    }

    public void put(byte[] key, byte[] value) {
        ReentrantReadWriteLock.WriteLock writeLock = this.dataLock.writeLock();
        writeLock.lock();
        try {
            this.walWriter.write(key, value);
            this.memTable.put(key, value);
            // 倘若读写跳表的大小未达到 level0 层 sstable 的大小阈值，则直接返回.
            // 考虑到溢写成 sstable 后，需要有一些辅助的元数据，预估容量放大为 5/4 倍
            if (this.memTable.size() * 5 / 4 <= this.config.getSstSize()) {
                return;
            }
            this.refreshMemTableLocked();

        } finally {
            writeLock.unlock();
        }
    }

    public Optional<byte[]> get(byte[] key) {
        ReentrantReadWriteLock.ReadLock readLock = this.dataLock.readLock();
        readLock.lock();
        try {
            Optional<byte[]> valOpt = this.memTable.get(key);
            if (valOpt.isPresent()) {
                return valOpt;
            }
            // readOnly memtable.  按照 index 倒序遍历，因为 index 越大，数据越晚写入，实时性越强
            int oldLen = this.rOnlyMemTable.size();
            for (int i = oldLen - 1; i >= 0; i--) {
                MemTableCompactItem memTableCompactItem = this.rOnlyMemTable.get(i);
                Optional<byte[]> oldOpt = memTableCompactItem.memTable.get(key);
                if (oldOpt.isPresent()) {
                    return oldOpt;
                }
            }
        } finally {
            readLock.unlock();
        }
        // 读 sstable level0 层. 按照 index 倒序遍历，因为 index 越大，数据越晚写入，实时性越强
        ReentrantReadWriteLock.ReadLock level0Lock = this.levelLocks[0].readLock();
        level0Lock.lock();
        try {
            List<Node> level0List = this.nodes.get(0);
            int level0NodesLen = level0List.size();
            for (int i = level0NodesLen - 1; i >= 0; i--) {
                Node node = level0List.get(i);
                Optional<byte[]> level0Opt = node.get(key);
                if (level0Opt.isPresent()) {
                    return level0Opt;
                }
            }
        } finally {
            level0Lock.unlock();
        }

        //  依次读 sstable level 1 ~ i 层，每层至多只需要和一个 sstable 交互. 因为这些 level 层中的 sstable 都是无重复数据且全局有序的
        for (int level = 1; level < this.nodes.size(); level++) {
            ReentrantReadWriteLock.ReadLock levelReadLock = this.levelLocks[level].readLock();
            levelReadLock.lock();
            try {
                Optional<Node> nodeOpt = this.levelBinarySearch(level, key, 0, this.nodes.get(level).size() - 1);
                if (!nodeOpt.isPresent()) {
                    continue;
                }
                Optional<byte[]> valOpt = nodeOpt.get().get(key);
                if (valOpt.isPresent()) {
                    return valOpt;
                }
            } finally {
                levelReadLock.unlock();
            }
        }
        return Optional.empty();
    }

    private Optional<Node> levelBinarySearch(int level, byte[] key, int start, int end) {
        if (start > end) {
            return Optional.empty();
        }
        int mid = start + ((end - start) >> 1);
        List<Node> levelNodes = this.nodes.get(level);
        Node midNode = levelNodes.get(mid);
        if (AllUtils.compare(midNode.end(), key) < 0) {
            return this.levelBinarySearch(level, key, mid + 1, end);
        }
        if (AllUtils.compare(midNode.start(), key) > 0) {
            return this.levelBinarySearch(level, key, start, mid - 1);
        }
        return Optional.of(midNode);
    }

    private void refreshMemTableLocked() {
        MemTableCompactItem oldItem = new MemTableCompactItem(this.walFile(), this.memTable);
        this.rOnlyMemTable.add(oldItem);
        this.walWriter.close();
        try {
            this.memCompactQueue.put(oldItem);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.memTableIndex++;
        this.newMemTable();
    }

    private void constructTree() {
        File[] sstEntries = this.getSortedSSTEntries();
        for (File sstEntry : sstEntries) {
            this.loadNode(sstEntry);
        }
    }

    private void constructMemTable() {
        File walDir = new File(this.config.getDir() + File.separator + "walfile");
        if (!walDir.exists() || !walDir.isDirectory()) {
            throw new IllegalStateException("非法的wal文件夹");
        }
        File[] wals = walDir.listFiles(f -> f.isFile() && f.getName().endsWith(".wal"));

        //倘若 wal 目录不存在或者 wal 文件不存在，则构造一个新的 memtable
        if (wals.length == 0) {
            this.newMemTable();
        } else {
            this.restoreMemTable(wals);
        }
    }

    private void restoreMemTable(File[] wals) {
        //排序
        Arrays.sort(wals, (f1, f2) -> {
            int f1Index = walFileToMemTableIndex(f1.getName());
            int f2Index = walFileToMemTableIndex(f2.getName());
            return f1Index - f2Index;
        });
        for (int i = 0; i < wals.length; i++) {
            File wal = wals[i];
            String name = wal.getName();
            String file = this.config.getDir() + File.separator + "walfile" + File.separator + name;
            WalReader walReader = new WalReader(file);
            try {
                MemTable memTable = this.config.getMemTableConstructor().create();
                walReader.restoreToMemTable(memTable);
                if (i == wals.length - 1) {
                    // 倘若是最后一个 wal 文件，则 memtable 作为读写 memtable
                    this.memTable = memTable;
                    this.memTableIndex = walFileToMemTableIndex(name);
                    this.walWriter = new WalWriter(file);
                } else {
                    // memtable 作为只读 memtable，需要追加到只读 slice 以及 channel 中，继续推进完成溢写落盘流程
                    MemTableCompactItem memTableCompactItem = new MemTableCompactItem(file, memTable);
                    this.rOnlyMemTable.add(memTableCompactItem);
                    try {
                        this.memCompactQueue.put(memTableCompactItem);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                walReader.close();
            }
        }
    }

    private void newMemTable() {
        this.walWriter = new WalWriter(this.walFile());
        this.memTable = this.config.getMemTableConstructor().create();
    }

    private void loadNode(File sstEntry) {
        String sstEntryName = sstEntry.getName();
        SstReader sstReader = new SstReader(sstEntryName, this.config);
        // 读取各 block 块对应的 filter 信息
        Map<Integer, BitsArray> blockToFilter = sstReader.readFilter();
        // 读取 index 信息
        Index[] index = sstReader.readIndex();
        // 获取 sst 文件的大小，单位 byte
        int size = sstReader.size();
        // 解析 sst 文件名，得知 sst 文件对应的 level 以及 seq 号
        int[] levelSeqFromSSTFile = getLevelSeqFromSSTFile(sstEntryName);
        int level = levelSeqFromSSTFile[0];
        int seq = levelSeqFromSSTFile[1];
        // 将 sst 文件作为一个 node 插入到 lsm tree 中
        this.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index);
    }

    private void insertNodeWithReader(SstReader sstReader, int level, int seq, int size,
                                      Map<Integer, BitsArray> blockToFilter, Index[] index) {
        String file = this.sstFile(level, seq);
        // 记录当前 level 层对应的 seq 号（单调递增）
        this.levelToSeq[level].set(seq);

        // 创建一个 lsm node
        Node newNode = new Node(this.config, file, sstReader, level, seq, size, blockToFilter, index);

        this.levelLocks[level].writeLock().lock();
        try {
            // 对于 level0 而言，只需要 append 插入 node 即可
            this.nodes.get(level).add(newNode);

            if (level > 0) {
                // 对于 level1~levelk 层，需要根据 node 中 key 的大小，遵循顺序插入
                Collections.sort(nodes.get(level), new Comparator<Node>() {
                    @Override
                    public int compare(Node n1, Node n2) {
                        return AllUtils.compare(n1.start(), n2.end());
                    }
                });
            }
        } finally {
            this.levelLocks[level].writeLock().unlock();
        }

    }

    private void insertNode(int level, int seq, int size, Map<Integer, BitsArray> blockToFilter, Index[] index) {
        String file = this.sstFile(level, seq);
        SstReader sstReader = new SstReader(file, this.config);
        this.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index);
    }

    private String sstFile(int level, int seq) {
        return level + "_" + seq + ".sst";
    }

    private String walFile() {
        return this.config.getDir() + File.separator + "walfile" + File.separator + this.memTableIndex + ".wal";
    }

    private int walFileToMemTableIndex(String walFile) {
        return Integer.valueOf(walFile.replaceAll(".wal", ""));
    }

    private File[] getSortedSSTEntries() {
        File file = new File(this.config.getDir());
        if (!file.exists() || !file.isDirectory()) {
            throw new IllegalStateException("非法的文件夹");
        }
        File[] files = file.listFiles(item -> item.isFile() && item.getName().endsWith(".sst"));
        if (null == files) {
            return new File[0];
        }
        Arrays.sort(files, (f1, f2) -> {
            int[] f1LevelSeq = getLevelSeqFromSSTFile(f1.getName());
            int[] f2LevelSeq = getLevelSeqFromSSTFile(f2.getName());
            if (f1LevelSeq[0] == f2LevelSeq[0]) {
                return f1LevelSeq[1] - f2LevelSeq[1];
            }
            return f1LevelSeq[0] - f2LevelSeq[0];
        });
        return files;
    }

    private int[] getLevelSeqFromSSTFile(String fileName) {
        String localFileName = fileName.replaceAll(".sst", "");
        String[] split = localFileName.split("_");
        return new int[]{Integer.valueOf(split[0]), Integer.valueOf(split[1])};
    }

    private void compactMemTable(MemTableCompactItem item) {
        this.flushMemTable(item.memTable);
        ReentrantReadWriteLock.WriteLock writeLock = this.dataLock.writeLock();
        writeLock.lock();
        try {
            List<MemTableCompactItem> rOnlyMemTable = this.rOnlyMemTable;
            int rOnlyMemTableSize = rOnlyMemTable.size();

            for (int i = 0; i < rOnlyMemTableSize; i++) {
                if (rOnlyMemTable.get(i).memTable != item.memTable) {
                    new File(rOnlyMemTable.get(i).walFile).delete();
                    continue;
                }
                this.rOnlyMemTable = new ArrayList<>(this.rOnlyMemTable.subList(i + 1, rOnlyMemTableSize));
                new File(rOnlyMemTable.get(i).walFile).delete();
                break;
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void flushMemTable(MemTable memTable) {
        int seq = this.levelToSeq[0].get() + 1;
        SstWriter sstWriter = new SstWriter(this.sstFile(0, seq), this.config);
        try {
            for (MemTable.Kv kv : memTable.all()) {
                sstWriter.append(kv.getKey(), kv.getValue());
            }
            // sstable 落盘
            SstWriter.FinishRes finish = sstWriter.finish();
            // 构造节点添加到 tree 的 node 中
            this.insertNode(0, seq, finish.getSize(), finish.getBlockToFilter(), finish.getIndex());
            // 尝试引发一轮 compact 操作
            this.tryTriggerCompact(0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sstWriter.close();
        }
    }

    private void tryTriggerCompact(int level) {
        // 最后一层不执行 compact 操作
        if (level == this.nodes.size() - 1) {
            return;
        }
        int size = 0;
        for (Node node : this.nodes.get(level)) {
            size += node.size();
        }
        if (size <= this.config.getSstSize() * this.config.getSstNumPerLevel() * ((int) Math.pow(10, level))) {
            return;
        }
        this.compactPool.submit(() -> {
            try {
                this.levelCompactQueue.put(level);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void compactLevel(int level) {
        List<Node> pickedNodes = this.pickCompactNodes(level);
        // 插入到 level + 1 层对应的目标 sstWriter
        int seq = this.levelToSeq[level + 1].get() + 1;
        SstWriter sstWriter = new SstWriter(this.sstFile(level + 1, seq), this.config);
        // 获取 level + 1 层每个 sst 文件的大小阈值
        int sstLimit = this.config.getSstSize() * (int) Math.pow(10, level + 1);
        // 获取本次排序归并的节点涉及到的所有 kv 数据
        try {
            List<Kv> pickedKVs = this.pickedNodesToKVs(pickedNodes);
            for (int i = 0; i < pickedKVs.size(); i++) {
                // 倘若新生成的 level + 1 层 sst 文件大小已经超限
                if (sstWriter.size() > sstLimit) {
                    SstWriter.FinishRes finish = sstWriter.finish();
                    sstWriter.close();
                    this.insertNode(level + 1, seq, finish.getSize(), finish.getBlockToFilter(), finish.getIndex());
                    // 构造一个新的 level + 1 层 sstWriter
                    seq = this.levelToSeq[level + 1].get() + 1;
                    sstWriter = new SstWriter(this.sstFile(level + 1, seq), this.config);
                }
                Kv kv = pickedKVs.get(i);
                sstWriter.append(kv.getKey(), kv.getValue());
                // 倘若这是最后一笔 kv 数据，需要负责把 sstWriter 溢写落盘并把对应 node 插入到 lsm tree 内存结构中
                if (i == pickedKVs.size() - 1) {
                    SstWriter.FinishRes finish = sstWriter.finish();
                    this.insertNode(level + 1, seq, finish.getSize(), finish.getBlockToFilter(), finish.getIndex());
                }
            }
        } finally {
            sstWriter.close();
        }

        // 移除这部分被合并的节点
        this.removeNodes(level, pickedNodes);

        // 尝试触发下一层的 compact 操作
        this.tryTriggerCompact(level + 1);
    }

    // 移除所有完成 compact 流程的老节点
    private void removeNodes(int level, List<Node> pickedNodes) {
        // 从 lsm tree 的 nodes 中移除老节点
        for (int i = level + 1; i >= level; i--) {
            ReentrantReadWriteLock.WriteLock writeLock = this.levelLocks[i].writeLock();
            writeLock.lock();
            try {
                this.nodes.get(i).removeAll(pickedNodes);
            } finally {
                writeLock.unlock();
            }
        }
        this.compactPool.submit(() -> {
            // 销毁老节点，包括关闭 sst reader，并且删除节点对应 sst 磁盘文件
            for (Node pickedNode : pickedNodes) {
                pickedNode.destroy();
            }
        });
    }

    // 获取本轮 compact 流程涉及到的所有 kv 对. 这个过程中可能存在重复 k，保证只保留最新的 v
    private List<Kv> pickedNodesToKVs(List<Node> pickedNodes) {
        // index 越小，数据越老. index 越大，数据越新
        // 所以使用大 index 的数据覆盖小 index 数据，以久覆新
        MemTable memTable = this.config.getMemTableConstructor().create();
        for (Node pickedNode : pickedNodes) {
            Kv[] kvs = pickedNode.getAll();
            for (Kv kv : kvs) {
                memTable.put(kv.getKey(), kv.getValue());
            }
        }

        // 借助 memtable 实现有序排列
        List<MemTable.Kv> kvs = memTable.all();
        List<Kv> res = new ArrayList<>();
        for (MemTable.Kv kv : kvs) {
            res.add(new Kv(kv.getKey(), kv.getValue()));
        }
        return res;
    }

    private List<Node> pickCompactNodes(int level) {
        // 每次合并范围为当前层前一半节点
        List<Node> levelNodes = this.nodes.get(level);
        byte[] startKey = levelNodes.get(0).start();
        byte[] endKey = levelNodes.get(0).end();

        int mid = levelNodes.size() >> 1;
        Node midNode = levelNodes.get(mid);
        if (AllUtils.compare(midNode.start(), startKey) < 0) {
            startKey = midNode.start();
        }
        if (AllUtils.compare(midNode.end(), endKey) > 0) {
            endKey = midNode.end();
        }

        List<Node> pickedNodes = new ArrayList<>();
        for (int i = level + 1; i >= level; i--) {
            for (Node node : this.nodes.get(level)) {
                if (AllUtils.compare(endKey, node.start()) < 0 || AllUtils.compare(startKey, node.end()) > 0) {
                    continue;
                }
                // 所有范围有重叠的节点都追加到 list
                pickedNodes.add(node);
            }
        }
        return pickedNodes;
    }

    public static class MemTableCompactItem {
        String walFile;
        MemTable memTable;

        public MemTableCompactItem(String file, MemTable memTable) {
            this.walFile = file;
            this.memTable = memTable;
        }
    }

}