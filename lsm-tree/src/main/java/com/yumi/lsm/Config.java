package com.yumi.lsm;

import com.yumi.lsm.filter.Filter;
import com.yumi.lsm.filter.LsmBloomFilter;
import com.yumi.lsm.memtable.MemTableConstructor;
import com.yumi.lsm.memtable.SkipListMemTable;

import java.io.File;
import java.util.function.Consumer;

public class Config {
    // sst文件存放的目录
    private String dir;
    // lsm tree 总共多少层
    private int maxLevel = 7;
    // 每个 sst table 大小，默认 2M
    private int sstSize = 1024 * 1024 * 2;
    // 每层多少个 sst table，默认 10 个
    private int sstNumPerLevel = 10;
    // sst table 中 block 大小 默认 32KB
    private int sstDataBlockSize = 32 * 1024;
    // sst table 中 footer 部分大小. 固定为 32B
    private final int sstFooterSize = 16;
    // 过滤器. 默认使用布隆过滤器
    private Filter filter = new LsmBloomFilter();
    // memtable 构造器，默认为跳表
    private MemTableConstructor memTableConstructor = SkipListMemTable::new;

    private Config() {

    }

    public static Config newConfig(String dir, ConfigOption ... opts) {
        Config config = new Config();
        config.setDir(dir);
        for (ConfigOption opt : opts) {
            opt.accept(config);
        }
        config.check();
        return config;
    }

    private void check() {
        File fileDir = new File(this.getDir());
        if (!fileDir.exists()) {
            boolean makeDirRes = fileDir.mkdirs();
            if (!makeDirRes) {
                throw new IllegalStateException("创建 " + this.getDir() + " 失败");
            }
        }
        String walDir = this.getDir() + File.separator + "walfile";
        File fileWalDir = new File(walDir);
        if (!fileWalDir.exists()) {
            boolean makeWalDirRes = fileWalDir.mkdirs();
            if (!makeWalDirRes) {
                throw new IllegalStateException("创建 " + walDir + " 失败");
            }
        }
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public int getSstSize() {
        return sstSize;
    }

    public int getLevelSstSize(int level) {
        if (level == 0) {
            return this.sstSize;
        }
        if (level == 1) {
            return this.sstSize * 10;
        }
        return this.sstSize * 100 * level;
    }

    public int getWalFileSize() {
        return getSstSize() * 4 / 5;
    }

    public void setSstSize(int sstSize) {
        if (sstSize <= 0) {
            throw new IllegalStateException("非法的sstSize");
        }
        this.sstSize = sstSize;
    }

    public int getSstNumPerLevel() {
        return sstNumPerLevel;
    }

    public void setSstNumPerLevel(int sstNumPerLevel) {
        if (sstNumPerLevel <= 0) {
            throw new IllegalStateException("非法的sstNumPerLevel");
        }
        this.sstNumPerLevel = sstNumPerLevel;
    }

    public int getSstDataBlockSize() {
        return sstDataBlockSize;
    }

    public void setSstDataBlockSize(int sstDataBlockSize) {
        if (sstDataBlockSize <= 0) {
            throw new IllegalStateException("非法的sstDataBlockSize");
        }
        this.sstDataBlockSize = sstDataBlockSize;
    }

    public int getSstFooterSize() {
        return sstFooterSize;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public MemTableConstructor getMemTableConstructor() {
        return memTableConstructor;
    }

    public void setMemTableConstructor(MemTableConstructor memTableConstructor) {
        this.memTableConstructor = memTableConstructor;
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public void setMaxLevel(int maxLevel) {
        if (maxLevel <= 1) {
            throw new IllegalStateException("非法的maxLevel");
        }
        this.maxLevel = maxLevel;
    }

    @FunctionalInterface
    public interface ConfigOption extends Consumer<Config> {}
}
