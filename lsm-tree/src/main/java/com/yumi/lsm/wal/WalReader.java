package com.yumi.lsm.wal;

import com.yumi.lsm.memtable.MemTable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.yumi.lsm.wal.WalConstants.MAGIC_END_NUM;

public class WalReader {
    // 预写日志文件名，是包含了目录在内的绝对路径
    private String file;
    // 预写日志文件
    private RandomAccessFile src;
    // reader
    private MappedByteBuffer reader;

    public WalReader(String file) {
        File jFile = new File(file);
        if (!jFile.exists()) {
            throw new IllegalStateException("文件不存在 " + file);
        }
        this.file = file;
        try {
            this.src = new RandomAccessFile(jFile, "r");
            this.reader = this.src.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, this.src.length());
        } catch (FileNotFoundException e) {
            IllegalStateException illegalStateException = new IllegalStateException("文件不存在");
            illegalStateException.addSuppressed(e);
            throw illegalStateException;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void restoreToMemTable(MemTable memTable) {
        ByteBuffer buffer = this.reader.slice();
        buffer.position(0);
        buffer.limit(buffer.capacity());
        List<MemTable.Kv> kvs = readAll(buffer);
        for (MemTable.Kv kv : kvs) {
            memTable.put(kv.getKey(), kv.getValue());
        }
    }

    private List<MemTable.Kv> readAll(ByteBuffer buffer) {
        List<MemTable.Kv> res = new ArrayList<>();
        while (buffer.hasRemaining()) {
            int keyLenOrMagic = buffer.getInt();
            if (keyLenOrMagic == MAGIC_END_NUM) {
                break;
            }
            int valLen = buffer.getInt();
            if (keyLenOrMagic == 0 && valLen == 0) {
                break;
            }
            byte[] key = new byte[keyLenOrMagic];
            byte [] val = new byte[valLen];
            buffer.get(key);
            buffer.get(val);
            res.add(new MemTable.Kv(key, val));
        }
        return res;
    }

    public void close() {
        try {
            this.src.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
