package com.yumi.lsm.wal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.yumi.lsm.wal.WalConstants.MAGIC_BYTES;
import static com.yumi.lsm.wal.WalConstants.MAGIC_END_NUM;

public class WalWriter {
    private String file;
    private RandomAccessFile dest;
    private MappedByteBuffer writer;
    private int maxSize;
    private int curPosition;

    public WalWriter(String file, int size) {
        File dest = new File(file);

        boolean needRecover = dest.exists();

        this.file = file;
        this.maxSize = size;
        try {
            this.dest = new RandomAccessFile(dest, "rw");
            this.writer = this.dest.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, size);
            int curPosition = 0;
            if (needRecover) {
                ByteBuffer view = this.writer.slice();
                while (curPosition < view.limit()) {
                    int keyLenOrMagic = view.getInt(curPosition);
                    if (keyLenOrMagic == MAGIC_END_NUM) {
                        throw new IllegalStateException("file write done");
                    }
                    int valLen = view.getInt(curPosition + 4);
                    if (keyLenOrMagic == 0 && valLen == 0) {
                        break;
                    }
                    curPosition += keyLenOrMagic + valLen + 8;
                }
                this.curPosition = curPosition;
                this.writer.position(curPosition);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public boolean write(byte[] key, byte[] value) {
        int willWriteBytes = key.length + value.length + 8;
        try {
            if (this.curPosition + MAGIC_BYTES + willWriteBytes > this.maxSize) {
                writer.putInt(MAGIC_END_NUM);
                return false;
            }
            writer.putInt(key.length);
            writer.putInt(value.length);
            writer.put(key);
            writer.put(value);
            //todo 每次都flush,性能低
            writer.force();
            this.curPosition += willWriteBytes;
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            this.dest.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
