package com.yumi.lsm.wal;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class WalWriter {
    private String file;
    private File dest;
    private BufferedOutputStream writer;

    public WalWriter(String file) {
        File dest = new File(file);
        this.file = file;
        this.dest = dest;
        try {
            this.writer = new BufferedOutputStream(new FileOutputStream(dest, dest.exists()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    public void write(byte[] key, byte[] value) {
        try {
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES * 2);
            tmp.putInt(key.length);
            tmp.putInt(value.length);
            writer.write(tmp.array());
            writer.write(key);
            writer.write(value);
            //todo 每次都flush,性能低
            writer.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            this.writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
