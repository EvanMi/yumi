package com.yumi.utils.system;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class ZipUtils {
    public static byte[] compress(byte[] src, int level) throws IOException {
        byte[] result = src;
        java.util.zip.Deflater deflater = new java.util.zip.Deflater(level);
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
            DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, deflater)) {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            deflater.end();
            throw e;
        } finally {
            deflater.end();
        }
        return result;
    }


    public static byte[] uncompress(byte[] src) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];


        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
             InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length)) {
            while (true) {
                int read = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
                if (read <= 0) {
                    break;
                }
                byteArrayOutputStream.write(uncompressData, 0, read);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        }
        return result;
    }

}
