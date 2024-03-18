package com.yumi.utils.system;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;

public class IoUtil {
    private IoUtil() {}

    /**
     * 字节流转为字符串
     * @param input 字节流
     * @param encoding 编码
     * @return 转换的字符串
     * @throws IOException
     */
    public static String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, "utf-8"))
                : toString(new InputStreamReader(input, encoding));
    }

    /**
     * 字符流转为字符串
     * @param reader 字符流读取
     * @return 转换的字符串
     * @throws IOException
     */
    public static String toString(Reader reader) throws IOException{
        CharArrayWriter writer = new CharArrayWriter();
        copy(reader, writer);
        return writer.toString();
    }

    /**
     * 字符读取器和写入器之间进行拷贝
     * @param reader 读取
     * @param writer 写入
     * @return 拷贝的字节数
     * @throws IOException
     */
    public static long copy(Reader reader, Writer writer) throws IOException{
        char[] buffer = new char[1 << 12];
        long count = 0;
        for (int n ; (n = reader.read(buffer)) >= 0; ) {
            writer.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    /**
     * 把一个文件的内容复制到另外一个文件中
     * @param source 被复制的文件
     * @param target 目标文件
     * @throws IOException
     */
    public static void copyFile (String source, String target) throws IOException {
        File sf = new File(source);
        if (!sf.exists()) {
            throw new IllegalArgumentException("source file does not exist.");
        }
        File tf = new File(target);
        tf.getParentFile().mkdirs();
        if (!tf.exists() && !tf.createNewFile()) {
            throw new RuntimeException("failed to create target file.");
        }
        FileChannel sc = null;
        FileChannel tc = null;
        try {
            tc = new FileOutputStream(tf).getChannel();
            sc = new FileInputStream(sf).getChannel();
            sc.transferTo(0, sc.size(), tc);
        } finally {
            if (null != sc) {
                sc.close();
            }
            if (null != tc) {
                tc.close();
            }
        }
    }

    /**
     * 删除文件或者目录 （目录会先清空）
     * @param fileOrDir 文件或者目录
     * @throws IOException
     */
    public static void delete(File fileOrDir) throws IOException {
        if (fileOrDir == null) {
            return;
        }

        if (fileOrDir.isDirectory()) {
            cleanDirectory(fileOrDir);
        }

        fileOrDir.delete();
    }

    /**
     * 清空目录
     * @param directory 要清空的目录
     * @throws IOException
     */
    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) { // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                delete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }

    }

    /**
     * 把字符串写入到文件中
     * @param file 目标文件
     * @param data 写入数据
     * @param encoding 编码格式
     * @throws IOException
     */
    public static void writeStringToFile(File file, String data, String encoding) throws IOException {
        if (null == file) {
            throw new IllegalArgumentException("File is null");
        }
        if (null == data) {
            throw new IllegalArgumentException("data is null");
        }
        if (null == encoding) {
            throw new IllegalArgumentException("encoding is null");
        }
        try (OutputStream out = new FileOutputStream(file)) {
            out.write(data.getBytes(encoding));
        }
    }

    public static String fileToString(String file) throws IOException {
        return fileToString(new File(file));
    }

    public static String fileToString(File file) throws IOException{
        if (!file.exists()) {
            return null;
        }

        byte[] data = new byte[(int) file.length()];

        boolean success;
        try ( FileInputStream inputStream = new FileInputStream(file)) {
            int len =  inputStream.read(data);
            success = len == data.length;
        }

        if (success) {
            return new String(data);
        }
        return null;
    }

    public static void stringToFileNotSafe(String data, String fileName) throws IOException {
        File file = new File(fileName);
        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.write(data);
        }
    }

    public static void stringToFile(String data, String fileName) throws IOException {
        String tmpFile = fileName + ".tmp";
        stringToFileNotSafe(data, tmpFile);

        String bakFile = fileName + ".bak";
        String prevContent = fileToString(fileName);
        if (null != prevContent) {
            stringToFileNotSafe(prevContent, bakFile);
        }

        File file = new File(fileName);
        file.delete();

        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }
}
