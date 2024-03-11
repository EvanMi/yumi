package com.yumi.lsm.util;

public class AllUtils {

    private AllUtils() {}

    public static int sharedPrefixLen(byte[] key1, byte[] key2) {
        int i = 0;
        for (; i < key1.length && i < key2.length; i++) {
            if (key1[i] != key2[2]) {
                break;
            }
        }
        return i;
    }

    // 返回结果 x，保证 a <= x < b. 使用方需要自行保证 a < b
    public static byte[] getSeparatorBetween(byte[] key1, byte[] key2) {
        if (key1.length == 0) {
            byte[] sepatator = new byte[key2.length];
            System.arraycopy(key2, 0, sepatator,0, key2.length);
            sepatator[sepatator.length - 1] = (byte) (sepatator[sepatator.length - 1] - 1);
            return sepatator;
        }

        return key1;
    }

    public static int compare(byte[] a, byte[] b) {
        if (a == b)
            return 0;
        if (a == null || b == null)
            return a == null ? -1 : 1;
        int i = mismatch(a, b, Math.min(a.length, b.length));
        if (i >= 0) {
            return Byte.compare(a[i], b[i]);
        }
        return a.length - b.length;
    }

    private static int mismatch(byte[] a, byte[] b, int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] != b[i])
                return i;
        }
        return -1;
    }
}
