package com.yumi.utils.system;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

public class ZipUtilsTest {

    @Test
    public void testZip() throws IOException {
        byte[] compress = ZipUtils.compress("23342ljlmlj0934344".getBytes(), 5);
        System.out.println(Arrays.toString(compress));
    }


    @Test
    public void testUnzip() throws IOException {
        byte[] compress = ZipUtils.compress("23342ljlmlj0934344".getBytes(), 5);
        byte[] uncompress = ZipUtils.uncompress(compress);
        System.out.println(new String(uncompress));
    }
}
