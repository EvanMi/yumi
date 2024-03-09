package com.yumi.utils.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestCountDownLatch2 {

    @Test
    public void test() {
        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch2 latch2 = new CountDownLatch2(2);
            for (int i = 0; i < 4; i++) {
                exec.execute(() -> {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                        latch2.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                exec.execute(() -> {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                        latch2.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                latch2.await();
                System.out.println("一个周期结束了 -> " + i);
                latch2.reset();
            }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
            exec.shutdown();
        }
    }
}
