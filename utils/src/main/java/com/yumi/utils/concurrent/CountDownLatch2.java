package com.yumi.utils.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 一个重用版本的count down latch
 */
public class CountDownLatch2 {

    private final Sync sync;

    public CountDownLatch2(int count) {
        if (count < 0)
            throw new IllegalArgumentException("count < 0");
        sync = new Sync(count);
    }

    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, timeUnit.toNanos(timeout));
    }

    public void countDown() {
        sync.releaseShared(1);
    }

    public long getCount() {
        return sync.getCount();
    }

    public void reset() {
        sync.reset();
    }

    @Override
    public String toString() {
        return super.toString() + "[Count = " + sync.getCount() + "]";
    }

    private static final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 4982264981922014374L;
        private final int startCount;

        Sync(final int startCount) {
            this.setState(startCount);
            this.startCount = startCount;
        }

        int getCount() {
            return this.getState();
        }

        @Override
        protected int tryAcquireShared(int arg) {
            return (this.getState() == 0) ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(int arg) {
            for(;;) {
                int c = this.getState();
                if (c == 0 || c < arg) {
                    return false;
                }
                int nextC = c - arg;
                if (compareAndSetState(c, nextC)) {
                    return nextC == 0;
                }
            }
        }

        void reset() {
            if (getState() == 0) {
                this.setState(this.startCount);
            }
            for(;;) {
                boolean released = this.releaseShared(this.getState());
                if (released || getState() == 0) {
                    break;
                }
            }
            this.setState(this.startCount);
        }
    }
}
