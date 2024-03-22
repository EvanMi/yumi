package com.yumi.lsm.sst;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.yumi.lsm.Config;
import com.yumi.lsm.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class BlockBufferPool {

    private final int poolSize;
    private final int bufferSize;
    private final Deque<ByteBuffer> availableBuffers;

    private final Config config;

    public BlockBufferPool(int poolSize, Config config) {
        this.poolSize = poolSize;
        this.bufferSize = config.getSstDataBlockSize();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
        this.config = config;
    }

    public void init() {
        for (int i = 0; i < this.poolSize; i++) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(this.bufferSize);
            final long address = ((DirectBuffer) buffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(this.bufferSize));
            availableBuffers.offer(buffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(this.bufferSize));
        }
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer byteBuffer = this.availableBuffers.pollFirst();
        if (null == byteBuffer) {
            byteBuffer = ByteBuffer.allocate(this.bufferSize);
        }
        return byteBuffer;
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        if (!byteBuffer.isDirect()) {
            return;
        }
        //重置buffer
        byteBuffer.position(0);
        byteBuffer.limit(this.bufferSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public int availableBufferNum() {
        return availableBuffers.size();
    }
}
