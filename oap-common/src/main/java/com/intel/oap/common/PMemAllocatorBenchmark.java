package com.intel.oap.common;

import com.intel.oap.common.unsafe.PersistentMemoryPlatform;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class PMemAllocatorBenchmark {

    public static void main(String[] args) {
        HashMap<Integer, ByteBuffer> map = new HashMap<>();
        Random random = new Random();
        PersistentMemoryPlatform.initialize("/dev/shm", 4294967296L, 0);
        byte[] bytesToWrite = new byte[1024 * 1024];
        random.nextBytes(bytesToWrite);
        long startTimeWrite = System.currentTimeMillis();
        for (int i = 0; i < 3481; i++) {
            ByteBuffer buf = PersistentMemoryPlatform.allocateVolatileDirectBuffer(1024 * 1024);
            buf.put(bytesToWrite);
            buf.flip();
            map.put(i, buf);
        }
        System.out.println("Time for write " + (System.currentTimeMillis() - startTimeWrite) + "ms");

        byte[] bytesToRead = new byte[1024 * 1024];
        long startTimeRead = System.currentTimeMillis();
        for (Map.Entry<Integer, ByteBuffer> entry: map.entrySet()) {
            ByteBuffer buf = map.get(entry.getKey());
            buf.get(bytesToRead);
        }
        System.out.println("Time for read " + (System.currentTimeMillis() - startTimeRead) + "ms");

    }

}
