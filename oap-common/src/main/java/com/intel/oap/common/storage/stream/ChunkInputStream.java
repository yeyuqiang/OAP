package com.intel.oap.common.storage.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class ChunkInputStream extends InputStream implements ReadableByteChannel {

   private boolean isOpen = true;
   protected ChunkReader chunkReader;

    public ChunkInputStream(byte[] name, DataStore dataStore) {
        super();
        this.chunkReader = dataStore.getChunkReader(name);
    }

    public int read() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int read(byte b[]) throws IOException {
        assert(b.length > 0);
        return chunkReader.read(b);
    }

    public int read(byte b[], int off, int len) throws IOException {
        return this.read(b);
    }

    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public int available() throws IOException {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void free() throws IOException {
        chunkReader.freeFromPMem();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int remaining = dst.remaining();
        byte[] bytes = new byte[remaining];
        read(bytes);
        dst.put(bytes);
        return remaining;
    }

    @Override
    public void close() {
        isOpen = false;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }
}
