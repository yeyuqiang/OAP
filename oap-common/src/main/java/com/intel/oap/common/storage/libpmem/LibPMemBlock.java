package com.intel.oap.common.storage.libpmem;

public class LibPMemBlock {

    private long baseAddress;
    private long offset;

    public LibPMemBlock(long baseAddress, long offset) {
        this.baseAddress = baseAddress;
        this.offset = offset;
    }

    public long getBaseAddress() {
        return baseAddress;
    }

    public long getOffset() {
        return offset;
    }

}
