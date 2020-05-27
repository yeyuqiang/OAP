package com.intel.oap.common.storage.libpmem;

import com.intel.oap.common.storage.stream.PMemPhysicalAddress;

public class LibPMemPhysicalAddress implements PMemPhysicalAddress {

    private LibPMemBlock pmemBlock;
    private String pmemFileName;

    public LibPMemPhysicalAddress(LibPMemBlock pmemBlock, String pmemFileName) {
        this.pmemBlock = pmemBlock;
        this.pmemFileName = pmemFileName;
    }

    public LibPMemBlock getPMemBlock() {
        return pmemBlock;
    }

    public String getPMemFileName() {
        return pmemFileName;
    }
}
