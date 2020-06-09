package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.PMemPhysicalAddress;

public class PMemBlkPhysicalAddress implements PMemPhysicalAddress {

    private int index;

    public PMemBlkPhysicalAddress(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

}
