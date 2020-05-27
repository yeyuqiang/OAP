package com.intel.oap.common.storage.libpmem;

import com.intel.oap.common.storage.libpmem.LibPMemMetaStore.PMemFile;
import com.intel.oap.common.storage.stream.ChunkReader;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.unsafe.PMemMemoryMapper;
import com.intel.oap.common.util.MemCopyUtil;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class LibPMemChunkReader extends ChunkReader {

    private LibPMemMetaStore pMemMetaStore;

    private long originAddress;
    private long baseAddress;
    private long pmemFileLength;

    private String currentPMemFileName;

    public LibPMemChunkReader(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
        pMemMetaStore = (LibPMemMetaStore) pMemManager.getpMemMetaStore();
        this.pmemFileLength = pMemMetaStore.getPMemFileLength();
        PMemFile pmemFile = pMemMetaStore.getFirstReadablePMemFile(logicalID);
        this.currentPMemFileName = pmemFile.getFileName();
        openPMemFile(currentPMemFileName, pmemFile.getOffset());
    }

    private void openPMemFile(String pmemFileName, long offset) {
        originAddress = PMemMemoryMapper.pmemMapFile(pmemFileName,
                0, true);
        baseAddress = originAddress + offset;
    }

    @Override
    protected int readFromPMem(PMemPhysicalAddress id, ByteBuffer data) {
        LibPMemPhysicalAddress pMemPhysicalAddress = (LibPMemPhysicalAddress) id;
        String pmemFileName = pMemPhysicalAddress.getPMemFileName();
        if (!pmemFileName.equals(currentPMemFileName)) {
            currentPMemFileName = pmemFileName;
            openPMemFile(currentPMemFileName, 0);
        }

        LibPMemBlock pMemBlock = pMemPhysicalAddress.getPMemBlock();
        int pmemBlockSize = (int) pMemBlock.getOffset();
        byte[] buf = new byte[pmemBlockSize];
        MemCopyUtil.copyMemory(null, baseAddress + pMemBlock.getBaseAddress(),
                buf, Unsafe.ARRAY_BYTE_BASE_OFFSET, pmemBlockSize);
        data.put(buf);
        return pmemBlockSize;
    }

    protected void closeInternal() {
        PMemMemoryMapper.pmemUnmap(originAddress, pmemFileLength);
    }

}
