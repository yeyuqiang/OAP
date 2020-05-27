package com.intel.oap.common.storage.libpmem;

import com.intel.oap.common.storage.libpmem.LibPMemMetaStore.PMemFile;
import com.intel.oap.common.storage.stream.ChunkWriter;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.unsafe.PMemMemoryMapper;

import java.nio.ByteBuffer;

public class LibPMemChunkWriter extends ChunkWriter {

    private LibPMemMetaStore pMemMetaStore;
    private long chunkSize;
    private byte[] logicalID;

    private long originAddress;
    private long baseAddress;
    private long pmemFileLength;
    private PMemFile pmemFile;

    public LibPMemChunkWriter(byte[] logicalID, PMemManager pMemManager) {
        super(logicalID, pMemManager);
        pMemMetaStore = (LibPMemMetaStore) pMemManager.getpMemMetaStore();
        this.logicalID = logicalID;
        this.chunkSize = pMemManager.getChunkSize();
        this.pmemFileLength = pMemMetaStore.getPMemFileLength();

        openPMemFile(logicalID);
    }

    private void openPMemFile(byte[] logicalID) {
        pmemFile = pMemMetaStore.getWritablePMemFile(logicalID);
        boolean isCreated = pmemFile.getOffset() > 0;
        originAddress = PMemMemoryMapper.pmemMapFile(pmemFile.getFileName(),
                isCreated ? 0 : pmemFileLength, isCreated);
        baseAddress = originAddress + pmemFile.getOffset();
    }

    @Override
    protected PMemPhysicalAddress writeInternal(ByteBuffer byteBuffer) {
        int dataSize = byteBuffer.position();
        PMemMemoryMapper.pmemMemcpy(baseAddress, byteBuffer.array(), dataSize);
        LibPMemBlock pmemBlock = new LibPMemBlock(baseAddress, dataSize);

        baseAddress += dataSize;
        if (pmemFile.getOffset() + dataSize >= pmemFileLength) {
            closeInternal();
            openPMemFile(logicalID);
        }
        return new LibPMemPhysicalAddress(pmemBlock, pmemFile.getFileName());
    }

    @Override
    protected void closeInternal() {
        PMemMemoryMapper.pmemUnmap(originAddress, pmemFileLength);
    }

}
