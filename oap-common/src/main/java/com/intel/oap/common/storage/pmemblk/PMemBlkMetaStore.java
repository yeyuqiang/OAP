package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemMetaStore;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import io.pmem.pmemkv.Database;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class PMemBlkMetaStore implements PMemMetaStore {

    private AtomicInteger index = new AtomicInteger(0);

    Database pmemkvDB;

    public PMemBlkMetaStore(Properties properties) {
        String pmemkvEngine = properties.getProperty("pmemkv_engine");
        String pmemkvPath = properties.getProperty("pmemkv_path");
        String pmemkvSize = properties.getProperty("pmemkv_size");
        pmemkvDB = PMemKVDatabase.open(pmemkvEngine, pmemkvPath, Long.parseLong(pmemkvSize));
        String currentPMemBlkIndex = pmemkvDB.get("currentPMemBlkIndex");
        if (currentPMemBlkIndex == null)
            currentPMemBlkIndex = "0";
        index.set(Integer.parseInt(currentPMemBlkIndex));
    }

    @Override
    public PMemPhysicalAddress getPhysicalAddressByID(byte[] id, int chunkID) {
        String indexStr = pmemkvDB.get(id.toString() + "_" + chunkID);
        return new PMemBlkPhysicalAddress(Integer.parseInt(indexStr));
    }

    @Override
    public void putMetaFooter(byte[] id, MetaData metaData) {
        pmemkvDB.put(id.toString() + "_hasDiskData", String.valueOf(metaData.isHasDiskData()));
        pmemkvDB.put(id.toString() + "_totalChunk", String.valueOf(metaData.getTotalChunk()));
        pmemkvDB.put("currentPMemBlkIndex", String.valueOf(index.intValue()));
    }

    @Override
    public void removeMetaFooter(byte[] id) {
        pmemkvDB.remove(id.toString() + "_hasDiskData");
        pmemkvDB.remove(id.toString() + "_totalChunk");
    }

    @Override
    public void putPhysicalAddress(byte[] id, int chunkID, PMemPhysicalAddress pMemPhysicalAddress) {
        PMemBlkPhysicalAddress pMemBlkPhysicalAddress = (PMemBlkPhysicalAddress) pMemPhysicalAddress;
        int index = pMemBlkPhysicalAddress.getIndex();
        pmemkvDB.put(id.toString() + "_" + chunkID, String.valueOf(index));
    }

    @Override
    public void removePhysicalAddress(byte[] id, int chunkID) {
        pmemkvDB.remove(id.toString() + "_" + chunkID);
    }

    @Override
    public MetaData getMetaFooter(byte[] id) {
        String hasDiskDataStr = pmemkvDB.get(id.toString() + "_hasDiskData");
        String totalChunkStr = pmemkvDB.get(id.toString() + "_totalChunk");
        return new MetaData(Boolean.parseBoolean(hasDiskDataStr), Integer.parseInt(totalChunkStr));
    }

    public int nextPMemBlockIndex() {
        return index.getAndIncrement();
    }

}
