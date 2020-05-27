package com.intel.oap.common.storage.libpmem;

import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemPhysicalAddress;
import com.intel.oap.common.storage.stream.PMemMetaStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LibPMemMetaStore implements PMemMetaStore {

    private String pmemRootDir;

    private long pmemFileLength;
    private long pmemCapacity;
    private int pmemFileNumber;

    private int chunkSize;
    private Map<String, Integer> filledChunksPerPMemFile = new ConcurrentHashMap<>();
    private Map<String, List<PMemFile>> logicIdToPMemFiles = new ConcurrentHashMap<>();
    private Map<String, MetaData> metaDataMap = new ConcurrentHashMap<>();
    private Map<String, PMemPhysicalAddress> pMemPhysicalAddressMap = new ConcurrentHashMap<>();

    public LibPMemMetaStore(String pmemRootDir, long pmemFileLength,
                            long pmemCapacity, int chunkSize) {
        this.pmemRootDir = pmemRootDir;
        this.pmemFileLength = pmemFileLength;
        this.pmemCapacity = pmemCapacity;
        this.pmemFileNumber = (int) (pmemCapacity / pmemFileLength);

        this.chunkSize = chunkSize;
        initPMemFileStatus();
    }

    public long getPMemFileLength() {
        return pmemFileLength;
    }

    private void initPMemFileStatus() {
        for (int i = 0; i < pmemFileNumber; i++) {
            filledChunksPerPMemFile.put(pmemRootDir + i, 0);
        }
    }

    public PMemFile getWritablePMemFile(byte[] id) {
        Map<String, Integer> sortedFilledChunksPerPMemFile = filledChunksPerPMemFile
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new)
                );
        String logicId = new String(id);
        Map.Entry<String, Integer> entry = sortedFilledChunksPerPMemFile.
                entrySet().stream().findFirst().get();
        String nextPMemFileName = entry.getKey();
        int nextPMemFileFilledChunkNumber = entry.getValue();
        PMemFile nextPMemFile = new PMemFile(nextPMemFileName,
                nextPMemFileFilledChunkNumber * chunkSize);

        List<PMemFile> pmemFiles = new ArrayList<>();
        if (logicIdToPMemFiles.containsKey(logicId)) {
            pmemFiles = logicIdToPMemFiles.get(logicId);
        }
        pmemFiles.add(nextPMemFile);
        logicIdToPMemFiles.put(logicId, pmemFiles);
        return nextPMemFile;
    }

    public PMemFile getFirstReadablePMemFile(byte[] id) {
        String logicId = new String(id);
        List<PMemFile> pmemFiles = logicIdToPMemFiles.get(logicId);
        return pmemFiles.get(0);
    }

    @Override
    public PMemPhysicalAddress getPMemIDByLogicalID(byte[] id, int chunkID) {
        String key = new String(id) + "_" + chunkID;
        return pMemPhysicalAddressMap.get(key);
    }

    @Override
    public void putMetaFooter(byte[] id, MetaData metaData) {
        metaDataMap.put(new String(id), metaData);
    }

    @Override
    public void putPMemID(byte[] id, int chunkID, PMemPhysicalAddress pMemID) {
        String key = new String(id) + "_" + chunkID;
        pMemPhysicalAddressMap.put(key, pMemID);
    }

    @Override
    public MetaData getMetaFooter(byte[] id) {
        return metaDataMap.get(new String(id));
    }

    class PMemFile {
        private String fileName;
        private long offset;

        public PMemFile(String fileName, long offset) {
            this.fileName = fileName;
            this.offset = offset;
        }

        public String getFileName() {
            return fileName;
        }

        public long getOffset() {
            return offset;
        }
    }

}
