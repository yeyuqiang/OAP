package com.intel.oap.common.storage.pmemblk;

import com.intel.oap.common.storage.stream.ChunkReader;
import com.intel.oap.common.storage.stream.ChunkWriter;
import com.intel.oap.common.storage.stream.MetaData;
import com.intel.oap.common.storage.stream.PMemManager;
import com.intel.oap.common.unsafe.PMemBlockPlatform;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assume.*;
import static org.junit.Assert.*;

public class PMemBlkChunkReaderWriterTest {

    private static int ELEMENT_SIZE = 1024;
    private static long POOL_SIZE = 32 * 1024 * 1024;
    private static String PATH = "/dev/shm/PMemBlkChunkReaderWriterTest_blk_file";
    private static byte[] LOGICID = "PMemBlkChunkReaderWriterTest-logicID".getBytes();
    private static String METASTORE = "pmemblk";
    private static String STORETYPE = "libpmem";

    private static String PMEMKV_PATH = "/dev/shm/pmemkv_db";
    private static String STORAGE_ENGINE = "cmap";
    private static long PMEMKV_SIZE = 32 * 1024 * 1024;

    private static PMemManager pMemManager;
    private final Random random = new Random();

    @Before
    public void checkIfLibPMemExisted() {
        assumeTrue(PMemBlockPlatform.isPMemBlkAvailable());
    }

    @BeforeClass
    public static void setUp() {
        PMemBlockPlatform.create(PATH, ELEMENT_SIZE, POOL_SIZE);
        Properties properties = new Properties();
        properties.setProperty("totalSize", String.valueOf(POOL_SIZE));
        properties.setProperty("chunkSize", String.valueOf(ELEMENT_SIZE));
        properties.setProperty("metaStore", METASTORE);
        properties.setProperty("storetype", STORETYPE);
        properties.setProperty("pmemkv_engine", STORAGE_ENGINE);
        properties.setProperty("pmemkv_path", PMEMKV_PATH);
        properties.setProperty("pmemkv_size", String.valueOf(PMEMKV_SIZE));
        pMemManager = new PMemManager(properties);
    }

    @AfterClass
    public static void tearDown() {
        PMemBlockPlatform.close();
        PMemKVDatabase.close();
        File pmemblkFile = new File(PATH);
        if (pmemblkFile != null && pmemblkFile.exists()) {
            pmemblkFile.delete();
        }

        File pmemkvFile = new File(PMEMKV_PATH);
        if (pmemkvFile != null && pmemkvFile.exists()) {
            pmemkvFile.delete();
        }
    }

    private byte[] writeBlock(int num) throws IOException {
        byte[] bytesToWrite = new byte[ELEMENT_SIZE * num];
        random.nextBytes(bytesToWrite);
        ChunkWriter chunkWriter = new PMemBlkChunkWriter(LOGICID, pMemManager);
        chunkWriter.write(bytesToWrite);
        chunkWriter.close();
        return bytesToWrite;
    }

    private byte[] readBlock(int num) throws IOException {
        byte[] bytesFromRead = new byte[ELEMENT_SIZE * num];
        ChunkReader chunkReader = new PMemBlkChunkReader(LOGICID, pMemManager);
        chunkReader.read(bytesFromRead);
        return bytesFromRead;
    }

    @Test
    public void testWriteSingleBlock() throws IOException {
        byte[] writtenBlock = writeBlock(1);
        byte[] readBlock = readBlock(1);
        assertArrayEquals(writtenBlock, readBlock);
    }

    @Test
    public void testWriteMultipleBlock() throws IOException {
        byte[] writtenBlock = writeBlock(10);
        byte[] readBlock = readBlock(10);
        assertArrayEquals(writtenBlock, readBlock);
    }

    @Test
    public void testWriteBlockExceedMaximum() throws IOException {
        int maxNum = PMemBlockPlatform.getBlockNum();
        byte[] writtenBlock = writeBlock(maxNum + 10);
        byte[] readBlock = readBlock(maxNum + 10);
        assertArrayEquals(writtenBlock, readBlock);
        MetaData meta = pMemManager.getpMemMetaStore().getMetaFooter(LOGICID);
        assertTrue(meta.isHasDiskData());
        assertEquals(maxNum, meta.getTotalChunk());
    }


}
