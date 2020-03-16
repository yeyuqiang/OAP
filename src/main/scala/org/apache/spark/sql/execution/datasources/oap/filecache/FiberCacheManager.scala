/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap.filecache

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.cache._
import org.apache.hadoop.fs.FSDataInputStream

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.io._
import org.apache.spark.sql.execution.datasources.oap.utils.CacheStatusSerDe
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OapBitSet

private[sql] class FiberCacheManager(
    sparkEnv: SparkEnv) extends Logging {

  private var _dataCacheCompressEnable = sparkEnv.conf.get(
    OapConf.OAP_ENABLE_DATA_FIBER_CACHE_COMPRESSION)
  private var _dataCacheCompressionCodec = sparkEnv.conf.get(
    OapConf.OAP_DATA_FIBER_CACHE_COMPRESSION_CODEC)
  private val _dataCacheCompressionSize = sparkEnv.conf.get(
    OapConf.OAP_DATA_FIBER_CACHE_COMPRESSION_SIZE)

  private val _dcpmmWaitingThreshold = sparkEnv.conf.get(OapConf.DCPMM_FREE_WAIT_THRESHOLD)

  private val cacheAllocator: CacheMemoryAllocator = CacheMemoryAllocator(sparkEnv)
  private val fiberLockManager = new FiberLockManager()

  def dataCacheMemory: Long = cacheAllocator.dataCacheMemory
  def indexCacheMemory: Long = cacheAllocator.indexCacheMemory
  def cacheGuardianMemory: Long = cacheAllocator.cacheGuardianMemory

  def dataCacheCompressEnable: Boolean = _dataCacheCompressEnable
  def dataCacheCompressionCodec: String = _dataCacheCompressionCodec
  def dataCacheCompressionSize: Int = _dataCacheCompressionSize

  def dcpmmWaitingThreshold: Long = _dcpmmWaitingThreshold

  private val separateMemory = cacheAllocator.separateMemory
  private val (cacheBackend, indexCacheBackend) = init()

  private def init(): (OapCache, OapCache) = {
    if (separateMemory) {
      val dataCacheBackend = OapCache(sparkEnv, OapConf.OAP_MIX_DATA_CACHE_BACKEND,
        dataCacheMemory, cacheGuardianMemory, FiberType.DATA);
      val indexCacheBackend = OapCache(sparkEnv, OapConf.OAP_MIX_INDEX_CACHE_BACKEND,
        indexCacheMemory, cacheGuardianMemory, FiberType.INDEX);
      (dataCacheBackend, indexCacheBackend)
    } else {
      val cacheBackend = OapCache(sparkEnv, OapConf.OAP_FIBERCACHE_STRATEGY,
        dataCacheMemory + indexCacheMemory, cacheGuardianMemory, FiberType.GENERAL);
      (cacheBackend, null)
    }
  }

  def stop(): Unit = {
    cacheAllocator.stop()
    if (separateMemory) {
      indexCacheBackend.cleanUp()
    }
    cacheBackend.cleanUp()
  }

  if (isDcpmmUsed()) {
    if (separateMemory) {
      indexCacheBackend.getCacheGuardian.enableWaitNotifyActive()
    }
    cacheBackend.getCacheGuardian.enableWaitNotifyActive()
  }
  // NOTE: all members' init should be placed before this line.
  logDebug(s"Initialized FiberCacheManager")

  def get(fiber: FiberId): FiberCache = {
    logDebug(s"Getting Fiber: $fiber")
    if (separateMemory &&
      (fiber.isInstanceOf[BTreeFiberId] ||
        fiber.isInstanceOf[BitmapFiberId] ||
        fiber.isInstanceOf[TestIndexFiberId])) {
      indexCacheBackend.get(fiber)
    } else {
      cacheBackend.get(fiber)
    }
  }

  def getIfPresent(fiber: FiberId): FiberCache = {
    if (separateMemory &&
      (fiber.isInstanceOf[BTreeFiberId] ||
        fiber.isInstanceOf[BitmapFiberId] ||
        fiber.isInstanceOf[TestIndexFiberId])) {
      indexCacheBackend.getIfPresent(fiber)
    } else {
      cacheBackend.getIfPresent(fiber)
    }
  }

  // only for unit test
  def setCompressionConf(dataEnable: Boolean = false,
      dataCompressCodec: String = "SNAPPY"): Unit = {
    _dataCacheCompressEnable = dataEnable
    _dataCacheCompressionCodec = dataCompressCodec
  }

  private[filecache] def freeFiber(fiberCache: FiberCache): Unit = {
    if (!fiberCache.isFailedMemoryBlock()) {
      freeFiberMemory(fiberCache)
    } else {
      fiberCache.resetColumn();
    }
    fiberLockManager.removeFiberLock(fiberCache.fiberId)
  }

  private[filecache] def allocateFiberMemory(fiberType: FiberType.FiberType,
    length: Long): MemoryBlockHolder = {
    fiberType match {
      case FiberType.DATA => cacheAllocator.allocateDataMemory(length)
      case FiberType.INDEX => cacheAllocator.allocateIndexMemory(length)
      case _ => throw new UnsupportedOperationException("Unsupported fiber type")
    }
  }

  private[filecache] def freeFiberMemory(fiberCache: FiberCache): Unit = {
    fiberCache.fiberType match {
      case FiberType.DATA => cacheAllocator.freeDataMemory(fiberCache.fiberData)
      case FiberType.INDEX => cacheAllocator.freeIndexMemory(fiberCache.fiberData)
      case _ => throw new UnsupportedOperationException("Unsupported fiber type")
    }
  }

  private[filecache] def getFiberLock(fiber: FiberId): ReentrantReadWriteLock = {
    fiberLockManager.getFiberLock(fiber)
  }

  private[filecache] def removeFiberLock(fiber: FiberId): Unit = {
    fiberLockManager.removeFiberLock(fiber)
  }

  @inline protected def toFiberCache(fiberType: FiberType.FiberType,
    bytes: Array[Byte]): FiberCache = {
    val block = allocateFiberMemory(fiberType, bytes.length)
    if (block.length != 0) {
      Platform.copyMemory(
        bytes,
        Platform.BYTE_ARRAY_OFFSET,
        block.baseObject,
        block.baseOffset,
        bytes.length)
      FiberCache(fiberType, block)
    } else {
      val fiberCache = FiberCache(fiberType, block)
      fiberCache.setOriginByteArray(bytes)
      fiberCache
    }
  }

  /**
   * Used by IndexFile
   */
  def toIndexFiberCache(in: FSDataInputStream, position: Long, length: Int): FiberCache = {
    val bytes = new Array[Byte](length)
    in.readFully(position, bytes)
    toFiberCache(FiberType.INDEX, bytes)
  }

  /**
   * Used by IndexFile. For decompressed data
   */
  def toIndexFiberCache(bytes: Array[Byte]): FiberCache = {
    toFiberCache(FiberType.INDEX, bytes)
  }

  /**
   * Used by OapDataFile since we need to parse the raw data in on-heap memory before put it into
   * off-heap memory
   */
  def toDataFiberCache(bytes: Array[Byte]): FiberCache = {
    toFiberCache(FiberType.DATA, bytes)
  }

  def getEmptyDataFiberCache(length: Long): FiberCache = {
    FiberCache(FiberType.DATA, allocateFiberMemory(FiberType.DATA, length))
  }

  def releaseIndexCache(indexName: String): Unit = {
    logDebug(s"Going to remove all index cache of $indexName")
    val fiberToBeRemoved = cacheBackend.getFibers.filter {
      case BTreeFiberId(_, file, _, _) => file.contains(indexName)
      case BitmapFiberId(_, file, _, _) => file.contains(indexName)
      case _ => false
    }
    if (separateMemory) {
      indexCacheBackend.invalidateAll(fiberToBeRemoved)
    } else {
      cacheBackend.invalidateAll(fiberToBeRemoved)
    }
    logDebug(s"Removed ${fiberToBeRemoved.size} fibers.")
  }

  def getCacheGuardian(): CacheGuardian = {
    cacheBackend.getCacheGuardian
  }

  def isDcpmmUsed(): Boolean = {
    cacheAllocator.isDcpmmUsed()
  }

  def isNeedWaitForFree(): Boolean = {
    logDebug(
      s"dcpmm wait threshold: " +
        s"${OapRuntime.getOrCreate.fiberCacheManager.dcpmmWaitingThreshold}, " +
        s"cache guardian pending size: " +
        s"${OapRuntime.getOrCreate.fiberCacheManager.pendingOccupiedSize}")
    isDcpmmUsed() &&
      (OapRuntime.getOrCreate.fiberCacheManager.pendingOccupiedSize >
      OapRuntime.getOrCreate.fiberCacheManager.dcpmmWaitingThreshold)
  }

  def releaseFiber(fiber: FiberId): Unit = {
    if (separateMemory &&
      (fiber.isInstanceOf[BTreeFiberId] ||
        fiber.isInstanceOf[BitmapFiberId] ||
        fiber.isInstanceOf[TestIndexFiberId])) {
      if (indexCacheBackend.getIfPresent(fiber) != null) {
        indexCacheBackend.invalidate(fiber)
      }
    }
    if (cacheBackend.getIfPresent(fiber) != null) {
      cacheBackend.invalidate(fiber)
    }
  }

  // Used by test suite
  private[oap] def clearAllFibers(): Unit = {
    if (separateMemory) {
      indexCacheBackend.cleanUp()
    }
    cacheBackend.cleanUp
  }

  // TODO: test case, consider data eviction, try not use DataFileMeta which may be costly
  private[sql] def status(): String = {
    logDebug(s"Reporting ${cacheBackend.cacheCount} fibers to the master")
    val dataFibers = {
      if (separateMemory) {
        (indexCacheBackend.getFibers.++(cacheBackend.getFibers)).collect {
          case fiber: DataFiberId => fiber
        }
      } else {
        cacheBackend.getFibers.collect {
          case fiber: DataFiberId => fiber
        }
      }
    }

    // Use a bit set to represent current cache status of one file.
    // Say, there is a file has 3 row groups and 3 columns. Then bit set size is 3 * 3 = 9
    // Say, cache status is below:
    //            field#0    field#1     field#2
    // group#0       -        cached        -          // BitSet(1 + 0 * 3) = 1
    // group#1       -        cached        -          // BitSet(1 + 1 * 3) = 1
    // group#2       -          -         cached       // BitSet(2 + 2 * 3) = 1
    // The final bit set is: 010010001
    val statusRawData = dataFibers.groupBy(_.file).map {
      case (dataFile, fiberSet) =>
        val fileMeta: DataFileMeta = OapRuntime.getOrCreate.dataFileMetaCacheManager.get(dataFile)
        val fiberBitSet = new OapBitSet(fileMeta.getGroupCount * fileMeta.getFieldCount)
        fiberSet.foreach(fiber =>
          fiberBitSet.set(fiber.columnIndex + fileMeta.getFieldCount * fiber.rowGroupId))
        FiberCacheStatus(dataFile.path, fiberBitSet, fileMeta.getGroupCount, fileMeta.getFieldCount)
    }.toSeq

    CacheStatusSerDe.serialize(statusRawData)
  }

  def cacheStats: CacheStats = {
    if (separateMemory) {
      indexCacheBackend.cacheStats + cacheBackend.cacheStats
    } else {
      cacheBackend.cacheStats
    }
  }

  def cacheSize: Long = {
    if (separateMemory) {
      indexCacheBackend.cacheSize + cacheBackend.cacheSize
    } else {
      cacheBackend.cacheSize
    }
  }

  def cacheCount: Long = {
    if (separateMemory) {
      indexCacheBackend.cacheCount + cacheBackend.cacheCount
    } else {
      cacheBackend.cacheCount
    }
  }

  // Get count of data cache, used by test suite
  private[oap] def dataCacheCount: Long = {
    if (separateMemory) {
      indexCacheBackend.dataCacheCount + cacheBackend.dataCacheCount
    } else {
      cacheBackend.dataCacheCount
    }
  }

  // Used by test suite
  private[filecache] def pendingCount: Int = {
    if (separateMemory) {
      indexCacheBackend.pendingFiberCount + cacheBackend.pendingFiberCount
    } else {
      cacheBackend.pendingFiberCount
    }
  }

  def pendingSize: Long = {
    if (separateMemory) {
      indexCacheBackend.pendingFiberSize + cacheBackend.pendingFiberSize
    } else {
      cacheBackend.pendingFiberSize
    }
  }

  def pendingOccupiedSize: Long = {
    if (separateMemory) {
      indexCacheBackend.pendingFiberOccupiedSize + cacheBackend.pendingFiberOccupiedSize
    } else {
      cacheBackend.pendingFiberOccupiedSize
    }
  }

  // A description of this FiberCacheManager for debugging.
  def toDebugString: String = {
    s"FiberCacheManager Statistics: { cacheCount=${dataCacheCount}, " +
        s"usedMemory=${Utils.bytesToString(cacheSize)}, ${cacheStats.toDebugString} }"
  }
}

private[sql] class DataFileMetaCacheManager extends Logging {
  type ENTRY = DataFile

  private val _cacheSize: AtomicLong = new AtomicLong(0)

  def cacheSize: Long = _cacheSize.get()

  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, DataFileMeta]() {
        override def onRemoval(n: RemovalNotification[ENTRY, DataFileMeta])
        : Unit = {
          logDebug(s"Evicting Data File Meta ${n.getKey.path}")
          _cacheSize.addAndGet(-n.getValue.len)
          n.getValue.close
        }
      })
      .build[ENTRY, DataFileMeta](new CacheLoader[ENTRY, DataFileMeta]() {
        override def load(entry: ENTRY)
        : DataFileMeta = {
          logDebug(s"Loading Data File Meta ${entry.path}")
          val meta = entry.getDataFileMeta()
          _cacheSize.addAndGet(meta.len)
          meta
        }
      })

  def get(fiberCache: DataFile): DataFileMeta = {
    cache.get(fiberCache)
  }

  def stop(): Unit = {
    cache.cleanUp()
  }
}

private[sql] class FiberLockManager {
  private val lockMap = new ConcurrentHashMap[FiberId, ReentrantReadWriteLock]()
  def getFiberLock(fiber: FiberId): ReentrantReadWriteLock = {
    var lock = lockMap.get(fiber)
    if (lock == null) {
      val newLock = new ReentrantReadWriteLock()
      val prevLock = lockMap.putIfAbsent(fiber, newLock)
      lock = if (prevLock == null) newLock else prevLock
    }
    lock
  }

  def removeFiberLock(fiber: FiberId): Unit = {
    lockMap.remove(fiber)
  }
}
