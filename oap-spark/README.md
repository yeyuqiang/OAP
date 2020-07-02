# RDD Cache PMem Extension
Please add feature description here.

## Contents
- [Introduction](#introduction)
- [User Guide](#userguide)

## Introduciton

OAP Spark support RDD Cache with Optane PMem. Spark has various storage levels serving for different purposes including memory and disk.

PMem storage level is added to support a new tier for storage level besides memory and disk.

Using PMem library to access Optane PMem can help to avoid the overhead from disk.

Large capacity and high I/O performance of PMem shows better performance than tied DRAM and disk solution under the same cost.

## User Guide
### Prerequisites

The following are required to configure OAP to use DCPMM cache.
- DCPMM hardware is successfully deployed on each node in cluster.
- Directories exposing DCPMM hardware on each socket. For example, on a two socket system the mounted DCPMM directories should appear as `/mnt/pmem0` and `/mnt/pmem1`. Correctly installed DCPMM must be formatted and mounted on every cluster worker node.

   ```
   // use impctl command to show topology and dimm info of DCPM
   impctl show -topology
   impctl show -dimm
   // provision dcpm in app direct mode
   ipmctl create -goal PersistentMemoryType=AppDirect
   // reboot system to make configuration take affect
   reboot
   // check capacity provisioned for app direct mode(AppDirectCapacity)
   impctl show -memoryresources
   // show the DCPM region information
   impctl show -region
   // create namespace based on the region, multi namespaces can be created on a single region
   ndctl create-namespace -m fsdax -r region0
   ndctl create-namespace -m fsdax -r region1
   // show the created namespaces
   fdisk -l
   // create and mount file system
   mount -o dax /dev/pmem0 /mnt/pmem0
   mount -o dax /dev/pmem1 /mnt/pmem1
   ```

   In this case file systems are generated for 2 numa nodes, which can be checked by "numactl --hardware". For a different number of numa nodes, a corresponding number of namespaces should be created to assure correct file system paths mapping to numa nodes.

- Make sure [Memkind](http://memkind.github.io/memkind/) library installed on every cluster worker node. Compile Memkind based on your system or directly place our pre-built binary of [libmemkind.so.0](https://github.com/Intel-bigdata/OAP/releases/download/v0.8.0-spark-2.4.4/libmemkind.so.0) for x86 64bit CentOS Linux in the `/lib64/`directory of each worker node in cluster.
   The Memkind library depends on `libnuma` at the runtime, so it must already exist in the worker node system.
   Build the latest memkind lib from source:

   ```
   git clone https://github.com/memkind/memkind
   cd memkind
   ./autogen.sh
   ./configure
   make
   make install
   ```

### Compiling

To build oap spark and oap common, you can run below commands:
```
cd ${OAP_CODE_HOME}
mvn clean package -Ppersistent-memory -DskipTests
```
You will find jar files under oap-common/target and oap-spark/target.

### Configuration

To enable rdd cache on Intel Optane PMem, you need add the following configurations:
```
spark.memory.pmem.initial.path [Your Optane PMem paths seperate with comma]
spark.memory.pmem.initial.size [Your Optane PMem size in GB]
spark.memory.pmem.usable.ratio [from 0 to 1, 0.85 is recommended]
spark.yarn.numa.enabled true
spark.yarn.numa.num [Your numa node number]

spark.files                       file://${{PATH_TO_OAP_SPARK_JAR}/oap-spark-${VERSION}.jar,file://${{PATH_TO_OAP_COMMON_JAR}/oap-common-${VERSION}.jar
spark.executor.extraClassPath     ./oap-spark-${VERSION}.jar:./oap-common-${VERSION}.jar
spark.driver.extraClassPath       file://${{PATH_TO_OAP_SPARK_JAR}/oap-spark-${VERSION}.jar:file://${{PATH_TO_OAP_COMMON_JAR}/oap-common-${VERSION}.jar
```

### Use Optane PMem to cache data

There's a new StorageLevel: PMEM_AND_DISK being added to cache data to Optane PMem, at the places you previously cache/persist data to memory, use PMEM_AND_DISK to substitute the previous StorageLevel, data will be cached to Optane PMem.
```
persist(StorageLevel.PMEM_AND_DISK)
```

### Run K-means benchmark

You can use [Hibench](https://github.com/Intel-bigdata/HiBench) to run K-means workload.

To verify whether Optane PMem RDD Cache works, please check the usage of /mnt/pmem0 and /mnt/pmem1.

### Limitations

For the scenario that data will exceed the block cache capacity. Memkind 1.9.0 and kernel 4.18 is recommended to avoid the unexpected issue.


### How to contribute

OAP Spark packages includes all Spark changed files. All codes are directly copied from
https://github.com/Intel-bigdata/Spark. Please make sure all your changes are committed to the
repository above. Otherwise, your change will be override by others.

The files from this package should avoid depending on other OAP module except OAP-Common.

All Spark source code changes are tracked in dev/changes_list/spark_changed_files

All changed files are ordered by file name.

You can execute the script dev/Apply_Spark_changes.sh with the specified Spark source directories
and OAP source directories accordingly.
