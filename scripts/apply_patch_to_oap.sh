#!/bin/bash

CUR_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
OAP_HOME=$(dirname $CUR_DIR)
PATCH_DIR=$OAP_HOME/patches

GIT=$(command -v git)
check_git_exist() {
  if ! [ -x "$GIT" ]; then
    echo "ERROR: git is not found!"
    exit 1
  fi
}

NDCTL=$(command -v ndctl)
check_ndctl_exist() {
  if ! [ -x "$NDCTL" ]; then
    echo "ERROR: ndctl is not found!"
    exit 1
  fi
}

NUMACTL=$(command -v numactl)
check_numactl_exist() {
  if ! [ -x "$NUMACTL" ]; then
    echo "ERROR: numactl is not found!"
    exit 1
  fi
}

check_pmem_numa() {
  check_numactl_exist
  check_ndctl_exist

  numa_node_num=`$NUMACTL -H | grep "available" | cut -f2 -d" "`
  if [ -z $numa_node_num ]; then
    echo "The machine is not numa architecture. It does not help to apply the numa patch."
    exit 1
  fi

  pmem_initialized=`$NDCTL list -R`
  if [ -z "$pmem_initialized" ]; then
    echo "Please initialize the persistent memory with AppDirect Mode first."
    exit 1
  fi

  for ((i=0; i<$numa_node_num; i++));
  do
    pmem_device=`$NDCTL list -U $i | grep "blockdev" | cut -f2 -d":" | sed 's/"//g'`
    mount_point=`grep $pmem_device /proc/mounts | cut -f2 -d" "`
    echo "$pmem_device is associate with numa node $i"
    if [ -z $mount_point ]; then
      echo "Please mount /dev/$pmem_device at the initial path which numa node id is $i in persistent-memory.xml"
    else
      echo "Current /dev/$pmem_device mounted at $mount_point."
      echo "Please check if $mount_point is associate with numa node id $i in persistent-memory.xml."
    fi
  done
}

patch_to_oap() {
  check_git_exist
  cd $OAP_HOME

  if ! [ -d .git ]; then
    $GIT init
  fi

  $GIT checkout -- src/main/spark2.4.4/scala/org/apache/spark/SparkEnv.scala
  $GIT apply $PATCH_DIR/OAP-SparkEnv-numa-binding.patch
  if [ $? != 0 ]; then
    echo "Fail to apply the patch to oap. Please check if you have already patched."
  else
    echo "Apply patches to oap successfully."
  fi
}

check_pmem_numa
patch_to_oap
