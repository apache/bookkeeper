#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
set -x
source ${SCRIPTS_DIR}/common.sh

function wait_for_zookeeper() {
    echo "wait for zookeeper"
    until zk-shell --run-once "ls /" ${BK_zkServers}; do sleep 5; done
}

function create_zk_root() {
    if [ "x${BK_CLUSTER_ROOT_PATH}" != "x" ]; then
        echo "create the zk root dir for bookkeeper at '${BK_CLUSTER_ROOT_PATH}'"
        zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH} '' false false true" ${BK_zkServers}
    fi
}

function create_zk_stream_storage_root() {
    if [ "x${BK_STREAM_STORAGE_ROOT_PATH}" != "x" ]; then
        echo "create the zk stream storage dir for bookkeeper at '${BK_STREAM_STORAGE_ROOT_PATH}'"
        zk-shell --run-once "create ${BK_STREAM_STORAGE_ROOT_PATH} '' false false true" ${BK_zkServers}
    fi
}

format_bookie_data_and_metadata() {
    if [ `find $BK_journalDirectory $BK_ledgerDirectories $BK_indexDirectories -type f 2> /dev/null | wc -l` -gt 0 ]; then
      # The container already contains data in BK directories. This is probably because
      # the container has been restarted; or, if running on Kubernetes, it has probably been
      # updated or evacuated without losing its persistent volumes.
      echo "Data available in bookkeeper directories; not formatting the bookie"
    else
      # The container does not contain any BK data, it is probably a new
      # bookie. We will format any pre-existent data and metadata before starting
      # the bookie to avoid potential conflicts.
      echo "Formatting bookie data and metadata"
      /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie || true
    fi
}

# Init the cluster if required znodes not exist in Zookeeper.
# Use ephemeral zk node as lock to keep initialize atomic.
function init_cluster() {
    if [ "x${BK_STREAM_STORAGE_ROOT_PATH}" == "x" ]; then
        echo "BK_STREAM_STORAGE_ROOT_PATH is not set. fail fast."
        exit -1
    fi

    # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_STREAM_STORAGE_ROOT_PATH}
    zk-shell --run-once "ls ${BK_STREAM_STORAGE_ROOT_PATH}" ${BK_zkServers}
    if [ $? -eq 0 ]; then
        echo "Metadata of cluster already exists, no need to init"
    else
        # create ephemeral zk node bkInitLock, initiator who this node, then do init; other initiators will wait.
        # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} create -e ${BK_CLUSTER_ROOT_PATH}/bkInitLock
        lock=`zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock '' true false false" ${BK_zkServers}`
        if [ -z "$lock" ]; then
            # bkInitLock created success, this is the successor to do znode init
            echo "Initializing bookkeeper cluster at service uri ${BK_metadataServiceUri}."
            #/opt/bookkeeper/bin/bkctl --service-uri ${BK_metadataServiceUri} cluster init
            /opt/bookkeeper/bin/bookkeeper shell initnewcluster
            if [ $? -eq 0 ]; then
                echo "Successfully initialized bookkeeper cluster at service uri ${BK_metadataServiceUri}."
            else
                echo "Failed to initialize bookkeeper cluster at service uri ${BK_metadataServiceUri}. please check the reason."
                exit
            fi
        else
            echo "Other docker instance is doing initialize at the same time, will wait in this instance."
            tenSeconds=1
            while [ ${tenSeconds} -lt 10 ]
            do
                sleep 10
                echo "run '/opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_STREAM_STORAGE_ROOT_PATH}'"
                # /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_STREAM_STORAGE_ROOT_PATH}
                zk-shell --run-once "ls ${BK_STREAM_STORAGE_ROOT_PATH}" ${BK_zkServers}
                if [ $? -eq 0 ]; then
                    echo "Waited $tenSeconds * 10 seconds, bookkeeper inited"
                    break
                else
                    echo "Waited $tenSeconds * 10 seconds, still not init"
                    (( tenSeconds++ ))
                    continue
                fi
            done

            if [ ${tenSeconds} -eq 10 ]; then
                echo "Waited 100 seconds for bookkeeper cluster init, something wrong, please check"
                exit
            fi
        fi
    fi
}

function init_bookie() {

    # create dirs if they don't exist
    create_bookie_dirs

    # wait zookeeper to run
    wait_for_zookeeper

    # create zookeeper root
    create_zk_root

    # create zookeeper stream storage root
    # create_zk_stream_storage_root

    # init the cluster
    init_cluster
}