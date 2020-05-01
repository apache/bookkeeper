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

function init_cluster() {
    zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
    if [ $? -eq 0 ]; then
        echo "Cluster metadata already exists"
    else
        # Create an ephemeral zk node `bkInitLock` for use as a lock.
        lock=`zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock '' true false false" ${BK_zkServers}`
        if [ -z "$lock" ]; then
            echo "znodes do not exist in Zookeeper for Bookkeeper. Initializing a new Bookkeekeper cluster in Zookeeper."
            /opt/bookkeeper/bin/bookkeeper shell initnewcluster
            if [ $? -eq 0 ]; then
                echo "initnewcluster operation succeeded"
            else
                echo "initnewcluster operation failed. Please check the reason."
                echo "Exit status of initnewcluster"
                echo $?
                exit
            fi
        else
            echo "Others may be initializing the cluster at the same time."
            tenSeconds=1
            while [ ${tenSeconds} -lt 100 ]
            do
                sleep 10
                zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
                if [ $? -eq 0 ]; then
                    echo "Waited $tenSeconds * 10 seconds. Successfully listed ''${BK_zkLedgersRootPath}/available/readonly'"
                    break
                else
                    echo "Waited $tenSeconds * 10 seconds. Continue waiting."
                    (( tenSeconds++ ))
                    continue
                fi
            done

            if [ ${tenSeconds} -eq 100 ]; then
                echo "Waited 100 seconds for bookkeeper cluster to initialize, but to no avail. Something is wrong, please check."
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

    # init the cluster
    init_cluster
}