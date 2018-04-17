#!/bin/bash
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

export PATH=$PATH:/opt/bookkeeper/bin
export JAVA_HOME=/usr

# env var used often
PORT0=${PORT0:-${BOOKIE_PORT}}
PORT0=${PORT0:-3181}
BK_DATA_DIR=${BK_DATA_DIR:-"/data/bookkeeper"}
BK_CLUSTER_ROOT_PATH=${BK_CLUSTER_ROOT_PATH:-" "}

# env vars to replace values in config files
export BK_bookiePort=${BK_bookiePort:-${PORT0}}
export BK_zkServers=${BK_zkServers}
export BK_zkLedgersRootPath=${BK_zkLedgersRootPath:-"${BK_CLUSTER_ROOT_PATH}/ledgers"}
export BK_journalDirectory=${BK_journalDirectory:-${BK_DATA_DIR}/journal}
export BK_ledgerDirectories=${BK_ledgerDirectories:-${BK_DATA_DIR}/ledgers}
export BK_indexDirectories=${BK_indexDirectories:-${BK_DATA_DIR}/index}

echo "BK_bookiePort bookie service port is $BK_bookiePort"
echo "BK_zkServers is $BK_zkServers"
echo "BK_DATA_DIR is $BK_DATA_DIR"
echo "BK_CLUSTER_ROOT_PATH is $BK_CLUSTER_ROOT_PATH"


mkdir -p "${BK_journalDirectory}" "${BK_ledgerDirectories}" "${BK_indexDirectories}"
# -------------- #
# Allow the container to be started with `--user`
if [ "$1" = '/opt/bookkeeper/bin/bookkeeper' -a "$(id -u)" = '0' ]; then
    echo "This is root, will use user $BK_USER to run it"
    chown -R "$BK_USER:$BK_USER" "/opt/bookkeeper/" "${BK_journalDirectory}" "${BK_ledgerDirectories}" "${BK_indexDirectories}"
    chmod -R +x "/opt/bookkeeper/"
    sudo -s -E -u "$BK_USER" /bin/bash "$0" "$@"
    exit
fi
# -------------- #

python apply-config-from-env.py /opt/bookkeeper/conf

echo "wait for zookeeper"
until /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} ls /; do sleep 5; done

echo "create the zk root dir for bookkeeper"
/opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} create ${BK_CLUSTER_ROOT_PATH}

export BOOKIE_CONF=/opt/bookkeeper/conf/bk_server.conf
export SERVICE_PORT=$PORT0

# Init the cluster if required znodes not exist in Zookeeper.
# Use ephemeral zk node as lock to keep initialize atomic.
/opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_zkLedgersRootPath}/available/readonly
if [ $? -eq 0 ]; then
    echo "Metadata of cluster already exists, no need format"
else
    # create ephemeral zk node bkInitLock, initiator who this node, then do init; other initiators will wait.
    /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} create -e ${BK_CLUSTER_ROOT_PATH}/bkInitLock
    if [ $? -eq 0 ]; then
        # bkInitLock created success, this is the successor to do znode init
        echo "Bookkeeper znodes not exist in Zookeeper, do the init to create them."
        /opt/bookkeeper/bin/bookkeeper shell initnewcluster
        if [ $? -eq 0 ]; then
            echo "Bookkeeper znodes init success."
        else
            echo "Bookkeeper znodes init failed. please check the reason."
            exit
        fi
    else
        echo "Other docker instance is doing initialize at the same time, will wait in this instance."
        tenSeconds=1
        while [ ${tenSeconds} -lt 10 ]
        do
            sleep 10
            /opt/bookkeeper/bin/bookkeeper org.apache.zookeeper.ZooKeeperMain -server ${BK_zkServers} stat ${BK_zkLedgersRootPath}/available/readonly
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

echo "run command by exec"
exec "$@"

