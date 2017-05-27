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

PORT0=${PORT0:-$BOOKIE_PORT}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
BK_DIR=${BK_DIR:-"/bk"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}

# bk : zk:/bookkeeper/ledgers
BK_LEDGERS_PATH="/${BK_CLUSTER_NAME}/ledgers"

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_DIR is $BK_DIR"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"

sed -i 's/3181/'$PORT0'/' /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|journalDirectory=/tmp/bk-txn|journalDirectory='${BK_DIR}'/journal|' /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|ledgerDirectories=/tmp/bk-data|ledgerDirectories='${BK_DIR}'/ledgers|' /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|# zkLedgersRootPath=/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf

echo "wait for zookeeper"
until /opt/zk/zookeeper-3.5.2-alpha/bin/zkCli.sh -server $ZK_URL ls /; do sleep 2; done

echo "create the zk root"
/opt/zk/zookeeper-3.5.2-alpha/bin/zkCli.sh -server $ZK_URL create /${BK_CLUSTER_NAME}


#Format bookie metadata in zookeeper, the command should be run only once, because this command will clear all the bookies metadata in zk.
retString=`/opt/zk/zookeeper-3.5.2-alpha/bin/zkCli.sh -server $ZK_URL stat ${BK_LEDGERS_PATH}/available/readonly 2>&1`
echo $retString | grep "not exist"
if [ $? -eq 0 ]; then
    # create ephemeral zk node bkInitLock
    retString=`/opt/zk/zookeeper-3.5.2-alpha/bin/zkCli.sh -server $ZK_URL create -e /${BK_CLUSTER_NAME}/bkInitLock 2>&1`
    echo $retString | grep "Created"
    if [ $? -eq 0 ]; then
        # bkInitLock created success, this is the first bookie creating
        echo "Bookkeeper metadata not be formated before, do the format."
        BOOKIE_CONF=/opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf /opt/bookkeeper/bookkeeper-server-4.4.0/bin/bookkeeper shell metaformat -n -f
        /opt/zk/zookeeper-3.5.2-alpha/bin/zkCli.sh -server $ZK_URL delete /${BK_CLUSTER_NAME}/bkInitLock
    else
        # Wait 100s for other bookie do the format
        i=0
        while [ $i -lt 10 ]
        do
            sleep 10
            (( i++ ))
            retString=`/opt/zk/zookeeper-3.5.2-alpha/bin/zkCli.sh -server $ZK_URL stat ${BK_LEDGERS_PATH}/available/readonly 2>&1`
            echo $retString | grep "not exist"
            if [ $? -eq 0 ]; then
                echo "wait $i * 10 seconds, still not formated"
                continue
            else
                echo "wait $i * 10 seconds, bookkeeper formated"
                break
            fi

            echo "Waited 100 seconds for bookkeeper metaformat, something wrong, please check"
            exit
        done
    fi
else
    echo "Bookkeeper metadata be formated before, no need format"
fi

echo "start a new bookie"
# start bookie,
SERVICE_PORT=$PORT0 /opt/bookkeeper/bookkeeper-server-4.4.0/bin/bookkeeper bookie --conf /opt/bookkeeper/bookkeeper-server-4.4.0/conf/bk_server.conf

