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
PORT0=${PORT0:-$BK_PORT}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
BK_DATA_DIR=${BK_DATA_DIR:-"/bkdata"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_LEDGERS_PATH=${BK_LEDGERS_PATH:-"/${BK_CLUSTER_NAME}/ledgers"}

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_DATA_DIR is $BK_DATA_DIR"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"

# env vars to replace values in config files
export bookiePort=${bookiePort:-${PORT0}}
export zkServers=${zkServers:-${ZK_URL}}
export zkLedgersRootPath=${zkLedgersRootPath:-${BK_LEDGERS_PATH}}
export journalDirectory=${journalDirectory:-${BK_DATA_DIR}/journal}
export ledgerDirectories=${ledgerDirectories:-${BK_DATA_DIR}/ledgers}
export indexDirectories=${indexDirectories:-${BK_DATA_DIR}/index}

python apply-config-from-env.py /opt/bookkeeper/conf

echo "wait for zookeeper"
until /opt/zk/bin/zkCli.sh -server $ZK_URL ls /; do sleep 2; done

echo "create the zk root"
/opt/zk/bin/zkCli.sh -server $ZK_URL create /${BK_CLUSTER_NAME}

echo "format zk metadata"
echo "please ignore the failure, if it has already been formatted, "
export BOOKIE_CONF=/opt/bookkeeper/conf/bk_server.conf
export SERVICE_PORT=$PORT0
/opt/bookkeeper/bin/bookkeeper shell metaformat -n

echo "run command by exec"
exec "$@"

