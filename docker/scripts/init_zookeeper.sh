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

# zk env vars to replace values in config files
export ZK_dataDir=${ZK_dataDir:-"data/zookeeper/data"}
export ZK_dataLogDir=${ZK_dataLogDir:-"data/zookeeper/txlog"}
export ZK_standaloneEnabled=${ZK_standaloneEnabled:-"false"}
export ZK_dynamicConfigFile=${ZK_dynamicConfigFile:-"${BK_HOME}/conf/zookeeper.conf.dynamic"}
export ZK_SERVERS=${ZK_SERVERS:-"server.1=127.0.0.1:2888:3888:participant;0.0.0.0:2181"}
export ZK_ID=${ZK_ID:-"1"}

echo "Environment Vars for zookeeper:"
echo "  ZK_dataDir = ${ZK_dataDir}"
echo "  ZK_dataLogDir = ${ZK_dataLogDir}"
echo "  ZK_ID = ${ZK_ID}"
echo "  ZK_SERVERS = ${ZK_SERVERS}"
echo "  ZK_standaloneEnabled = ${ZK_standaloneEnabled}"
echo "  ZK_dynamicConfigFile = ${ZK_dynamicConfigFile}"

function create_zk_dirs() {
    mkdir -p "${ZK_dataDir}" "${ZK_dataLogDir}"
    echo "Created zookeeper dirs : "
    echo "  data = ${ZK_dataDir}"
    echo "  txnlog = ${ZK_dataLogDir}"

    if [[ ! -f "${ZK_dataDir}/myid" ]]; then
        echo "${ZK_ID}" > "${ZK_dataDir}/myid"
    fi

    # -------------- #
    # Allow the container to be started with `--user`
    if [ "$(id -u)" = '0' ]; then
        chown -R "${BK_USER}:${BK_USER}" "${ZK_dataDir}" "${ZK_dataLogDir}"
    fi
    # -------------- #
}

function create_zk_dynamic_conf() {
    for server in ${ZK_SERVERS}; do
        echo "$server" >> "${ZK_dynamicConfigFile}"
    done
}

function init_zookeeper() {

    # apply zookeeper envs
    python scripts/apply-config-from-env.py ${BK_HOME}/conf

    # create dirs if they don't exist
    create_zk_dirs

    # create dynamic config
    create_zk_dynamic_conf

}