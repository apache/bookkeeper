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

# env var used often
PORT0=${PORT0:-${BOOKIE_PORT}}
PORT0=${PORT0:-3181}
BK_DATA_DIR=${BK_DATA_DIR:-"/data/bookkeeper"}
BK_CLUSTER_ROOT_PATH=${BK_CLUSTER_ROOT_PATH:-""}

# bk env vars to replace values in config files
export BK_HOME=/opt/bookkeeper
export BK_bookiePort=${BK_bookiePort:-${PORT0}}
export BK_zkServers=${BK_zkServers}
export BK_zkLedgersRootPath=${BK_zkLedgersRootPath:-"${BK_CLUSTER_ROOT_PATH}/ledgers"}
export BK_journalDirectory=${BK_journalDirectory:-${BK_DATA_DIR}/journal}
export BK_ledgerDirectories=${BK_ledgerDirectories:-${BK_DATA_DIR}/ledgers}
export BK_indexDirectories=${BK_indexDirectories:-${BK_DATA_DIR}/index}
export BK_metadataServiceUri=${BK_metadataServiceUri:-"zk://${BK_zkServers}${BK_zkLedgersRootPath}"}
export BK_dlogRootPath=${BK_dlogRootPath:-"${BK_CLUSTER_ROOT_PATH}/distributedlog"}

echo "Environment Vars for bookie:"
echo "  BK_bookiePort bookie service port is $BK_bookiePort"
echo "  BK_zkServers is $BK_zkServers"
echo "  BK_DATA_DIR is $BK_DATA_DIR"
echo "  BK_CLUSTER_ROOT_PATH is $BK_CLUSTER_ROOT_PATH"
echo "  BK_metadataServiceUri is $BK_metadataServiceUri"
echo "  BK_dlogRootPath is $BK_dlogRootPath"

python scripts/apply-config-from-env.py ${BK_HOME}/conf

export BOOKIE_CONF=${BK_HOME}/conf/bk_server.conf
export SERVICE_PORT=${PORT0}

function create_bookie_dirs() {
    mkdir -p "${BK_journalDirectory}" "${BK_ledgerDirectories}" "${BK_indexDirectories}"
    echo "Created bookie dirs : "
    echo "  journal = ${BK_journalDirectory}"
    echo "  ledger = ${BK_ledgerDirectories}"
    echo "  index = ${BK_indexDirectories}"
    # -------------- #
    # Allow the container to be started with `--user`
    if [ "$(id -u)" = '0' ]; then
        chown -R "${BK_USER}:${BK_USER}" "${BK_journalDirectory}" "${BK_ledgerDirectories}" "${BK_indexDirectories}"
    fi
    # -------------- #
}
