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
export JAVA_HOME=/usr/lib/jvm/jre-1.8.0

BK_HOME=/opt/bookkeeper
BINDIR=${BK_HOME}/bin
BOOKKEEPER=${BINDIR}/bookkeeper
SCRIPTS_DIR=${BK_HOME}/scripts

if [ $# = 0 ]; then
    echo "No command is found";
    exit 1;
fi

COMMAND=$1
shift

function run_command() {
    if [ "$(id -u)" = '0' ]; then
        chown -R "$BK_USER:$BK_USER" ${BK_HOME}
        chmod -R +x ${BINDIR}
        chmod -R +x ${SCRIPTS_DIR}
        echo "This is root, will use user $BK_USER to run command '$@'"
        sudo -s -E -u "$BK_USER" /bin/bash "$@"
        exit
    else
        echo "Run command '$@'"
        $@
    fi
}

# for BC consideration - the old scripts run `entrypoint.sh /opt/bookkeeper/bin/bookkeeper bookie`
if [ ${COMMAND} == "${BOOKKEEPER}" ]; then
    if [ $# -gt 0 ]; then
        if [ $1 == "bookie" ]; then
            source ${SCRIPTS_DIR}/init_bookie.sh
            init_bookie
            run_command /opt/bookkeeper/bin/bookkeeper bookie
            exit
        fi
    fi
fi

if [ -f ${SCRIPTS_DIR}/init_${COMMAND}.sh ]; then
    source ${SCRIPTS_DIR}/init_${COMMAND}.sh
    init_${COMMAND}
    run_command ${BOOKKEEPER} ${COMMAND} $@
else
    source ${SCRIPTS_DIR}/init_generic.sh
    run_command ${COMMAND} $@
fi
