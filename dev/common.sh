#!/usr/bin/env bash
#
# vim:et:ft=sh:sts=2:sw=2
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

function get_bk_version() {
    bk_version=$(mvn -q \
    -Dexec.executable="echo" \
    -Dexec.args='${project.version}' \
    --non-recursive \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec)
    echo ${bk_version}
}

function get_snapshot_version_with_gitsha() {
    local gitsha=$(git rev-parse --short HEAD)
    local commitdate=$(git log -1 --date=format:'%Y%m%d' --pretty=format:%cd)
    local version=$(get_bk_version)
    echo ${version/-SNAPSHOT/}-${commitdate}-${gitsha}-SNAPSHOT
}

export BK_DEV_DIR=`dirname "$0"`
export BK_HOME=`cd ${BK_DEV_DIR}/..;pwd`
export BK_VERSION=`get_bk_version`
