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

###############################################################################
# Script to update the maven project version to a snapshot version with gitsha
# before publishing snapshot versions.
#
# usage: ./dev/update-snapshot-version.sh

source `dirname "$0"`/common.sh

OLD_VERSION=$(get_bk_version)
if [ "x${PUBLISH_GITSHA}" = "xtrue" ]; then
    NEW_VERSION=$(get_snapshot_version_with_gitsha)
    echo "Update version from ${OLD_VERSION} to ${NEW_VERSION}"

    mvn versions:set -DnewVersion=${NEW_VERSION}
    mvn versions:commit
fi
