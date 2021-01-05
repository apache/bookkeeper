#!/bin/bash
#
#/**
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

SCRIPT_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

## BASE_DIR will be ./bookkeeper/
BASE_DIR=${SCRIPT_DIR}/../../../
mkdir "${BASE_DIR}"/tests/docker-images/statestore-image/dist
mkdir "${BASE_DIR}"/tests/docker-images/statestore-image/scripts
mkdir "${BASE_DIR}"/tests/docker-images/statestore-image/temp_conf
mkdir "${BASE_DIR}"/tests/docker-images/statestore-image/temp_bin

cp "${BASE_DIR}"/stream/server/build/distributions/server.tar "${BASE_DIR}"/tests/docker-images/statestore-image/dist
cp "${BASE_DIR}"/docker/scripts/* "${BASE_DIR}"/tests/docker-images/statestore-image/scripts
cp "${BASE_DIR}"/conf/* "${BASE_DIR}"/tests/docker-images/statestore-image/temp_conf
cp "${BASE_DIR}"/bin/* "${BASE_DIR}"/tests/docker-images/statestore-image/temp_bin
docker build -t apachebookkeeper/bookkeeper-current:latest "${BASE_DIR}"/tests/docker-images/statestore-image

rm -rf "${BASE_DIR}"/tests/docker-images/statestore-image/dist
rm -rf "${BASE_DIR}"/tests/docker-images/statestore-image/scripts
rm -rf "${BASE_DIR}"/tests/docker-images/statestore-image/temp_conf
rm -rf "${BASE_DIR}"/tests/docker-images/statestore-image/temp_bin