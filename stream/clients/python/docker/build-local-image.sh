#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -x

CURRENT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=$( cd "${CURRENT_DIR}/../../../../" && pwd )
BK_TAR_BALL="bookkeeper-server-bin.tar.gz"

if [ -f "${CURRENT_DIR}/${BK_TAR_BALL}" ]; then
  rm "${CURRENT_DIR}/${BK_TAR_BALL}"
fi

cp ${ROOT_DIR}/bookkeeper-dist/server/target/bookkeeper-server*-bin.tar.gz "${CURRENT_DIR}/${BK_TAR_BALL}"

pushd "${CURRENT_DIR}"
docker build --build-arg BK_TAR_BALL=${BK_TAR_BALL} -t apache/bookkeeper:local .
popd
