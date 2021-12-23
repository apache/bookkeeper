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
set -e
IMAGE_NAME=apachebookkeeper/bookkeeper-current:latest
FORCE_REBUILD="${BOOKKEEPER_DOCKER_IMAGES_FORCE_REBUILD:-false}"
if [[ "$FORCE_REBUILD" != "true" && "$(docker images -q $IMAGE_NAME 2> /dev/null)" != "" ]]; then
  echo "reusing local image: $IMAGE_NAME"
  exit 0
fi
SCRIPT_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

## BASE_DIR will be ./bookkeeper/
BASE_DIR=${SCRIPT_DIR}/../../../

mkdir -p ${BASE_DIR}/tests/docker-images/current-version-image/build
OUTPUT_DIR=${BASE_DIR}/tests/docker-images/current-version-image/build/package
rm -rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

# Python Client
${BASE_DIR}/stream/clients/python/scripts/docker_build.sh

cp -Rp "${BASE_DIR}stream/clients/python/dist" "${OUTPUT_DIR}/bookkeeper-client"
cp -Rp "${BASE_DIR}docker/scripts" "${OUTPUT_DIR}/scripts"

VERSION=${1:-UNKNOWN}
cp -p ${BASE_DIR}bookkeeper-dist/server/build/distributions/bookkeeper-server-${VERSION}-bin.tar.gz "${OUTPUT_DIR}"/bookkeeper-dist-server-${VERSION}-bin.tar.gz

docker build -t ${IMAGE_NAME} --build-arg BK_VERSION="${VERSION}" "${BASE_DIR}"/tests/docker-images/current-version-image