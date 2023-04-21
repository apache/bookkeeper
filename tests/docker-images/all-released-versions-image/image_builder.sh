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
IMAGE_NAME=apachebookkeeper/bookkeeper-all-released-versions:latest
FORCE_REBUILD="${BOOKKEEPER_DOCKER_IMAGES_FORCE_REBUILD:-false}"
if [[ "$FORCE_REBUILD" != "true" && "$(docker images -q $IMAGE_NAME 2> /dev/null)" != "" ]]; then
  echo "reusing local image: $IMAGE_NAME"
  exit 0
fi

SCRIPT_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

## BASE_DIR will be ./bookkeeper/
BASE_DIR=${SCRIPT_DIR}/../../../
docker build -t ${IMAGE_NAME} "${BASE_DIR}"/tests/docker-images/all-released-versions-image