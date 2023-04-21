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
# Script to publish docker images to docker hub. This script is run at Jenkins
# after building the current images.
#
# The script is sourced from Apache Pulsar (incubating).
# https://github.com/apache/incubator-pulsar/blob/master/docker/publish.sh
#
# usage: ./dev/publish-docker-images.sh

source `dirname "$0"`/common.sh

if [ -z "${DOCKER_USER}" ]; then
    echo "Docker user in variable \$DOCKER_USER was not set. Skipping image publishing"
    exit 1
fi

if [ -z "${DOCKER_PASSWORD}" ]; then
    echo "Docker password in variable \$DOCKER_PASSWORD was not set. Skipping image publishing"
    exit 1
fi

DOCKER_ORG="${DOCKER_ORG:-apachebookkeeper}"

docker login ${DOCKER_REGISTRY} -u="${DOCKER_USER}" -p="${DOCKER_PASSWORD}"
if [ $? -ne 0 ]; then
    echo "Failed to loging to Docker Hub"
    exit 1
fi

echo "BookKeeper Version: ${BK_VERSION}"

if [[ -z ${DOCKER_REGISTRY} ]]; then
    docker_registry_org=${DOCKER_ORG}
else
    docker_registry_org=${DOCKER_REGISTRY}/${DOCKER_ORG}
fi
echo "Starting to push images to ${docker_registry_org} as user ${DOCKER_USER}..."

set -x

# Fail if any of the subsequent commands fail
set -e

# Publish the current image
docker push ${docker_registry_org}/bookkeeper-current:latest
docker push ${docker_registry_org}/bookkeeper-current:$BK_VERSION

echo "Finished pushing images to ${docker_registry_org}"
