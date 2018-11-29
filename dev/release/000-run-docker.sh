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

if [ $# = 0 ]; then
    cat <<EOF
Usage: ./dev/release/000-run-docker.sh <RC_NUM>
EOF
    exit 1;
fi

set -e -x -u

RC_NUM=$(($1 + 0)) 
shift

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export IMAGE_NAME="bookkeeper-release-build"

pushd ${SCRIPT_DIR}

docker build --rm=true -t ${IMAGE_NAME} .

popd

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=$USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
fi

docker build -t "${IMAGE_NAME}-${USER_NAME}" - <<UserSpecificDocker
FROM ${IMAGE_NAME}
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME} && \
  useradd -l -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
ENV  HOME /home/${USER_NAME}
UserSpecificDocker

BOOKKEEPER_ROOT=${SCRIPT_DIR}/../..

VERSION=`cd $BOOKKEEPER_ROOT && mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -Ev '(^\[|Download\w+:)' | sed 's/^\(.*\)-SNAPSHOT/\1/'`
versions_list=(`echo $VERSION | tr '.' ' '`)
major_version=${versions_list[0]}
minor_version=${versions_list[1]}
patch_version=${versions_list[2]}
next_minor_version=$((minor_version + 1))
MAJOR_VERSION="${major_version}.${minor_version}"
NEXT_VERSION="${major_version}.${next_minor_version}.0"
BRANCH_NAME="branch-${MAJOR_VERSION}"
DEVELOPMENT_VERSION="${NEXT_VERSION}-SNAPSHOT"

TAG="release-${VERSION}"
RC_DIR="bookkeeper-${VERSION}-rc${RC_NUM}"
RC_TAG="v${VERSION}-rc${RC_NUM}"

CMD="
gpg-agent --daemon --pinentry-program /usr/bin/pinentry --homedir \$HOME/.gnupg --use-standard-socket
echo
echo 'Welcome to Apache BookKeeper Release Build Env'
echo
echo 'Release $VERSION - RC$RC_NUM'
echo
echo 'Release Environment Variables:'
echo
echo 'VERSION                   = $VERSION'
echo 'MAJOR_VERSION             = $MAJOR_VERSION'
echo 'NEXT_VERSION              = $NEXT_VERSION'
echo 'DEVELOPMENT_VERSION       = $DEVELOPMENT_VERSION'
echo 'BRANCH_NAME               = $BRANCH_NAME'
echo 'TAG                       = $TAG'
echo 'RC_NUM                    = $RC_NUM'
echo 'RC_DIR                    = $RC_DIR'
echo 'RC_TAG                    = $RC_TAG'
echo
echo 'Before executing any release scripts, PLEASE configure your git to cache your github password:'
echo
echo ' // configure credential helper to cache your github password for 1 hr during the whole release process '
echo ' \$ git config --global credential.helper \"cache --timeout=3600\" '
echo ' \$ git push apache --dry-run '
echo
bash
"

pushd ${BOOKKEEPER_ROOT}

docker run -i -t \
  --rm=true \
  -w ${BOOKKEEPER_ROOT} \
  -u "${USER}" \
  -v "$(realpath $BOOKKEEPER_ROOT):${BOOKKEEPER_ROOT}" \
  -v "$(realpath ~):/home/${USER_NAME}" \
  -e VERSION=${VERSION} \
  -e MAJOR_VERSION=${MAJOR_VERSION} \
  -e NEXT_VERSION=${NEXT_VERSION} \
  -e BRANCH_NAME=${BRANCH_NAME} \
  -e DEVELOPMENT_VERSION=${DEVELOPMENT_VERSION} \
  -e RC_NUM=${RC_NUM} \
  -e TAG=${TAG} \
  -e RC_TAG=${RC_TAG} \
  -e RC_DIR=${RC_DIR} \
  ${IMAGE_NAME}-${USER_NAME} \
  bash -c "${CMD}"

popd

