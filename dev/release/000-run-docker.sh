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
FROM --platform=linux/amd64 ${IMAGE_NAME}
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME} && \
  useradd -l -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME} && \
  ([ "$(dirname "$HOME")" -eq "/home" ] || ln -s /home $(dirname "$HOME")) && \
  mkdir -p /gpg && chown ${USER_ID}:${GROUP_ID} /gpg && chmod 700 /gpg
ENV  HOME /home/${USER_NAME}
UserSpecificDocker

BOOKKEEPER_ROOT=${SCRIPT_DIR}/../..

VERSION=`cd $BOOKKEEPER_ROOT && mvn initialize help:evaluate -Dexpression=project.version -pl . -q -DforceStdout | grep -Ev '(^\[|Download\w+:)' | sed 's/^\(.*\)-SNAPSHOT/\1/'`
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
# copy ~/.gnupg to /gpg in the container to workaround issue with permissions
cp -Rdp \$HOME/.gnupg /gpg
# remove any previous sockets
rm -rf /gpg/.gnupg/S.*
# set GNUPGHOME to /gpg/.gnupg
export GNUPGHOME=/gpg/.gnupg
gpg-agent --daemon --pinentry-program /usr/bin/pinentry --allow-loopback-pinentry --default-cache-ttl 3600
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
echo ' // take a backup of ~/.gitconfig, remember to restore it after the release process'
echo ' \$ cp ~/.gitconfig ~/.gitconfig.bak.\$(date -I)'
echo ' // remove any previous credential helper configuration'
echo ' \$ git config --global -l --name-only | grep credential | uniq | xargs -i{} git config --global --unset-all {}'
echo ' // fix permission warning with git in docker on MacOS'
echo ' \$ git config --global --add safe.directory $PWD'
echo ' \$ git config --global --add safe.directory \$PWD'
echo ' // configure credential helper to cache your github password for 1 hr during the whole release process '
echo ' \$ git config --global credential.helper \"cache --timeout=3600\" '
echo ' // in another terminal get a GitHub token to be used as a password for the release process, assuming you are using GitHub CLI.'
echo ' \$ gh auth token '
echo ' // attempt to push to apache remote to cache your password'
echo ' \$ git push apache HEAD:test --dry-run '
echo ' // cache gpg password by signing a dummy file'
echo ' \$ echo dummy > /tmp/dummy && gpg -sa /tmp/dummy'
echo
bash
"

pushd ${BOOKKEEPER_ROOT}
echo $BOOKKEEPER_ROOT

docker run -i -t \
  --rm=true \
  -w ${BOOKKEEPER_ROOT} \
  -u "${USER}" \
  -v "$BOOKKEEPER_ROOT:${BOOKKEEPER_ROOT}" \
  -v "$(realpath ~/):/home/${USER_NAME}" \
  -e MAVEN_CONFIG=/home/${USER_NAME}/.m2 \
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

