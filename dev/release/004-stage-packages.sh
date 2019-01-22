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

set -e -x -u

BINDIR=`dirname "$0"`
BK_HOME=`cd $BINDIR/../..;pwd`

DIST_DEV_DIR="${BK_HOME}/target/dist_dev"

if [ -f ${BK_HOME}/target/dist_dev ]; then
    rm -r ${BK_HOME}/target/dist_dev 
fi

mkdir -p $DIST_DEV_DIR
cd $DIST_DEV_DIR

echo "Cloning dist/dev repo ..."
svn co https://dist.apache.org/repos/dist/dev/bookkeeper

SRC_DIR="${BK_HOME}/target/checkout" 
DEST_DIR="${DIST_DEV_DIR}/bookkeeper/${RC_DIR}"
echo "Create release directory ${RC_DIR} ..."
mkdir -p $DEST_DIR


echo "Generating asc files ..."
# bookkeeper-all doesn't generate `asc` file
gpg -ab ${SRC_DIR}/bookkeeper-dist/all/target/bookkeeper-all-${VERSION}-bin.tar.gz
echo "Generated asc files."

echo "Copying packages ..."
cp ${SRC_DIR}/bookkeeper-dist/target/bookkeeper-${VERSION}-src.tar.gz                   ${DEST_DIR}/bookkeeper-${VERSION}-src.tar.gz
cp ${SRC_DIR}/bookkeeper-dist/target/bookkeeper-${VERSION}-src.tar.gz.asc               ${DEST_DIR}/bookkeeper-${VERSION}-src.tar.gz.asc
cp ${SRC_DIR}/bookkeeper-dist/server/target/bookkeeper-server-${VERSION}-bin.tar.gz     ${DEST_DIR}/bookkeeper-server-${VERSION}-bin.tar.gz
cp ${SRC_DIR}/bookkeeper-dist/server/target/bookkeeper-server-${VERSION}-bin.tar.gz.asc ${DEST_DIR}/bookkeeper-server-${VERSION}-bin.tar.gz.asc
cp ${SRC_DIR}/bookkeeper-dist/all/target/bookkeeper-all-${VERSION}-bin.tar.gz           ${DEST_DIR}/bookkeeper-all-${VERSION}-bin.tar.gz
cp ${SRC_DIR}/bookkeeper-dist/all/target/bookkeeper-all-${VERSION}-bin.tar.gz.asc       ${DEST_DIR}/bookkeeper-all-${VERSION}-bin.tar.gz.asc
cp ${SRC_DIR}/bookkeeper-dist/bkctl/target/bkctl-${VERSION}-bin.tar.gz                  ${DEST_DIR}/bkctl-${VERSION}-bin.tar.gz
cp ${SRC_DIR}/bookkeeper-dist/bkctl/target/bkctl-${VERSION}-bin.tar.gz.asc              ${DEST_DIR}/bkctl-${VERSION}-bin.tar.gz.asc
echo "Copied packages."

echo "Generating sha512 files ..."
cd ${DEST_DIR}
shasum -a 512 bookkeeper-${VERSION}-src.tar.gz            > bookkeeper-${VERSION}-src.tar.gz.sha512
shasum -a 512 bookkeeper-server-${VERSION}-bin.tar.gz     > bookkeeper-server-${VERSION}-bin.tar.gz.sha512
shasum -a 512 bookkeeper-all-${VERSION}-bin.tar.gz        > bookkeeper-all-${VERSION}-bin.tar.gz.sha512
shasum -a 512 bkctl-${VERSION}-bin.tar.gz                 > bkctl-${VERSION}-bin.tar.gz.sha512
echo "Generated sha512 files."

cd ${DIST_DEV_DIR}/bookkeeper
svn add ${RC_DIR}
svn commit -m "Apache BookKeeper ${VERSION}, Release Candidate ${RC_NUM}"
