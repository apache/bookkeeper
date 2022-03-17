#!/bin/bash
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

# NOTE: this is the script used by CI to push to apache.

ROOT_DIR=$(git rev-parse --show-toplevel)

ORIGIN_REPO=$(git remote show origin | grep 'Push  URL' | awk -F// '{print $NF}')
echo "ORIGIN_REPO: $ORIGIN_REPO"
TMP_DIR=/tmp/bookkeeper-site

(
  rm -rf $TMP_DIR
  mkdir -p $TMP_DIR
  cd $TMP_DIR

  # clone the remote repo
  git clone "https://$ORIGIN_REPO" .
  git config user.name "Apache BookKeeper Site Updater"
  git config user.email "dev@bookkeeper.apache.org"
  git checkout asf-staging
  # copy the apache generated dir
  mkdir -p content
  cp -r $ROOT_DIR/site3/website/build/* content

  git add -A .
  git diff-index --quiet HEAD || (git commit -m "Updated site at revision $REVISION" && git push -q origin HEAD:asf-staging)

  rm -rf $TMP_DIR
)
