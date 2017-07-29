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

source scripts/common.sh

ORIGIN_REPO=$(git remote show origin | grep 'Push  URL' | awk -F// '{print $NF}')
echo "ORIGIN_REPO: $ORIGIN_REPO"
STAGING_REPO=`echo $ORIGIN_REPO | sed -e 's/bookkeeper\.git/bookkeeper-staging-site.git/g'`
echo "STAGING_REPO: $STAGING_REPO"
echo "GENERATE SITE DIR: $LOCAL_GENERATED_DIR"

(
  rm -rf $TMP_DIR
  mkdir -p $TMP_DIR
  cp -r $LOCAL_GENERATED_DIR $TMP_DIR/docs
  cd $TMP_DIR

  git init

  git remote add upstream "https://$STAGING_REPO"
  git fetch upstream
  git reset upstream/master

  touch .

  git add -A .
  git commit -m "Updated site at revision $REVISION"
  git push -q upstream HEAD:master

  rm -rf $TMP_DIR
)
