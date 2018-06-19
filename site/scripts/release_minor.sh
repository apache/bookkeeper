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

BINDIR=`dirname "$0"`
DOC_HOME=`cd $BINDIR/..;pwd`

LATEST_RELEASE=`grep latest_release _config.yml | sed 's/^latest_release: "\(.*\)"/\1/'`
versions_list=(`echo $LATEST_RELEASE | tr '.' ' '`)
major_version=${versions_list[0]}
minor_version=${versions_list[1]}
patch_version=${versions_list[2]}
next_patch_version=$((patch_version + 1))
RELEASE_VERSION="${major_version}.${minor_version}.${next_patch_version}"

echo "Releasing version $RELEASE_VERSION ..."

# create a release directory

if [[ -d ${DOC_HOME}/docs/${RELEASE_VERSION} ]]; then
  echo "Release $RELEASE_VERSION docs dir '${DOC_HOME}/docs/${RELEASE_VERSION}' already exists."
  exit 1
fi

cp -r ${DOC_HOME}/docs/${LATEST_RELEASE} ${DOC_HOME}/docs/${RELEASE_VERSION}

# add the release to git repo
git add ${DOC_HOME}/docs/${RELEASE_VERSION}

cd ${DOC_HOME}/docs/${RELEASE_VERSION}

find . -name "*.md" | xargs sed -i'.bak' "s/${LATEST_RELEASE}/${RELEASE_VERSION}/"
find . -name "*.md.bak" | xargs rm

# go to doc home

cd ${DOC_HOME}

# insert release section
find releases.md | xargs sed -i'.bak' "/## News/r _data/releaseNotesSummary.template"
rm releases.md.bak

# bump the version in _config.yml
echo "- \"${RELEASE_VERSION}\"" > /tmp/bk_release_version
find _config.yml | xargs sed -i'.bak' "/^versions:/r /tmp/bk_release_version"
find _config.yml | xargs sed -i'.bak' "s/latest_release: \"${LATEST_RELEASE}\"/latest_release: \"${RELEASE_VERSION}\"/"
find _config.yml | xargs sed -i'.bak' "s/distributedlog_version: \"${LATEST_RELEASE}\"/distributedlog_version: \"${RELEASE_VERSION}\"/"
rm _config.yml.bak

echo "Released version $RELEASE_VERSION."
