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
set -e

# LATEST_RELEASED=4.14.4
# NEW_RELEASE=4.14.5


if [[ -z $LATEST_RELEASED ]]; then
  echo "env LATEST_RELEASED not set"
  exit 1
fi

if [[ -z $NEW_RELEASE ]]; then
  echo "env NEW_RELEASE not set"
  exit 1
fi


sidebar_file="version-${LATEST_RELEASED}-sidebars.json"

cd site3/website
find versioned_sidebars -name "version-${LATEST_RELEASED}-sidebars.json" || (echo "sidebar json file not found for ${LATEST_RELEASED}" && exit 1)
find versioned_docs -name "version-${LATEST_RELEASED}" || (echo "docs directory not found for ${LATEST_RELEASED}" && exit 1)

# Sidebar
sed -i '' "s/version-${LATEST_RELEASED}/version-${NEW_RELEASE}/" versioned_sidebars/$sidebar_file
mv versioned_sidebars/$sidebar_file versioned_sidebars/version-${NEW_RELEASE}-sidebars.json

# Docs
find versioned_docs/version-${LATEST_RELEASED} -type f -exec sed -i '' "s/${LATEST_RELEASED}/${NEW_RELEASE}/g" {} +
# markdown links
latest_released_md_link=${LATEST_RELEASED//./}
new_release_md_link=${NEW_RELEASE//./}
find versioned_docs/version-${LATEST_RELEASED} -type f -exec sed -i '' "s/#${latest_released_md_link}/#${new_release_md_link}/g" {} +
mv versioned_docs/version-${LATEST_RELEASED} versioned_docs/version-${NEW_RELEASE}

# Versions
sed -i '' "s/${LATEST_RELEASED}/${NEW_RELEASE}/" versions.json