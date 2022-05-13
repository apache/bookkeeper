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
find versioned_sidebars -name "version-${LATEST_RELEASED}-sidebars.json"
find versioned_docs -name "version-${LATEST_RELEASED}"

# replace sidebar items identifier
sed -i'.bak' "s/version-${LATEST_RELEASED}/version-${NEW_RELEASE}/" versioned_sidebars/$sidebar_file
rm "versioned_sidebars/${sidebar_file}.bak"
# rename sidebar file
mv versioned_sidebars/$sidebar_file versioned_sidebars/version-${NEW_RELEASE}-sidebars.json

# replace version in files
find versioned_docs/version-${LATEST_RELEASED} -type f -exec sed -i'.bak' "s/${LATEST_RELEASED}/${NEW_RELEASE}/g" {} +
find versioned_docs/version-${LATEST_RELEASED} -name "*.bak" | xargs rm
latest_released_md_link=${LATEST_RELEASED//./}
new_release_md_link=${NEW_RELEASE//./}
# replace Markdown links
find versioned_docs/version-${LATEST_RELEASED} -type f -exec sed -i'.bak' "s/#${latest_released_md_link}/#${new_release_md_link}/g" {} +
find versioned_docs/version-${LATEST_RELEASED} -name "*.bak" | xargs rm
# rename versioned_docs directory
mv versioned_docs/version-${LATEST_RELEASED} versioned_docs/version-${NEW_RELEASE}

# update versions.json
sed -i'.bak' "s/${LATEST_RELEASED}/${NEW_RELEASE}/" versions.json
rm versions.json.bak