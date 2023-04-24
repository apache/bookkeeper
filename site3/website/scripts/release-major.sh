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

if [[ -z $NEW_RELEASE ]]; then
  echo "env NEW_RELEASE not set"
  exit 1
fi

cd site3/website

sidebar_file="sidebars.json"
docs_dir="docs"

# create versioned sidebar file
cp $sidebar_file versioned_sidebars/version-${NEW_RELEASE}-sidebars.json
# add unique id for each item in the sidebar
sed -i'.bak' "s/\"id\": \"/\"id\": \"version-${NEW_RELEASE}\//" versioned_sidebars/version-${NEW_RELEASE}-sidebars.json
rm versioned_sidebars/version-${NEW_RELEASE}-sidebars.json.bak


# create new versioned_docs from "docs" dir
cp -R $docs_dir versioned_docs/version-${NEW_RELEASE}
# replace {{ site.latest_release }} with the current release
find versioned_docs/version-${NEW_RELEASE} -type f -exec sed -i'.bak' "s/{{ site.latest_release }}/${NEW_RELEASE}/g" {} +
find versioned_docs/version-${NEW_RELEASE} -name "*.bak" | xargs rm
# resolve release notes link
new_release_md_link=${NEW_RELEASE//./}
find versioned_docs/version-${NEW_RELEASE} -type f -exec sed -i'.bak' "s/(\/release-notes)/(\/release-notes#${new_release_md_link})/g" {} +
find versioned_docs/version-${NEW_RELEASE} -name "*.bak" | xargs rm

# update versions.json with the new release
node > versions.json <<EOF
var data = require('./versions.json');
data.unshift(process.env.NEW_RELEASE);
console.log(JSON.stringify(data));
EOF