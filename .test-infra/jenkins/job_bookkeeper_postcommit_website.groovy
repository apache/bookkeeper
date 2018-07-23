/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import common_job_properties

// This job builds and publishes the website
job('bookkeeper_postcommit_publish_website') {
  description('Publish website to asf-site branch')

  // clean up the workspace before build
  wrappers { preBuildCleanup() }

  // Set common parameters.
  common_job_properties.setTopLevelWebsiteJobProperties(delegate)

  // Sets that this is a WebsitePostCommit job.
  common_job_properties.setWebsitePostCommit(delegate)

  steps {
    // Run the following shell script as a build step.
    shell '''
export MAVEN_HOME=/home/jenkins/tools/maven/latest
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH
export MAVEN_OPTS=-Xmx2048m
export JEKYLL_ENV=production

# CD site/
cd site

# Build the javadoc
make clean

# generate javadoc
make javadoc

# run the docker image to build the website
./docker/ci.sh

# publish website
source scripts/common.sh

ORIGIN_REPO=$(git remote show origin | grep 'Push  URL' | awk -F// '{print $NF}')
echo "ORIGIN_REPO: $ORIGIN_REPO"

(
  cd $APACHE_GENERATED_DIR

  rm -rf $TMP_DIR
  mkdir -p $TMP_DIR
  cd $TMP_DIR

  # clone the remote repo
  git clone "https://$ORIGIN_REPO" .
  git config user.name "Apache BookKeeper Site Updater"
  git config user.email "dev@bookkeeper.apache.org"
  git fetch origin
  git checkout asf-site
  git log | head
  # copy the apache generated dir
  cp -r $APACHE_GENERATED_DIR/content/* $TMP_DIR/content

  git add -A .
  git diff-index --quiet HEAD || (git commit -m "Updated site at revision $REVISION" && (git log | head) && git push -q origin HEAD:asf-site)

  rm -rf $TMP_DIR
)
    '''.stripIndent().trim()
  }
}
