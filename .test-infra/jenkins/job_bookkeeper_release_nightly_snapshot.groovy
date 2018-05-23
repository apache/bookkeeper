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

// This job deploys a snapshot of latest master to artifactory nightly
freeStyleJob('bookkeeper_release_nightly_snapshot') {
  description('runs a `mvn clean deploy` of the nightly snapshot for bookkeeper.')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(
      delegate,
      'H 12 * * *',
      false)

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Release Snapshot',
      '/release-snapshot')

  steps {
    maven {
      // Set maven parameters.
      common_job_properties.setMavenConfig(delegate)

      // Maven build project.
      goals('clean apache-rat:check package spotbugs:check -Dmaven.test.failure.ignore=true deploy -Ddistributedlog -Dstream -DstreamTests -Pdocker')
    }

    // publish the docker images
    shell '''
export MAVEN_HOME=/home/jenkins/tools/maven/latest
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH
export MAVEN_OPTS=-Xmx2048m

./dev/publish-docker-images.sh
    '''.stripIndent().trim()
  }
}
