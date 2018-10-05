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

// This job runs postcommit tests on python client
freeStyleJob('bookkeeper_postcommit_master_python') {
  description('Runs nightly build for bookkeeper python client.')

  // clean up the workspace before build
  wrappers { preBuildCleanup() }

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate, 'master', 'JDK 1.8 (latest)')

  throttleConcurrentBuilds {
    // limit builds to 1 per node to avoid conflicts on building docker images
    maxPerNode(1)
  }

  // Sets that this is a PostCommit job.
  common_job_properties.setPostCommit(
      delegate,
      'H 12 * * *',
      false)

  steps {
    shell('.test-infra/scripts/pre-docker-tests.sh')
    shell('docker pull python:3.7')
    shell('docker pull python:3.6')
    shell('docker pull python:3.5')
    shell('docker pull python:2.7')
    shell('./stream/clients/python/scripts/test.sh')
    shell('.test-infra/scripts/post-docker-tests.sh')
  }
}
