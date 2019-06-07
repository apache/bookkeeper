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

// This is the Java precommit which runs a maven install, and the current set of precommit tests.
mavenJob('bookkeeper_precommit_pullrequest_java11') {
  description('precommit verification for pull requests of <a href="http://bookkeeper.apache.org">Apache BookKeeper</a> in Java 11.')

  // clean up the workspace before build
  wrappers { preBuildCleanup() }

  // Temporary information gathering to see if full disks are causing the builds to flake
  preBuildSteps {
    shell("id")
    shell("ulimit -a")
    shell("pwd")
    shell("df -Th")
    shell("ps aux")
  }

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    'JDK 11 (latest)',
    200,
    'ubuntu',
    '${sha1}')

  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(
    delegate,
    'Build (Java 11) (trigger via `rebuild java11`)',
    '.*(re)?build java11.*',
    '.*\\[x\\] \\[skip build java11\\].*')

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Maven build project
  // Spotbugs disabled on JDK11 due to https://github.com/spotbugs/spotbugs/issues/756
  goals('clean package -Dstream -DskipBookKeeperServerTests')
}
