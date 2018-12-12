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
mavenJob('bookkeeper_precommit_pullrequest_java9') {
  description('precommit verification for pull requests of <a href="http://bookkeeper.apache.org">Apache BookKeeper</a> in Java 9.')

  // clean up the workspace before build
  wrappers { preBuildCleanup() }

  // Temporary information gathering to see if full disks are causing the builds to flake
  preBuildSteps {
    shell("id")
    shell("ulimit -a")
    shell("pwd")
    shell("df -h")
    shell("ps aux")
  }

  // Execute concurrent builds if necessary.
  concurrentBuild()

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(
    delegate,
    'master',
    'JDK 1.9 (latest)',
    200,
    'ubuntu',
    '${sha1}')

  // Sets that this is a PreCommit job.
  common_job_properties.setPreCommit(
    delegate,
    'Build (Java 9) (trigger via `rebuild java9`)',
    '.*(re)?build java9.*',
    '.*\\[x\\] \\[skip build java9\\].*')

  // Set Maven parameters.
  common_job_properties.setMavenConfig(delegate)

  // Maven build project
  goals('clean package spotbugs:check -Dstream -DskipBookKeeperServerTests')
}
