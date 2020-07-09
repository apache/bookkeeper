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

// This is the Java postcommit which runs a maven install, and the current set of precommit tests.
freeStyleJob('bookkeeper_release_branch_410_integrationtests') {
    description('Run integration tests on bookkeeper branch-4.10.')

    // Set common parameters.
    common_job_properties.setTopLevelMainJobProperties(
        delegate,
        'branch-4.10',
        'JDK 1.8 (latest)',
        120)

    // Sets that this is a PostCommit job.
    common_job_properties.setPostCommit(
        delegate,
        'H 12 * * *',
        false)

    steps {
        // Temporary information gathering to see if full disks are causing the builds to flake
        shell('id')
        shell('ulimit -a')
        shell('pwd')
        shell('df -Th')
        shell('ps -eo euser,pid,ppid,pgid,start,pcpu,pmem,cmd')
        shell('docker network prune -f --filter name=testnetwork_*') // clean up any dangling networks from previous runs
        shell('docker system events > docker.log & echo $! > docker-log.pid')

        shell('docker pull apachebookkeeper/bookkeeper-all-released-versions:latest')

        // Build everything
        maven {
            // Set Maven parameters.
            common_job_properties.setMavenConfig(delegate)

            goals('-B clean install -Pdocker')
            properties(skipTests: true, interactiveMode: false)
        }

        maven {
            // Set Maven parameters.
            common_job_properties.setMavenConfig(delegate)
            rootPOM('tests/pom.xml')
            goals('-B test -DintegrationTests')
        }

        shell('kill $(cat docker-log.pid) || true')
    }

    publishers {
        archiveArtifacts {
            allowEmpty(true)
            pattern('**/target/container-logs/**')
            pattern('docker.log')
        }
        archiveJunit('**/surefire-reports/TEST-*.xml')
    }
}
