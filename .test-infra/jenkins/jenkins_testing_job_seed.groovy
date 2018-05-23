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

job('bookkeeper-jenkins-testing-seed') {
  description('Seed job, which allows DSL jobs to be tested before being pushed for review')

  // Source code management.
  scm {
    git {
      remote {
        url('${gitrepo}')
        refspec('+refs/heads/*:refs/remotes/origin/*')
      }
      branch('${sha1}')
      extensions {
        cleanAfterCheckout()
      }
    }
  }

  parameters {
    stringParam(
      'gitrepo', 'https://github.com/apache/bookkeeper/', 'Repo to clone')

    stringParam(
      'sha1',
      'master',
      'Commit id or refname (eg: origin/pr/9/head) you want to build.')
  }

  steps {
    dsl {
      // A list or a glob of other groovy files to process.
      external('.test-infra/jenkins/jenkins_testing_job_*.groovy')
      lookupStrategy('SEED_JOB')
      // If a job is removed from the script, delete it
      removeAction('DELETE')
    }
  }
}
