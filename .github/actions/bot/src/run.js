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

async function run(core, context, github) {

    try {
        const owner = process.env.PROVIDER;
        const repo = process.env.REPOSITORY;
        const reRunCmd = process.env.RERUN_CMD;
        const comment = context.payload.comment.body;

        if (comment !== reRunCmd) {
            console.log("this is not a bot command");
            return;
        }

        const {
            data: {
                head: {
                    sha: prRef,
                }
            }
        } = await github.pulls.get({
            owner,
            repo,
            pull_number: context.issue.number,
        });

        const jobs = await github.checks.listForRef({
            owner,
            repo,
            ref: prRef,
            status: "completed"
        });

        jobs.data.check_runs.forEach(job => {
            if (job.conclusion === 'failure' || job.conclusion === 'cancelled') {
                console.log("rerun job " + job.name);
                github.checks.rerequestSuite({
                    owner,
                    repo,
                    check_suite_id: job.check_suite.id
                })
            }
        });
    } catch (e) {
        core.setFailed(e);
    }

}

module.exports = ({core}, {context}, {github}) => {
    return run(core, context, github);
}
