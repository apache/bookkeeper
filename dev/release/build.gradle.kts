/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.github.vlsi.gradle.release.ReleaseExtension
import com.github.vlsi.gradle.release.ReleaseParams

plugins {
    id("com.github.vlsi.stage-vote-release")
}

rootProject.configure<ReleaseExtension> {
    voteText.set { it.voteTextGen() }
}

fun ReleaseParams.voteTextGen(): String = """
From: Release Manager
To: dev@bookkeeper.apache.org
Subject: [VOTE] Release $version, release candidate #$rc

Hi everyone,
Please review and vote on the release candidate $rc for the version $version, as follows:
[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)

The complete staging area is available for your review, which includes:
* Release notes [1]
  * ** Add release notes url **
* The official Apache source and binary distributions to be deployed to dist.apache.org
  * https://dist.apache.org/repos/dist/dev/bookkeeper/bookkeeper-$version-rc$rc/

* All artifacts to be deployed to the Maven Central Repository
  * https://repository.apache.org/content/repositories/staging/org/apache/bookkeeper/

* Source code tag "release-$version-rc$rc" [4] with git sha $gitSha

BookKeeper's KEYS file contains PGP keys we used to sign this release:
https://dist.apache.org/repos/dist/release/bookkeeper/KEYS

Please download these packages and review this release candidate:

- Review release notes
- Download the source package (verify shasum, and asc) and follow the instructions
  to build and run the bookkeeper service.
- Download the binary package (verify shasum, and asc) and follow the instructions
  to run the bookkeeper service.
- Review maven repo, release tag, licenses, and any other things you think it is
  important to a release.

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

Thanks,
Release Manager
""".trimIndent()

