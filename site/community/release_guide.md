# Apache BookKeeper Release Guide

* TOC
{:toc}

This page documents the procedure to make an Apache BookKeeper release.

## Introduction

The Apache BookKeeper project periodically declares and publishes releases. A release is one or more packages of the project artifact(s) that are approved for general public distribution and use. They may come with various degrees of caveat regarding their perceived quality and potential for change, such as “alpha”, “beta”, “incubating”, “stable”, etc.

The BookKeeper community treats releases with great importance. They are a public face of the project and most users interact with the project only through the releases. Releases are signed off by the entire BookKeeper community in a public vote.

Each release is executed by a *Release Manager*, who is selected among the [BookKeeper committers](http://bookkeeper.apache.org/credits.html). This document describes the process that the Release Manager follows to perform a release. Any changes to this process should be discussed and adopted on the [dev@ mailing list](http://bookkeeper.apache.org/lists.html).

Please remember that publishing software has legal consequences. This guide complements the foundation-wide [Product Release Policy](http://www.apache.org/dev/release.html) and [Release Distribution Policy](http://www.apache.org/dev/release-distribution).

## Overview

The release process consists of several steps:

1. Decide to release
2. Prepare for the release
3. Build a release candidate
4. Vote on the release candidate
5. If necessary, fix any issues and go back to step 3.
6. Finalize the release
7. Promote the release

**********

## Decide to release

Deciding to release and selecting a Release Manager is the first step of the release process. This is a consensus-based decision of the entire community.

Anybody can propose a release on the dev@ mailing list, giving a solid argument and nominating a committer as the Release Manager (including themselves). There’s no formal process, no vote requirements, and no timing requirements. Any objections should be resolved by consensus before starting the release.

In general, the community prefers to have a rotating set of 3-5 Release Managers. Keeping a small core set of managers allows enough people to build expertise in this area and improve processes over time, without Release Managers needing to re-learn the processes for each release. That said, if you are a committer interested in serving the community in this way, please reach out to the community on the dev@ mailing list.

### Checklist to proceed to the next step

1. Community agrees to release
2. Community selects a Release Manager

**********

## Prepare for the release

Before your first release, you should perform one-time configuration steps. This will set up your security keys for signing the release and access to various release repositories.

To prepare for each release, you should audit the project status both in the JIRA issue tracker and the Github issue tracker, and do necessary bookkeeping. Finally, you should create a release branch from which individual release candidates will be built.

### One-time setup instructions

#### GPG Key

You need to have a GPG key to sign the release artifacts. Please be aware of the ASF-wide [release signing guidelines](https://www.apache.org/dev/release-signing.html). If you don’t have a GPG key associated with your Apache account, please create one according to the guidelines.

Determine your Apache GPG Key and Key ID, as follows:

    gpg --list-keys

This will list your GPG keys. One of these should reflect your Apache account, for example:

    --------------------------------------------------
    pub   2048R/845E6689 2016-02-23
    uid                  Nomen Nescio <anonymous@apache.org>
    sub   2048R/BA4D50BE 2016-02-23

Here, the key ID is the 8-digit hex string in the `pub` line: `845E6689`.

Now, add your Apache GPG key to the BookKeeper’s `KEYS` file in [`dist`](https://svn.apache.org/repos/asf/bookkeeper/dist/KEYS). Follow the instructions listed at the top of these files.

Configure `git` to use this key when signing code by giving it your key ID, as follows:

    git config --global user.signingkey 845E6689

You may drop the `--global` option if you’d prefer to use this key for the current repository only.

You may wish to start `gpg-agent` to unlock your GPG key only once using your passphrase. Otherwise, you may need to enter this passphrase hundreds of times. The setup for `gpg-agent` varies based on operating system, but may be something like this:

    eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
    export GPG_TTY=$(tty)
    export GPG_AGENT_INFO

#### Access to Apache Nexus repository

Configure access to the [Apache Nexus repository](http://repository.apache.org/), which enables final deployment of releases to the Maven Central Repository.

1. You log in with your Apache account.
2. Confirm you have appropriate access by finding `org.apache.bookkeeper` under `Staging Profiles`.
3. Navigate to your `Profile` (top right dropdown menu of the page).
4. Choose `User Token` from the dropdown, then click `Access User Token`. Copy a snippet of the Maven XML configuration block.
5. Insert this snippet twice into your global Maven `settings.xml` file, typically `${HOME}/.m2/settings.xml`. The end result should look like this, where `TOKEN_NAME` and `TOKEN_PASSWORD` are your secret tokens:

        <settings>
          <servers>
            <server>
              <id>apache.releases.https</id>
              <username>TOKEN_NAME</username>
              <password>TOKEN_PASSWORD</password>
            </server>
            <server>
              <id>apache.snapshots.https</id>
              <username>TOKEN_NAME</username>
              <password>TOKEN_PASSWORD</password>
            </server>
          </servers>
        </settings>

#### Website development setup

[TBD]

### Create a new version in JIRA and Github

When contributors resolve an issue in JIRA, they are tagging it with a release that will contain their changes. With the release currently underway, new issues should be resolved against a subsequent future release. Therefore, you should create a release item for this subsequent release, as follows:

1. In JIRA, navigate to the [`BookKeeper > Administration > Versions`](https://issues.apache.org/jira/plugins/servlet/project-config/BOOKKEEPER/versions).
2. Add a new release: choose the next minor version number compared to the one currently underway, select today’s date as the `Start Date`, and choose `Add`.
3. In Github, navigate to the [`Issues > Milestones`](https://github.com/apache/bookkeeper/milestones).
4. Add a new milestone: choose the next minor version number compared to the one currently underway, select a day that is 3-months from now as the `Due Date`, write a description `Release x.y.z` and choose `Create milestone`.

### Triage release-blocking issues in JIRA and Github

#### JIRA

There could be outstanding release-blocking issues, which should be triaged before proceeding to build a release candidate. We track them by assigning a specific `Fix version` field even before the issue resolved.

The list of release-blocking issues is available at the [version status page](https://issues.apache.org/jira/browse/BOOKKEEPER/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel). Triage each unresolved issue with one of the following resolutions:

* If the issue has been resolved and JIRA was not updated, resolve it accordingly.
* If the issue has not been resolved and it is acceptable to defer this until the next release, update the `Fix Version` field to the new version you just created. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.
* If the issue has not been resolved and it is not acceptable to release until it is fixed, the release cannot proceed. Instead, work with the BookKeeper community to resolve the issue.

#### Github

There could be outstanding release-blocking issues, which should be triaged before proceeding to build a release candidate. We track them by assigning a specific `Milestone` field even before the issue resolved.

The list of release-blocking issues is available at the [milestones page](https://github.com/apache/bookkeeper/milestones). Triage each unresolved issue with one of the following resolutions:

* If the issue has been resolved and was not updated, close it accordingly.
* If the issue has not been resolved and it is acceptable to defer this until the next release, update the `Milestone` field to the new milestone you just created. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.
* If the issue has not been resolved and it is not acceptable to release until it is fixed, the release cannot proceed. Instead, work with the BookKeeper community to resolve the issue.

### Review Release Notes in JIRA and Github

#### JIRA

JIRA automatically generates Release Notes based on the `Fix Version` field applied to issues. Release Notes are intended for BookKeeper users (not BookKeeper committers/contributors). You should ensure that Release Notes are informative and useful.

Open the release notes from the [version status page](https://issues.apache.org/jira/browse/BOOKKEEPER/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel) by choosing the release underway and clicking Release Notes.

You should verify that the issues listed automatically by JIRA are appropriate to appear in the Release Notes. Specifically, issues should:

* Be appropriately classified as `Bug`, `New Feature`, `Improvement`, etc.
* Represent noteworthy user-facing changes, such as new functionality, backward-incompatible API changes, or performance improvements.
* Have occurred since the previous release; an issue that was introduced and fixed between releases should not appear in the Release Notes.
* Have an issue title that makes sense when read on its own.

Adjust any of the above properties to the improve clarity and presentation of the Release Notes.

#### Github

Unlike JIRA, Github does not automatically generates Release Notes based on the `Milestone` field applied to issues.
We can use [github-changelog-generator](https://github.com/skywinder/github-changelog-generator) to generate a ChangeLog for a milestone.

(instructions to generate changelogs)

### Create a release branch

Release candidates are built from a release branch. As a final step in preparation for the release, you should create the release branch, push it to the code repository, and update version information on the original branch.

Check out the version of the codebase from which you start the release. For a new minor or major release, this may be `HEAD` of the `master` branch. To build a hotfix/incremental release, instead of the `master` branch, use the release tag of the release being patched. (Please make sure your cloned repository is up-to-date before starting.)

    git checkout <master branch OR release tag>


Set up a few environment variables to simplify Maven commands that follow. (We use `bash` Unix syntax in this guide.)

    MAJOR_VERSION="4.5"
    VERSION="4.5.0"
    NEXT_VERSION="4.6.0"
    BRANCH_NAME="branch-${MAJOR_VERSION}"
    DEVELOPMENT_VERSION="${NEXT_VERSION}-SNAPSHOT"

Version represents the release currently underway, while next version specifies the anticipated next version to be released from that branch. Normally, 4.5.0 is followed by 4.6.0, while 4.5.0 is followed by 4.5.1.

Use Maven release plugin to create the release branch and update the current branch to use the new development version. This command applies for the new major or minor version.
(Warning: This command automatically check in and tag your code in the code repository configured in the SCM.) It is recommended to do a "dry run" before executing the command.
To "dry run", you can provide "-DdryRun" at the end of this command. "dry run" will generate some temporary files in the project folder, you can remove them by running "mvn release:clean".

    mvn release:branch \
        -DbranchName=${BRANCH_NAME} \
        -DdevelopmentVersion=${DEVELOPMENT_VERSION} \
        [-DdryRun]

However, if you are doing an incremental/hotfix release, please run the following command after checking out the release tag of the release being patched.

    mvn release:branch \
        -DbranchName=${BRANCH_NAME} \
        -DupdateWorkingCopyVersions=false \
        -DupdateBranchVersions=true \
        -DreleaseVersion="${VERSION}-SNAPSHOT" \
        [-DdryRun]

Check out the release branch.

    git checkout ${BRANCH_NAME}

The rest of this guide assumes that commands are run in the root of a repository on `${BRANCH_NAME}` with the above environment variables set.

### Checklist to proceed to the next step

1. Release Manager’s GPG key is published to `dist.apache.org`
2. Release Manager’s GPG key is configured in `git` configuration
3. Release Manager has `org.apache.bookkeeper` listed under `Staging Profiles` in Nexus
4. Release Manager’s Nexus User Token is configured in `settings.xml`
5. JIRA release item for the subsequent release has been created
6. Github milestone item for the subsequet release has been created
7. There are no release blocking JIRA issues
8. There are no release blocking Github issues
9. Release Notes in JIRA have been audited and adjusted
10. Release Notes for Github Milestone is generated, audited and adjusted
11. Release branch has been created
12. Originating branch has the version information updated to the new version

**********

## Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

### Build and stage Java artifacts with Maven


    TODO: Currently we have to build and stage maven artifacts manually, because it requires pushing the artifacts to apache staging. We should look for a solution to automate that.


Set up a few environment variables to simplify Maven commands that follow. This identifies the release candidate being built. Start with `release candidate number` equal to `0` and increment it for each candidate.

    RC_NUM="0"
    TAG="release-${VERSION}"
    RC_DIR="bookkeeper-${VERSION}-rc${RC_NUM}"

Use Maven release plugin to build the release artifacts, as follows:

    mvn release:prepare \
        -Dresume=false \
        -DreleaseVersion=${VERSION} \
        -Dtag=${TAG} \
        -DupdateWorkingCopyVersions=false \
        [-DdryRun] [-DskipTests]

Use Maven release plugin to stage these artifacts on the Apache Nexus repository, as follows:

    mvn release:perform [-DdryRun] [-DskipTests]

Review all staged artifacts. They should contain all relevant parts for each module, including `pom.xml`, jar, test jar, source, test source, javadoc, etc. Artifact names should follow [the existing format](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.bookkeeper%22) in which artifact name mirrors directory structure, e.g., `bookkeeper-server`. Carefully review any new artifacts.

Close the staging repository on Apache Nexus. When prompted for a description, enter “Apache BookKeeper, version X, release candidate Y”.

### Stage source release on dist.apache.org

Copy the source release to the dev repository of `dist.apache.org`.

1. If you have not already, check out the BookKeeper section of the `dev` repository on `dist.apache.org` via Subversion. In a fresh directory:

        svn co https://dist.apache.org/repos/dist/dev/bookkeeper

2. Make a directory for the new release:

        mkdir bookkeeper/${RC_DIR}

3. Copy and rename the BookKeeper source distribution, hashes, and GPG signature:

        cp target/bookkeeper-${VERSION}-src.tar.gz bookkeeper/${RC_DIR}/bookkeeper-${VERSION}-src.tar.gz
        cp target/bookkeeper-${VERSION}-src.tar.gz.asc bookkeeper/${RC_DIR}/bookkeeper-${VERSION}-src.tar.gz.asc
        cp target/bookkeeper-${VERSION}-src.tar.gz.md5 bookkeeper/${RC_DIR}/bookkeeper-${VERSION}-src.tar.gz.md5
        cp target/bookkeeper-${VERSION}-src.tar.gz.sha1 bookkeeper/${RC_DIR}/bookkeeper-${VERSION}-src.tar.gz.sha1
        cp bookkeeper-server/target/bookkeeper-server-${VERSION}-bin.tar.gz bookkeeper/${RC_DIR}/bookkeeper-server-${VERSION}-bin.tar.gz
        cp bookkeeper-server/target/bookkeeper-server-${VERSION}-bin.tar.gz.asc bookkeeper/${RC_DIR}/bookkeeper-server-${VERSION}-bin.tar.gz.asc
        cp bookkeeper-server/target/bookkeeper-server-${VERSION}-bin.tar.gz.md5 bookkeeper/${RC_DIR}/bookkeeper-server-${VERSION}-bin.tar.gz.md5
        cp bookkeeper-server/target/bookkeeper-server-${VERSION}-bin.tar.gz.sha1 bookkeeper/${RC_DIR}/bookkeeper-server-${VERSION}-bin.tar.gz.sha1

4. Add and commit all the files.

        cd bookkeeper
        svn add ${RC_DIR}
        svn commit

5. Verify that files are [present](https://dist.apache.org/repos/dist/dev/bookkeeper).

### Build Javadoc and Website

(TBD for new website)

### Checklist to proceed to the next step

1. Maven artifacts deployed to the staging repository of [repository.apache.org](https://repository.apache.org/content/repositories/)
1. Source and Binary distribution deployed to the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/incubator/bookkeeper/)
1. (TODO: javadoc and website)

**********

## Vote on the release candidate

Once you have built and individually reviewed the release candidate, please share it for the community-wide review. Please review foundation-wide [voting guidelines](http://www.apache.org/foundation/voting.html) for more information.

Start the review-and-vote thread on the dev@ mailing list. Here’s an email template; please adjust as you see fit.

    From: Release Manager
    To: dev@bookkeeper.apache.org
    Subject: [VOTE] Release 4.5.0, release candidate #0

    Hi everyone,
    Please review and vote on the release candidate #0 for the version 0.4.0, as follows:
    [ ] +1, Approve the release
    [ ] -1, Do not approve the release (please provide specific comments)

    The complete staging area is available for your review, which includes:
    * JIRA release notes [1],
    * the official Apache source release to be deployed to dist.apache.org [2],
    * all artifacts to be deployed to the Maven Central Repository [3],
    * source code tag "release-4.5.0" [4],
    * website pull request listing the release and publishing the API reference manual [5].

    The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

    Thanks,
    Release Manager

    [1] link
    [2] link
    [3] link
    [4] link
    [5] link

If there are any issues found in the release candidate, reply on the vote thread to cancel the vote. There’s no need to wait 72 hours. Proceed to the `Fix Issues` step below and address the problem. However, some issues don’t require cancellation. For example, if an issue is found in the website pull request, just correct it on the spot and the vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a separate email. Here’s an email template; please adjust as you see fit.

    From: Release Manager
    To: dev@bookkeeper.incubator.apache.org
    Subject: [RESULT] [VOTE] Release 0.4.0, release candidate #0

    I'm happy to announce that we have unanimously approved this release.

    There are XXX approving votes, XXX of which are binding:
    * approver 1
    * approver 2
    * approver 3
    * approver 4

    There are no disapproving votes.

    Thanks everyone!


### Checklist to proceed to the finalization step

1. Community votes to release the proposed candidate

**********

## Fix any issues

Any issues identified during the community review and vote should be fixed in this step.

Code changes should be proposed as standard pull requests to the `master` branch and reviewed using the normal contributing process. Then, relevant changes should be cherry-picked into the release branch. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.

### Checklist to proceed to the next step

1. Issues identified during vote have been resolved, with fixes committed to the release branch.

**********

## Finalize the release

Once the release candidate has been reviewed and approved by the community, the release should be finalized. This involves the final deployment of the release candidate to the release repositories, merging of the website changes, etc.

### Deploy artifacts to Maven Central Repository

Use the Apache Nexus repository to release the staged binary artifacts to the Maven Central repository. In the `Staging Repositories` section, find the relevant release candidate `orgapachebookkeeper-XXX` entry and click `Release`. Drop all other release candidates that are not being released.

#### Deploy source release to dist.apache.org

Copy the source release from the `dev` repository to the `release` repository at `dist.apache.org` using Subversion.

    svn move https://dist.apache.org/repos/dist/dev/bookkeeper/bookkeeper-${VERSION}-rc${RC_NUM} https://dist.apache.org/repos/dist/release/bookkeeper/bookkeeper-${VERSION}

### Update Website

[TBD]

### Mark the version as released in JIRA and Github

In JIRA, inside [version management](https://issues.apache.org/jira/plugins/servlet/project-config/BOOKKEEPER/versions), hover over the current release and a settings menu will appear. Click `Release`, and select today’s date.

In Github, inside [milestones](https://github.com/apache/bookkeeper/milestones), hover over the current milestone and click `close` button to close a milestone and set today's date as due-date.

### Checklist to proceed to the next step

* Maven artifacts released and indexed in the [Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.bookkeeper%22)
* Source and Binary distribution available in the release repository of [dist.apache.org](https://dist.apache.org/repos/dist/release/incubator/bookkeeper/)
* Website is updated with new release
* Release tagged in the source code repository
* Release version finalized in JIRA and Github

**********

## Promote the release

Once the release has been finalized, the last step of the process is to promote the release within the project and beyond.

### Apache mailing lists

Announce on the dev@ mailing list that the release has been finished.

Announce on the release on the user@ mailing list, listing major improvements and contributions.

(NOTE: Make sure sending the announce email using apache email, otherwise announce@apache.org will reject your email.)


    From: xxx@apache.org
    To: dev@bookkeeper.apache.org, dev@bookkeeper.apache.org, announce@apache.org
    Subject: [ANNOUNCE] Apache BookKeeper x.y.z released
     
    The Apache BookKeeper team is proud to announce Apache BookKeeper version
    x.y.z.
     
    This is the N release of the Apache BookKeeper.

    [highlights the release]
     
    For BookKeeper release details and downloads, visit:
     
    [download link]
     
    BookKeeper x.y.z Release Notes are at:

    [release notes link]
     
    We would like to thank the contributors that made the release possible.
     
    Regards,
     
    The BookKeeper Team


### ASF press release

After incubation, use reporter.apache.org to promote the release.

### Social media

Tweet, post on Facebook, LinkedIn, and other platforms. Ask other contributors to do the same.

### Checklist to declare the process completed

1. Release announced on the user@ mailing list.
1. Blog post published, if applicable.
1. Apache Software Foundation press release published.
1. Release announced on social media.
1. Completion declared on the dev@ mailing list.

**********

## Improve the process

It is important that we improve the release processes over time. Once you’ve finished the release, please take a step back and look what areas of this process and be improved. Perhaps some part of the process can be simplified. Perhaps parts of this guide can be clarified.

If we have specific ideas, please start a discussion on the dev@ mailing list and/or propose a pull request to update this guide. Thanks!
