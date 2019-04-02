---
title: Apache BookKeeper Release Guide
layout: community
---

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

To prepare for each release, you should audit the project status in Github issue tracker, and do necessary bookkeeping. Finally, you should create a release branch from which individual release candidates will be built.

### One-time setup instructions

#### GPG Key

You need to have a GPG key to sign the release artifacts. Please be aware of the ASF-wide [release signing guidelines](https://www.apache.org/dev/release-signing.html).
If you don’t have a GPG key associated with your Apache account, please create one according to the [guidelines](http://apache.org/dev/openpgp.html#generate-key) and [upload](https://www.apache.org/dev/release-signing.html#keyserver-upload) your key to a public key server.

> It is important to [link](https://www.apache.org/dev/release-signing.html#apache-wot) your GPG key into the Apache web of trust.
> You can reach out other committers in Apache BookKeeper community for signing your key.

Once you have a GPG key associated with your Apache count, then:

**First**, Determine your Apache GPG Key and Key ID, as follows:

    gpg --list-keys

This will list your GPG keys. One of these should reflect your Apache account, for example:

    --------------------------------------------------
    pub   2048R/845E6689 2016-02-23
    uid                  Nomen Nescio <anonymous@apache.org>
    sub   2048R/BA4D50BE 2016-02-23

Here, the key ID is the 8-digit hex string in the `pub` line: `845E6689`.

**Second**, add your Apache GPG key to the BookKeeper’s `KEYS` file in [`dist`](https://dist.apache.org/repos/dist/release/bookkeeper/KEYS).

```shell

# checkout the svn folder that contains the KEYS file
svn co https://dist.apache.org/repos/dist/release/bookkeeper bookkeeper_dist
cd bookkeeper_dist

# Export the key in ascii format and append it to the file
( gpg --list-sigs $USER@apache.org
  gpg --export --armor $USER@apache.org ) >> KEYS

# Commit to svn
svn ci -m "Added gpg key for $USER"

```

Once you committed, please verify if your GPG key shows up in the BookkKeeper's `KEYS` file in [`dist`](https://dist.apache.org/repos/dist/release/bookkeeper/KEYS).

**Third**, configure `git` to use this key when signing code by giving it your key ID, as follows:

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
5. Insert this snippet twice into your global Maven `settings.xml` file (use command `mvn -X | grep settings`, and read out the global Maven setting file), typically `${HOME}/.m2/settings.xml`. The end result should look like this, where `TOKEN_NAME` and `TOKEN_PASSWORD` are your secret tokens:

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

#### Create an account on PyPi

Since 4.9.0 we are releasing a python client for table service during release process. In order to publishing
a python package to PyPi, you need to [create an account](https://pypi.org/account/register/) there. After
you create the account successfully, you also need to add the account as a maintainer
for [bookkeeper-client](https://pypi.org/project/apache-bookkeeper-client/) project. You can checkout who
are the maintainers at the project page and ask them for adding your account as the maintainer.

You can also read the instructions on [how to upload packages to PyPi](https://twine.readthedocs.io/en/latest/)
if you are interested in learning more details.

### Create a new version in Github

When contributors resolve an issue in GitHub, they are tagging it with a release that will contain their changes. With the release currently underway, new issues should be resolved against a subsequent future release. Therefore, you should create a release item for this subsequent release, as follows:

1. In Github, navigate to the [`Issues > Milestones`](https://github.com/apache/bookkeeper/milestones).
2. Add a new milestone: choose the next minor version number compared to the one currently underway, select a day that is 3-months from now as the `Due Date`, write a description `Release x.y.z` and choose `Create milestone`.

Skip this step in case of a minor release, as milestones are only for major releases.

### Triage release-blocking issues in Github

There could be outstanding release-blocking issues, which should be triaged before proceeding to build a release candidate. We track them by assigning a specific `Milestone` field even before the issue resolved.

The list of release-blocking issues is available at the [milestones page](https://github.com/apache/bookkeeper/milestones). Triage each unresolved issue with one of the following resolutions:

* If the issue has been resolved and was not updated, close it accordingly.
* If the issue has not been resolved and it is acceptable to defer this until the next release, update the `Milestone` field to the new milestone you just created. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.
* If the issue has not been resolved and it is not acceptable to release until it is fixed, the release cannot proceed. Instead, work with the BookKeeper community to resolve the issue.

### Change Python Client Version

Before cutting a release, you need to update the python client version in
[setup.py](https://github.com/apache/bookkeeper/blob/master/stream/clients/python/setup.py#L22)
from `SNAPSHOT` version to a release version and get the change merge to master. For example,
in release 4.10.0, you need to change the version from `4.10.0-alpha-0` to `4.10.0`.

### Review Release Notes in Github

> Github does not automatically generates Release Notes based on the `Milestone` field applied to issues.
> We can use [github-changelog-generator](https://github.com/skywinder/github-changelog-generator) to generate a ChangeLog for a milestone in future.

For Github, we can use the milestone link in the Release Notes. E.g. [Release 4.5.0 milestone](https://github.com/apache/bookkeeper/milestone/1?closed=1).

#### Prepare Release Notes

After review the release notes on both Github, you should write a `releaseNotes` under `site/docs/${release_version}/overview/releaseNotes.md` and then send out a pull request for review.

[4.5.0 Release Notes](https://github.com/apache/bookkeeper/pull/402) is a good example to follow.

### Prepare release branch

Release candidates are built from a release branch. As a final step in preparation for the release, you should create the release branch, push it to the code repository, and update version information on the original branch.

Check out the version of the codebase from which you start the release. For a new minor or major release, this may be `HEAD` of the `master` branch. To build a hotfix/incremental release, instead of the `master` branch, use the release tag of the release being patched. (Please make sure your cloned repository is up-to-date before starting.)

    git checkout <master branch OR release tag>


Set up a few environment variables to simplify Maven commands that follow. (We use `bash` Unix syntax in this guide.)

For a major release (for instance 4.5.0):

    MAJOR_VERSION="4.5"
    VERSION="4.5.0"
    NEXT_VERSION="4.6.0"
    BRANCH_NAME="branch-${MAJOR_VERSION}"
    DEVELOPMENT_VERSION="${NEXT_VERSION}-SNAPSHOT"

For a minor release (for instance 4.5.1):

    MAJOR_VERSION="4.5"
    VERSION="4.5.1"
    NEXT_VERSION="4.5.2"
    BRANCH_NAME="branch-${MAJOR_VERSION}"
    DEVELOPMENT_VERSION="${NEXT_VERSION}-SNAPSHOT"

Version represents the release currently underway, while next version specifies the anticipated next version to be released from that branch. Normally, 4.5.0 is followed by 4.6.0, while 4.5.0 is followed by 4.5.1.

#### Create branch for major release

If you are cutting a minor release, you can skip this step and go to section [Checkout release branch](#checkout-release-branch).

If you are cutting a major release use Maven release plugin to create the release branch and update the current branch to use the new development version. This command applies for the new major or minor version.

> This command automatically check in and tag your code in the code repository configured in the SCM.
> It is recommended to do a "dry run" before executing the command. To "dry run", you can provide "-DdryRun"
> at the end of this command. "dry run" will generate some temporary files in the project folder, you can remove
> them by running "mvn release:clean".

    mvn release:branch \
        -DbranchName=${BRANCH_NAME} \
        -DdevelopmentVersion=${DEVELOPMENT_VERSION} \
        [-DdryRun]

> If you failed at the middle of running this command, please check if you have `push` permissions on `github.com`.
> You need use [personal access token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) rather than your own password, if you enabled `2 factor authentication`.
>
> On failures, you need to reset on failures:
>
> $ git reset --hard apache/<master branch OR release tag>
> $ git branch -D ${BRANCH_NAME}

##### Create CI jobs for release branch

Once the release branch is created, please create corresponding CI jobs for the release branch. These CI jobs includes postcommit jobs for different java versions and
integration tests.

Example PR: [release-4.7.0](https://github.com/apache/bookkeeper/pull/1328) [integration tests for release-4.7.0](https://github.com/apache/bookkeeper/pull/1353)

#### Checkout release branch
<a name="checkout-release-branch"></a>

Check out the release branch.

    git checkout ${BRANCH_NAME}

The rest of this guide assumes that commands are run in the root of a repository on `${BRANCH_NAME}` with the above environment variables set.

Verify that pom.xml contains the correct VERSION, it should still end with the '-SNAPSHOT' suffix.

### Checklist to proceed to the next step

1. Release Manager’s GPG key is published to `dist.apache.org`
2. Release Manager’s GPG key is configured in `git` configuration
3. Release Manager has `org.apache.bookkeeper` listed under `Staging Profiles` in Nexus
4. Release Manager’s Nexus User Token is configured in `settings.xml`
5. Github milestone item for the subsequet release has been created
6. There are no release blocking Github issues
7. Release Notes for Github Milestone is generated, audited and adjusted
8. Release branch has been created
9. Originating branch has the version information updated to the new version

**********

## Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

> Since 4.7.0, bookkeeper is releasing a CRC32C module `circe-checksum`. so all the steps on building a release candidate should happen in linux environment.
> It ensures the release candidate built with right jni library for `circe-checksum`.

Set up a few environment variables to simplify Maven commands that follow. This identifies the release candidate being built. Start with `release candidate number` equal to `0` and increment it for each candidate.

    RC_NUM="0"
    TAG="release-${VERSION}"
    RC_DIR="bookkeeper-${VERSION}-rc${RC_NUM}"
    RC_TAG="v${VERSION}-rc${RC_NUM}"

> Please make sure `gpg` command is in your $PATH. The maven release plugin use `gpg` to sign generated jars and packages.

### Run linux docker container to build release candidate

```shell
./dev/release/000-run-docker.sh ${RC_NUM}
```

After the docker process is lauched, use `cache` credential helper to cache github credentials during releasing process.

```shell
$ git config --global credential.helper "cache --timeout=3600"
```

Then run a dry-run github push to apache github repo. You will be asked for typing your github password, so the password will be cached for the whole releasing process.
If your account is configured with 2FA, use your personal token as the github password.

The remote `apache` should point to `https://github.com/apache/bookkeeper`.

```shell
$ git push apache --dry-run
```

### Build and stage Java artifacts with Maven


Use Maven release plugin to build the release artifacts, as follows:

```shell
./dev/release/002-release-prepare.sh
```

Use Maven release plugin to stage these artifacts on the Apache Nexus repository, as follows:

```shell
./dev/release/003-release-perform.sh
```

> If `release:perform` failed, 
> delete the release tag: git tag -d release-${VERSION} && git push apache :refs/tags/release-${VERSION}
> 
> Also, you need to check the git commits on the github and if needed you may have to
> force push backed out local git branch to github again.
>
> After reset, run `./dev/release/002-release-prepare.sh` again.

Review all staged artifacts. They should contain all relevant parts for each module, including `pom.xml`, jar, test jar, source, test source, javadoc, etc. Artifact names should follow [the existing format](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.bookkeeper%22) in which artifact name mirrors directory structure, e.g., `bookkeeper-server`. Carefully review any new artifacts.

Close the staging repository on Apache Nexus. When prompted for a description, enter “Apache BookKeeper, version X, release candidate Y”.

### Stage source release on dist.apache.org

1. Copy the source release to the dev repository of `dist.apache.org`.

```shell
./dev/release/004-stage-packages.sh
```

2. Verify that files are [present](https://dist.apache.org/repos/dist/dev/bookkeeper).

### Checklist to proceed to the next step

1. Maven artifacts deployed to the staging repository of [repository.apache.org](https://repository.apache.org/content/repositories/)
1. Source and Binary distribution deployed to the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/bookkeeper/)

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
    * Release notes [1]
    * The official Apache source and binary distributions to be deployed to dist.apache.org [2]
    * All artifacts to be deployed to the Maven Central Repository [3]
    * Source code tag "release-4.5.0" [4] with git sha XXXXXXXXXXXXXXXXXXXX

    BookKeeper's KEYS file contains PGP keys we used to sign this release:
    https://dist.apache.org/repos/dist/release/bookkeeper/KEYS

    Please download these packages and review this release candidate:

    - Review release notes
    - Download the source package (verify shasum, and asc) and follow the
    instructions to build and run the bookkeeper service.
    - Download the binary package (verify shasum, and asc) and follow the
    instructions to run the bookkeeper service.
    - Review maven repo, release tag, licenses, and any other things you think
    it is important to a release.

    The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PMC affirmative votes.

    Thanks,
    Release Manager

    [1] link
    [2] link
    [3] link
    [4] link
    [5] link

If there are any issues found in the release candidate, reply on the vote thread to cancel the vote. There’s no need to wait 72 hours. Proceed to the `Fix Issues` step below and address the problem. However, some issues don’t require cancellation. For example, if an issue is found in the website pull request, just correct it on the spot and the vote can continue as-is.

If there are no issues, reply on the vote thread to close the voting. Then, tally the votes in a separate email. Here’s an email template; please adjust as you see fit. (NOTE: the approver list are binding approvers.)

    From: Release Manager
    To: dev@bookkeeper.apache.org
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

### Deploy source release to dist.apache.org

Copy the source release from the `dev` repository to the `release` repository at `dist.apache.org` using Subversion.

    svn move https://dist.apache.org/repos/dist/dev/bookkeeper/bookkeeper-${VERSION}-rc${RC_NUM} https://dist.apache.org/repos/dist/release/bookkeeper/bookkeeper-${VERSION}

### Update Website

1. Create the documentation for `${VERSION}`. Run the `release.sh` to generate the branch for `${VERSION}` and bump
    the versions for website documentation; or run the `release_minor.sh` to release documentation when doing a
    mintor release.

    ```shell
    $ cd site

    // use `release.sh` for major releases
    $ ./scripts/release.sh

    // or `release_minor.sh` for minor releases
    $ ./scripts/release_minor.sh
    ```

    Once run the `release.sh`, please send a pull request for it and get approval from any committers, then merge it.
    The CI job will automatically update the website in a few minutes. Please review the website to make sure the
    documentation for `${VERSION}` is live.

2. Merge the Release Notes pull request and make sure the Release Notes is updated.

### Git tag

> NOTE: Only create the release tag after the release package is showed up under https://archive.apache.org/dist/bookkeeper/ as creating the tag triggers a docker autobuild which needs the package to exist. If you forget to do so, the build will fail. In this case you can delete the tag from github and push it again.

Create and push a new signed for the released version by copying the tag for the final release tag, as follows

```shell
git tag -s "${TAG}" "${RC_TAG}"
git push apache "${TAG}"
```

Remove rc tags:

```shell
for num in $(seq 0 ${RC_NUM}); do
    git tag -d "v${VERSION}-rc${num}"
    git push apache :"v${VERSION}-rc${num}"
done
```

### Update DC/OS BookKeeper package

> NOTE: Please update DC/OS bookkeeper package only after the release package is showed up under https://archive.apache.org/dist/bookkeeper/

Once we have new version of BookKeeper docker image available at [docker hub](https://hub.docker.com/r/apache/bookkeeper/), We could update DC/OS BookKeeper package in [mesosphere universe](https://github.com/mesosphere/universe). A new pull request is needed in it. 

It is easy if only version need be bump.

1. Clone repo [mesosphere universe](https://github.com/mesosphere/universe).

    ```shell
    $ git clone https://github.com/mesosphere/universe
    ```

2. cd into the repo, Checkout a branch for the changes.

    ```shell
    $ cd universe
    $ git checkout -b bookkeeper_new_version
    ```

3. Make a copy of latest code of BookKeeper package.

    ```shell
    $ cp -rf repo/packages/B/bookkeeper/1 repo/packages/B/bookkeeper/2
    $ git add repo/packages/B/bookkeeper/2
    $ git commit -m "copy old version"
    ```

4. Bump the version of BookKeeper docker image in file [resource.json](https://github.com/mesosphere/universe/blob/version-3.x/repo/packages/B/bookkeeper/1/resource.json#L5) and [package.json](https://github.com/mesosphere/universe/blob/version-3.x/repo/packages/B/bookkeeper/1/package.json#L4).

    ```
    diff --git repo/packages/B/bookkeeper/2/package.json repo/packages/B/bookkeeper/2/package.json
    index 07199d56..75f4aa81 100644
    --- repo/packages/B/bookkeeper/2/package.json
    +++ repo/packages/B/bookkeeper/2/package.json
    @@ -1,7 +1,7 @@
     {
       "packagingVersion": "3.0",
       "name": "bookkeeper",
    -  "version": "4.5.1",
    +  "version": "4.7.0",
       "scm": "https://github.com/apache/bookkeeper",
       "maintainer": "zhaijia@apache.org",
       "description": "BookKeeper is A scalable, fault-tolerant, and low-latency storage service optimized for real-time workloads.Further information can be found here: http://bookkeeper.apache.org/",
    diff --git repo/packages/B/bookkeeper/2/resource.json repo/packages/B/bookkeeper/2/resource.json
    index 3801750e..72690ea0 100644
    --- repo/packages/B/bookkeeper/2/resource.json
    +++ repo/packages/B/bookkeeper/2/resource.json
    @@ -2,7 +2,7 @@
       "assets": {
         "container": {
           "docker": {
    -        "bookkeeper": "apache/bookkeeper:4.5.1"
    +        "bookkeeper": "apache/bookkeeper:4.7.0"
           }
         }
       },
    ```

5. Commit the change, create a pull request and wait for it to be approved and merged.

    ```shell
    $ git add repo/packages/B/bookkeeper/2
    $ git commit -m "new bookkeeper version"
    ```

### Verify Docker Image

> After release tag is created, it will automatically trigger docker auto build. 

1. Verify the [docker hub](https://hub.docker.com/r/apache/bookkeeper/) to see if a new build for the given tag is build.

2. Once the new docker image is built, update BC tests to include new docker image. Example: [release-4.7.0](https://github.com/apache/bookkeeper/pull/1352)

### Release Python Client

Make sure you have installed [`pip`](https://pypi.org/project/pip/) and
[`twine`](https://twine.readthedocs.io/en/latest/).

- Install Pip
  ```bash
  brew install pip
  ```

- Install Twine
  ```bash
  pip install twine
  ```

After install `twine`, make sure `twine` exist in your PATH before releasing python client.

```bash
twine --version
```

Now, you are ready to publish the python client.

```bash
cd stream/clients/python
./publish.sh
```

Check the PyPi project package to make sure the python client is uploaded to  https://pypi.org/project/apache-bookkeeper-client/ .

### Advance version on release branch

> only do this for minor release

Use the Maven Release plugin in order to advance the version in all poms.

> This command will upgrade the <version> tag on every pom.xml locally to your workspace.

    mvn release:update-versions
        -DdevelopmentVersion=${DEVELOPMENT_VERSION}
        -Dstream

For instance if you have released 4.5.1, you have to change version to 4.5.2-SNAPSHOT.
Then you have to create a PR and submit it for review.

Example PR: [release-4.7.0](https://github.com/apache/bookkeeper/pull/1350)

### Advance python client version

If you are doing a major release, you need to update the python client version to next major development version in master
and next minor development version in the branch. For example, if you are doing 4.9.0 release, you need to bump the version
in master to `4.10.0-alpha-0` (NOTE: we are using `alpha-0` as `SNAPSHOT`, otherwise pypi doesn't work), and the version in
`branch-4.9` to `4.9.1-alpha-0`.

If you are only doing a minor release, you just need to update the version in release branch. For example, if you are doing
4.9.1 release, you need to bump the version in `branch-4.9` to `4.9.2-alpha-0`.

### Mark the version as released in Github

> only do this for feature release

In Github, inside [milestones](https://github.com/apache/bookkeeper/milestones), hover over the current milestone and click `close` button to close a milestone and set today's date as due-date.

### Update Release Schedule

> only do this for feature release

Update the [release schedule](../releases) page:

- Bump the next feature release version and update its release window.
- Update the release schedule to remove released version and add a new release.

### Checklist to proceed to the next step

* Maven artifacts released and indexed in the [Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.bookkeeper%22)
* Source and Binary distribution available in the release repository of [dist.apache.org](https://dist.apache.org/repos/dist/release/bookkeeper/)
* Website is updated with new release
* Docker image is built with new release
* Release tagged in the source code repository
* Release version finalized in Github
* Release section with release summary is added in [releases.md](https://github.com/apache/bookkeeper/blob/master/site/releases.md)
* Release schedule page is updated

**********

## Promote the release

Once the release has been finalized, the last step of the process is to promote the release within the project and beyond.

### Apache mailing lists

- Announce on the dev@ mailing list that the release has been finished.
- Announce on the release on the user@ mailing list, listing major improvements and contributions.
- Announce the release on the announce@apache.org mailing list

Use the template below for all the messages.

> NOTE: Make sure sending the announce email using apache email, otherwise announce@apache.org will reject your email.


    From: xxx@apache.org
    To: dev@bookkeeper.apache.org, user@bookkeeper.apache.org, announce@apache.org
    Subject: [ANNOUNCE] Apache BookKeeper x.y.z released
     
    The Apache BookKeeper team is proud to announce Apache BookKeeper version
    x.y.z.

    Apache BookKeeper is a scalable, fault-tolerant, and low-latency storage service optimized for
    real-time workloads. It has been used for a fundamental service to build reliable services.
    It is also the log segment store for Apache DistributedLog and the message store for Apache Pulsar.

    This is the N release of the Apache BookKeeper.

    [highlights the release and why users need to try the release]
     
    For BookKeeper release details and downloads, visit:
     
    [download link]
     
    BookKeeper x.y.z Release Notes are at:

    [release notes link]
     
    We would like to thank the contributors that made the release possible.
     
    Regards,
     
    The BookKeeper Team


### Recordkeeping

Use reporter.apache.org to seed the information about the release into future project reports.

This step can be done only by PMC.

### Social media

Tweet, post on Facebook, LinkedIn, and other platforms. Ask other contributors to do the same.

This step can be done only by PMC.

### Cleanup old releases

According to [ASF policy](http://www.apache.org/legal/release-policy.html#when-to-archive), `/www.apache.org/dist` should contain the latest release in each branch that
is currently under development. We need to remove the old releases from `release` repository.

For example, if 4.6.1 is a newer release, we need to remove releases older than 4.6.1.


    ```shell
    $ svn del https://dist.apache.org/repos/dist/release/bookkeeper/bookkeeper-${old-release} -m "remove bookkeeper release <old-release>"
    ```

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
