<!-- markdown-link-check-disable -->

---
id: releases
title: Apache BookKeeper Releases
---

Version **{{ site.latest_release }}** is the latest release of BookKeeper. The current stable version is **{{ site.stable_release }}**.

[Release notes](/release-notes#)

import RecentReleases from "@site/src/components/RecentReleases"

<RecentReleases />

## Release Integrity
You must [verify](https://www.apache.org/info/verification.html) the integrity of the downloaded files. We provide OpenPGP signatures for every release file. This signature should be matched against the [KEYS]({{ site.dist_base_url }}/KEYS) file which contains the OpenPGP keys of BookKeeper's Release Managers. We also provide SHA-512 checksums for every release file. After you download the file, you should calculate a checksum for your download, and make sure it is the same as ours.

If you want to download older, archived releases, they are available in the [Apache archive](http://archive.apache.org/dist/bookkeeper/).


## Getting Started

Once you've downloaded a BookKeeper release, instructions on getting up and running with a standalone cluster that you can run your laptop can be found
in [Run BookKeeper locally](docs/getting-started/run-locally).

If you need to connect to an existing BookKeeper cluster using an officially supported client, see client docs for these languages:

Client Guide | API docs
:------------|:--------
[The BookKeeper Client](docs/api/ledger-api) | [Javadoc]({{ site.javadoc_base_url }})
[The DistributedLog Library](docs/api/distributedlog-api) | [Javadoc](https://distributedlog.io/docs/latest/api/java)

## News

### 9 May, 2022 Release 4.15.0 available

This is the 31st release of Apache BookKeeper !
See [BookKeeper 4.15.0 Release Notes](/release-notes#4150) for details.

### 22 December, 2021 Release 4.14.4 available

This is the 30th release of Apache BookKeeper !
See [BookKeeper 4.14.4 Release Notes](/release-notes#4144) for details.

### 9 November, 2021 Release 4.14.3 available

This is the 29th release of Apache BookKeeper !
See [BookKeeper 4.14.3 Release Notes](/release-notes#4143) for details.

### 23 August, 2021 Release 4.14.2 available

This is the 28th release of Apache BookKeeper !
See [BookKeeper 4.14.2 Release Notes](/release-notes#4142) for details.

### 31 May, 2021 Release 4.14.1 available

This is the 27th release of Apache BookKeeper !
See [BookKeeper 4.14.1 Release Notes](/release-notes#4141) for details.

### 25 May, 2021 Release 4.14.0 available

This is the 26th release of Apache BookKeeper !
See [BookKeeper 4.14.0 Release Notes](/release-notes#4140) for details.

### 25 February, 2021 Release 4.13.0 available

This is the 25th release of Apache BookKeeper !
See [BookKeeper 4.13.0 Release Notes](/release-notes#4130) for details.

### 11 January, 2021 Release 4.12.1 available

This is the 24th release of Apache BookKeeper !

See [BookKeeper 4.12.1 Release Notes](/release-notes#4121) for details.

### 11 November, 2020 Release 4.12.0 available

This is the 23th release of Apache BookKeeper !

See [BookKeeper 4.12.0 Release Notes](/release-notes#4120) for details.

### 20 October, 2020 Release 4.11.1 available

This is the 22th release of Apache BookKeeper !

See [BookKeeper 4.11.1 Release Notes](/release-notes#4111) for details.

### 10 July, 2020  Release 4.11.0 available

This is the 21th release of Apache BookKeeper !

See [BookKeeper 4.11.0 Release Notes](/release-notes#4110) for details.

### 6 November, 2019 Release 4.10.0 available

This is the 20th release of Apache BookKeeper!

The 4.10.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.9.0.

See [BookKeeper 4.10.0 Release Notes](/release-notes#4100) for details.

### 16 May, 2019 Release 4.9.2 available

This is the 19th release of Apache BookKeeper !

The 4.9.2 release is a bugfix release which fixes a couple of issues reported from users of 4.9.1.

See [BookKeeper 4.9.2 Release Notes](/release-notes#492) for details.

### 7 April, 2019 Release 4.9.1 available

This is the 18th release of Apache BookKeeper !

The 4.9.1 release is a bugfix release which fixes a couple of issues reported from users of 4.9.0.

See [BookKeeper 4.9.1 Release Notes](/release-notes#491) for details.

### 19 March, 2019 Release 4.8.2 available

This is the 17th release of Apache BookKeeper!

The 4.8.2 release is a bugfix release which fixes a couple of issues reported from users of 4.8.1.

See [BookKeeper 4.8.2 Release Notes](/release-notes#482) for details.

### 31 January, 2019 Release 4.9.0 available

This is the 16th release of Apache BookKeeper!

The 4.9.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.8.0,
which was released four months ago. It is a new milestone in Apache BookKeeper community.

See [BookKeeper 4.9.0 Release Notes](/release-notes#490) for details.

### 04 December, 2018 Release 4.7.3 available

This is the 15th release of Apache BookKeeper!

The 4.7.3 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.2.

See [BookKeeper 4.7.3 Release Notes](/release-notes#473) for details.

### 22 November, 2018 Release 4.8.1 available

This is the 14th release of Apache BookKeeper !

The 4.8.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.8.0.

See [BookKeeper 4.8.1 Release Notes](/release-notes#481) for details.

### 26 September, 2018 Release 4.8.0 available

This is the 13th release of Apache BookKeeper !

The 4.8.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.7.0.
It is a new big milestone in Apache BookKeeper community, this release include great new features, like Relaxed Durability, Stream Storage service and Multiple Active Entrylogs.

See [BookKeeper 4.8.0 Release Notes](/release-notes#480) for details.

### 29 August, 2018: Release 4.7.2 available

This is the 12th release of Apache BookKeeper!

The 4.7.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.1. These fixes include
bug fixes around DbLedgerStorage, failure handling around ensemble changes, bookie shutdown and such.

See [BookKeeper 4.7.2 Release Notes](/release-notes#472) for details.


### 19 June, 2018: Release 4.7.1 available

This is the 11th release of Apache BookKeeper!

The 4.7.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.0. These fixes include
bug fixes around ledger cache and object pooling, performance enhancement avoiding memory copies and such.

See [BookKeeper 4.7.1 Release Notes](/release-notes#471) for details.


### 17 April, 2018: Release 4.7.0 available

This is the 10th release of Apache BookKeeper!

The 4.7.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.6.0,
which was released four months ago. It is a big milestone in Apache BookKeeper community - Yahoo branch is fully merged
back to upstream, and Apache Pulsar (incubating) starts using official BookKeeper release for its upcoming 2.0 release.

It is also the first release of Apache DistributedLog after it is merged as sub modules of Apache BookKeeper.

See [BookKeeper 4.7.0 Release Notes](/release-notes#470) for details.


### 9 April, 2018: Release 4.6.2 available

This is the ninth release of BookKeeper as an Apache Top Level Project!

The 4.6.2 release is a bugfix release.

See [BookKeeper 4.6.2 Release Notes](/release-notes#462) for details.


### 30 January, 2018: Release 4.6.1 available

This is the eighth release of BookKeeper as an Apache Top Level Project!

The 4.6.1 release is a bugfix release.

See [BookKeeper 4.6.1 Release Notes](/release-notes#461) for details.


### 27 December, 2017: Release 4.6.0 available

This is the seventh release of BookKeeper as an Apache Top Level Project!

The 4.6.0 release incorporates new fixes, improvements, and features since previous major release 4.5.0.

See [BookKeeper 4.6.0 Release Notes](/release-notes#460) for details.

### 22 November, 2017: Release 4.5.1 available

This is the sixth release of BookKeeper as an Apache Top Level Project!

The 4.5.1 release is a bugfix release.

See [BookKeeper 4.5.1 Release Notes](/release-notes#451) for details.

### 10 August, 2017: Release 4.5.0 available

This is the fifth release of BookKeeper as an Apache Top Level Project!

The 4.5.0 release incorporates hundreds of new fixes, improvements, and features since previous major release, 4.4.0,
which was released over a year ago. It is a big milestone in Apache BookKeeper community, converging from three
main branches (Salesforce, Twitter and Yahoo).

See [BookKeeper 4.5.0 Release Notes](/release-notes#450) for details.

### 16 May, 2016: release 4.4.0 available

This is the fourth release of BookKeeper as an Apache Top Level Project!

This release contains a total of 94 Jira tickets fixed and brings several bookie
reliability and operability improvements, along with a long list of bugfixes.

See [BookKeeper 4.4.0 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.4.0/releaseNotes.html) for details.

### 30 Nov, 2015: release 4.3.2 available

This is the third release of BookKeeper as an Apache Top Level Project!

This release fixes some issues in both bookie server and bookkeeper client.

See [BookKeeper 4.3.2 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.3.2/releaseNotes.html) for details.

### 27 May, 2015: release 4.3.1 available

This is the second release of BookKeeper as an Apache Top Level Project!

This release fixes some issues in both bookie server and bookkeeper client.

See [BookKeeper 4.3.1 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.3.1/releaseNotes.html) for details.

### 16 Jan, 2015: release 4.2.4 available

This is the first release of BookKeeper as an Apache Top Level Project!

This release fixes some critical issues in fencing when the ack quorum and write quorum are different sizes.

See [BookKeeper 4.2.4 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.2.4/releaseNotes.html) for details.

### 14 Oct, 2014: release 4.3.0 available

This is the seventh release of BookKeeper as a subproject of Zookeeper.

This release includes a lot of improvements to the bookie on-disk performance, a new statistics framework, and protobuffer protocol support along with numerous bugfixes.

See [BookKeeper 4.3.0 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.3.0/releaseNotes.html) for details.

### 27 June, 2013: release 4.2.3 available

This is the sixth release of BookKeeper as a subproject of Zookeeper.

This is a bugfix release for 4.2.2.

Notable fixes and improvements include new utilities to give administrators better visibility of cluster state (BOOKKEEPER-746),
improvements to allow for smoother rolling upgrades (BOOKKEEPER-745),
fixes to ledger polling to ensure metadata updates aren't missed (BOOKKEEPER-710 & BOOKKEEPER-747) and shading of protobuf libraries
to avoid conflicts when included with other version (BOOKKEEPER-708).

See [BookKeeper 4.2.3 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.2.3/releaseNotes.html) for details.

### 9 Oct, 2013: release 4.2.2 available

This is the fifth release of BookKeeper as a subproject of Zookeeper.

This is a bugfix release for 4.2.1. There are some minor API improvements. Notably, it is now possible to check whether a ledger is closed without opening it, and it is now possible to get a list of ledgers available in the cluster.

See [BookKeeper 4.2.2 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.2.2/releaseNotes.html) for details.

### 27 Feb, 2013: release 4.2.1 available

This is the fourth release of BookKeeper as a subproject of Zookeeper.
*This release fixes a major performance bug in release 4.2.0. All users of BookKeeper 4.2.0 should upgrade immediately.*

See [BookKeeper 4.2.1 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.2.1/releaseNotes.html) for details.

### 18 Jan, 2013: release 4.2.0 available

This is the third release of BookKeeper as a subproject of Zookeeper.
See [BookKeeper 4.2.0 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.2.0/releaseNotes.html) for details.

### 12 Jun, 2012: release 4.1.0 available

This is the second release of BookKeeper as a subproject of Zookeeper.
See [BookKeeper 4.1.0 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.1.0/releaseNotes.html) for details.

### 7 Dec, 2011: release 4.0.0 available

This is the first release of BookKeeper as a subproject of Zookeeper.
See [BookKeeper 4.0.0 Release Notes]({{ site.archive_releases_base_url }}/docs/r4.0.0/releaseNotes.html) for details.
