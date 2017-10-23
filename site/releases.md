---
title: Apache BookKeeper&trade; Releases
layout: community
---

## Download

{{ site.latest_release }} is latest release. The current stable version is {{ site.stable_release }}.

Releases are available to download from Apache mirrors: [Download](http://www.apache.org/dyn/closer.cgi/bookkeeper)

You can verify your download by following these [procedures](http://www.apache.org/info/verification.html) and using these [KEYS](https://dist.apache.org/repos/dist/release/bookkeeper/KEYS).

If you want to download older releases, they are available in the [Apache archive](http://archive.apache.org/dist/bookkeeper/).

## Getting Started

Once you've downloaded a BookKeeper release, instructions on getting up and running with a standalone cluster that you can run your laptop can be found
in [Run BookKeeper locally]({{ site.baseurl }}docs/latest/getting-started/run-locally).

If you need to connect to an existing BookKeeper cluster using an officially supported client, see client docs for these languages:

Client Guide | API docs
:------------|:--------
[The BookKeeper Client]({{ site.baseurl }}docs/latest/api/ledger-api) | [Javadoc]({{ site.baseurl }}docs/latest/api/javadoc)
[The DistributedLog Library]({{ site.baseurl }}docs/latest/api/distributedlog-api) | [Javadoc](https://distributedlog.io/docs/latest/api/java)

## News

### 10 August, 2017: Release 4.5.0 available

This is the fifth release of BookKeeper as an Apache Top Level Project!

The 4.5.0 release incorporates hundreds of new fixes, improvements, and features since previous major release, 4.4.0,
which was released over a year ago. It is a big milestone in Apache BookKeeper community, converging from three
main branches (Salesforce, Twitter and Yahoo).

See [BookKeeper 4.5.0 Release Notes](../docs/4.5.0/overview/releaseNotes) for details.

### 16 May, 2016: release 4.4.0 available

This is the fourth release of BookKeeper as an Apache Top Level Project!

This release contains a total of 94 Jira tickets fixed and brings several bookie
reliability and operability improvements, along with a long list of bugfixes.

See [BookKeeper 4.4.0 Release Notes]({{ site.baseurl }}archives/docs/r4.4.0/releaseNotes.html) for details.

### 30 Nov, 2015: release 4.3.2 available

This is the third release of BookKeeper as an Apache Top Level Project!

This release fixes some issues in both bookie server and bookkeeper client.

See [BookKeeper 4.3.2 Release Notes]({{ site.baseurl }}archives/docs/r4.3.2/releaseNotes.html) for details.

### 27 May, 2015: release 4.3.1 available

This is the second release of BookKeeper as an Apache Top Level Project!

This release fixes some issues in both bookie server and bookkeeper client.

See [BookKeeper 4.3.1 Release Notes]({{ site.baseurl }}archives/docs/r4.3.1/releaseNotes.html) for details.

### 16 Jan, 2015: release 4.2.4 available

This is the first release of BookKeeper as an Apache Top Level Project!

This release fixes some critical issues in fencing when the ack quorum and write quorum are different sizes.

See [BookKeeper 4.2.4 Release Notes]({{ site.baseurl }}archives/docs/r4.2.4/releaseNotes.html) for details.

### 14 Oct, 2014: release 4.3.0 available

This is the seventh release of BookKeeper as a subproject of Zookeeper.

This release includes a lot of improvements to the bookie on-disk performance, a new statistics framework, and protobuffer protocol support along with numerous bugfixes.

See [BookKeeper 4.3.0 Release Notes]({{ site.baseurl }}archives/docs/r4.3.0/releaseNotes.html) for details.

### 27 June, 2013: release 4.2.3 available

This is the sixth release of BookKeeper as a subproject of Zookeeper.

This is a bugfix release for 4.2.2.

Notable fixes and improvements include new utilities to give administrators better visibility of cluster state (BOOKKEEPER-746),
improvements to allow for smoother rolling upgrades (BOOKKEEPER-745),
fixes to ledger polling to ensure metadata updates aren't missed (BOOKKEEPER-710 & BOOKKEEPER-747) and shading of protobuf libraries
to avoid conflicts when included with other version (BOOKKEEPER-708).

See [BookKeeper 4.2.3 Release Notes]({{ site.baseurl }}archives/docs/r4.2.3/releaseNotes.html) for details.

### 9 Oct, 2013: release 4.2.2 available

This is the fifth release of BookKeeper as a subproject of Zookeeper.

This is a bugfix release for 4.2.1. There are some minor API improvements. Notably, it is now possible to check whether a ledger is closed without opening it, and it is now possible to get a list of ledgers available in the cluster.

See [BookKeeper 4.2.2 Release Notes]({{ site.baseurl }}archives/docs/r4.2.2/releaseNotes.html) for details.

### 27 Feb, 2013: release 4.2.1 available

This is the fourth release of BookKeeper as a subproject of Zookeeper.
*This release fixes a major performance bug in release 4.2.0. All users of BookKeeper 4.2.0 should upgrade immediately.*

See [BookKeeper 4.2.1 Release Notes]({{ site.baseurl }}archives/docs/r4.2.1/releaseNotes.html) for details.

### 18 Jan, 2013: release 4.2.0 available

This is the third release of BookKeeper as a subproject of Zookeeper.
See [BookKeeper 4.2.0 Release Notes]({{ site.baseurl }}archives/docs/r4.2.0/releaseNotes.html) for details.

### 12 Jun, 2012: release 4.1.0 available

This is the second release of BookKeeper as a subproject of Zookeeper.
See [BookKeeper 4.1.0 Release Notes]({{ site.baseurl }}archives/docs/r4.1.0/releaseNotes.html) for details.

### 7 Dec, 2011: release 4.0.0 available

This is the first release of BookKeeper as a subproject of Zookeeper.
See [BookKeeper 4.0.0 Release Notes]({{ site.baseurl }}archives/docs/r4.0.0/releaseNotes.html) for details.

