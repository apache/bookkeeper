---
title: Apache BookKeeper&trade; Releases
layout: community
---

{% capture mirror_url %}https://www.apache.org/dyn/closer.lua/bookkeeper{% endcapture %}
{% capture latest_source_url %}{{ mirror_url }}/bookkeeper-{{ site.latest_release }}/bookkeeper-{{ site.latest_release }}-src.tar.gz{% endcapture %}
{% capture latest_bin_url %}{{ mirror_url }}/bookkeeper-{{ site.latest_release }}/bookkeeper-server-{{ site.latest_release }}-bin.tar.gz{% endcapture %}
{% capture dist_url %}https://www.apache.org/dist/bookkeeper{% endcapture %}
{% capture latest_source_dist_url %}{{ dist_url }}/bookkeeper-{{ site.latest_release }}/bookkeeper-{{ site.latest_release }}-src.tar.gz{% endcapture %}
{% capture latest_bin_dist_url %}{{ dist_url }}/bookkeeper-{{ site.latest_release }}/bookkeeper-server-{{ site.latest_release }}-bin.tar.gz{% endcapture %}

{% capture archive_url %}https://archive.apache.org/dist/bookkeeper{% endcapture %}
{% capture stable_source_url %}{{ archive_url }}/bookkeeper-{{ site.stable_release }}/bookkeeper-{{ site.stable_release }}-src.tar.gz{% endcapture %}
{% capture stable_bin_url %}{{ archive_url }}/bookkeeper-{{ site.stable_release }}/bookkeeper-server-{{ site.stable_release }}-bin.tar.gz{% endcapture %}

Version **{{ site.latest_release }}** is the [latest release](#latest-release) of BookKeeper. The current [stable version](#latest-stable-release) is **{{ site.stable_release }}**.

> You can verify your download by following these [procedures](http://www.apache.org/info/verification.html) and using these [KEYS](https://www.apache.org/dist/bookkeeper/KEYS).

If you want to download older, archived releases, they are available in the [Apache archive](http://archive.apache.org/dist/bookkeeper/).

## Latest release (version {{ site.latest_release }})
<a name="latest-release"></a>

Release | Link | Crypto files
:-------|:-----|:------------
Source | [bookkeeper-{{ site.latest_release }}-src.tar.gz]({{ latest_source_url }}) | [asc]({{ latest_source_dist_url }}.asc), [sha512]({{ latest_source_dist_url }}.sha512)
Binary | [bookkeeper-server-{{ site.latest_release }}-bin.tar.gz]({{ latest_bin_url }}) | [asc]({{ latest_bin_dist_url }}.asc), [sha512]({{ latest_bin_dist_url }}.sha512)

## Latest stable release (version {{ site.stable_release }})
<a name="latest-stable-release"></a>

Release | Link | Crypto files
:-------|:-----|:------------
Source | [bookkeeper-{{ site.stable_release }}-src.tar.gz]({{ stable_source_url }}) | [asc]({{ stable_source_url }}.asc), [sha1]({{ stable_source_url }}.sha1)
Binary | [bookkeeper-server-{{ site.stable_release }}-bin.tar.gz]({{ stable_bin_url }}) | [asc]({{ stable_bin_url }}.asc), [sha1]({{ stable_bin_url }}.sha1)

## Recent releases

{% for version in site.versions %}{% if version != site.latest_release %}
{% capture root_url %}https://archive.apache.org/dist/bookkeeper/bookkeeper-{{ version }}{% endcapture %}
{% capture src_root %}{{ root_url }}/bookkeeper-{{ version }}-src.tar.gz{% endcapture %}
{% capture bin_root %}{{ root_url }}/bookkeeper-server-{{ version }}-bin.tar.gz{% endcapture %}
### Version {{ version }}

Release | Link | Crypto files
:-------|:-----|:------------
Source | [bookkeeper-{{ version }}-src.tar.gz]({{ src_root }}) | [asc]({{ src_root }}.asc), [sha1]({{ src_root }}.sha1)
Binary | [bookkeeper-server-{{ version }}-bin.tar.gz]({{ bin_root }}) | [asc]({{ bin_root }}.asc), [sha1]({{ bin_root }}.sha1)
{% endif %}{% endfor %}

## Getting Started

Once you've downloaded a BookKeeper release, instructions on getting up and running with a standalone cluster that you can run your laptop can be found
in [Run BookKeeper locally]({{ site.baseurl }}docs/latest/getting-started/run-locally).

If you need to connect to an existing BookKeeper cluster using an officially supported client, see client docs for these languages:

Client Guide | API docs
:------------|:--------
[The BookKeeper Client]({{ site.baseurl }}docs/latest/api/ledger-api) | [Javadoc]({{ site.baseurl }}docs/latest/api/javadoc)
[The DistributedLog Library]({{ site.baseurl }}docs/latest/api/distributedlog-api) | [Javadoc](https://distributedlog.io/docs/latest/api/java)

## News

### 6 November, 2019 Release 4.10.0 available

This is the 20th release of Apache BookKeeper!

The 4.10.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.9.0.

See [BookKeeper 4.10.0 Release Notes](../docs/4.10.0/overview/releaseNotes) for details.

### 16 May, 2019 Release 4.9.2 available

This is the 19th release of Apache BookKeeper !

The 4.9.2 release is a bugfix release which fixes a couple of issues reported from users of 4.9.1.

See [BookKeeper 4.9.2 Release Notes](../docs/4.9.2/overview/releaseNotes) for details.

### 7 April, 2019 Release 4.9.1 available

This is the 18th release of Apache BookKeeper !

The 4.9.1 release is a bugfix release which fixes a couple of issues reported from users of 4.9.0.

See [BookKeeper 4.9.1 Release Notes](../docs/4.9.1/overview/releaseNotes) for details.

### 19 March, 2019 Release 4.8.2 available

This is the 17th release of Apache BookKeeper!

The 4.8.2 release is a bugfix release which fixes a couple of issues reported from users of 4.8.1.

See [BookKeeper 4.8.2 Release Notes](../docs/4.8.2/overview/releaseNotes) for details.

### 31 January, 2019 Release 4.9.0 available

This is the 16th release of Apache BookKeeper!

The 4.9.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.8.0,
which was released four months ago. It is a new milestone in Apache BookKeeper community.

See [BookKeeper 4.9.0 Release Notes](../docs/4.9.0/overview/releaseNotes) for details.

### 04 December, 2018 Release 4.7.3 available

This is the 15th release of Apache BookKeeper!

The 4.7.3 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.2.

See [BookKeeper 4.7.3 Release Notes](../docs/4.7.3/overview/releaseNotes) for details.

### 22 November, 2018 Release 4.8.1 available

This is the 14th release of Apache BookKeeper !

The 4.8.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.8.0.

See [BookKeeper 4.8.1 Release Notes](../docs/4.8.1/overview/releaseNotes) for details.

### 26 September, 2018 Release 4.8.0 available

This is the 13th release of Apache BookKeeper !

The 4.8.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.7.0.
It is a new big milestone in Apache BookKeeper community, this release include great new features, like Relaxed Durability, Stream Storage service and Multiple Active Entrylogs.

See [BookKeeper 4.8.0 Release Notes](../docs/4.8.0/overview/releaseNotes) for details.

### 29 August, 2018: Release 4.7.2 available

This is the 12th release of Apache BookKeeper!

The 4.7.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.1. These fixes include
bug fixes around DbLedgerStorage, failure handling around ensemble changes, bookie shutdown and such.

See [BookKeeper 4.7.2 Release Notes](../docs/4.7.2/overview/releaseNotes) for details.


### 19 June, 2018: Release 4.7.1 available

This is the 11th release of Apache BookKeeper!

The 4.7.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.7.0. These fixes include
bug fixes around ledger cache and object pooling, performance enhancement avoiding memory copies and such.

See [BookKeeper 4.7.1 Release Notes](../docs/4.7.1/overview/releaseNotes) for details.


### 17 April, 2018: Release 4.7.0 available

This is the 10th release of Apache BookKeeper!

The 4.7.0 release incorporates hundreds of bug fixes, improvements, and features since previous major release, 4.6.0,
which was released four months ago. It is a big milestone in Apache BookKeeper community - Yahoo branch is fully merged
back to upstream, and Apache Pulsar (incubating) starts using official BookKeeper release for its upcoming 2.0 release.

It is also the first release of Apache DistributedLog after it is merged as sub modules of Apache BookKeeper.

See [BookKeeper 4.7.0 Release Notes](../docs/4.7.0/overview/releaseNotes) for details.


### 9 April, 2018: Release 4.6.2 available

This is the ninth release of BookKeeper as an Apache Top Level Project!

The 4.6.2 release is a bugfix release.

See [BookKeeper 4.6.2 Release Notes](../docs/4.6.2/overview/releaseNotes) for details.


### 30 January, 2018: Release 4.6.1 available

This is the eighth release of BookKeeper as an Apache Top Level Project!

The 4.6.1 release is a bugfix release.

See [BookKeeper 4.6.1 Release Notes](../docs/4.6.1/overview/releaseNotes) for details.


### 27 December, 2017: Release 4.6.0 available

This is the seventh release of BookKeeper as an Apache Top Level Project!

The 4.6.0 release incorporates new fixes, improvements, and features since previous major release 4.5.0.

See [BookKeeper 4.6.0 Release Notes](../docs/4.6.0/overview/releaseNotes) for details.

### 22 November, 2017: Release 4.5.1 available

This is the sixth release of BookKeeper as an Apache Top Level Project!

The 4.5.1 release is a bugfix release.

See [BookKeeper 4.5.1 Release Notes](../docs/4.5.1/overview/releaseNotes) for details.

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
