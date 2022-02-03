---
title: BookKeeper installation
subtitle: Download or clone BookKeeper and build it locally
next: ../run-locally
---

{% capture download_url %}http://apache.claz.org/bookkeeper/bookkeeper-{{ site.latest_release }}/bookkeeper-{{ site.latest_release }}-src.tar.gz{% endcapture %}

You can install BookKeeper either by [downloading](#download) a [GZipped](http://www.gzip.org/) tarball package or [cloning](#clone) the BookKeeper repository.

## Requirements

* [Unix environment](http://www.opengroup.org/unix)
* [Java Development Kit 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later

## Download

You can download Apache BookKeeper releases from one of many [Apache mirrors](https://dlcdn.apache.org/bookkeeper/).

## Clone

To build BookKeeper from source, clone the repository, either from the [GitHub mirror]({{ site.github_repo }}):

```shell
$ git clone {{ site.github_repo}}
```

## Build using Gradle

Once you have the BookKeeper on your local machine, either by [downloading](#download) or [cloning](#clone) it, you can then build BookKeeper from source using Gradle:

```shell
$ ./gradlew build -x signDistTar -x test
```

To run all the tests:

```shell
$ ./gradlew test -x signDistTar
```

## Package directory

The BookKeeper project contains several subfolders that you should be aware of:

Subfolder | Contains
:---------|:--------
[`bookkeeper-server`]({{ site.github_repo }}/tree/master/bookkeeper-server) | The BookKeeper server and client
[`bookkeeper-benchmark`]({{ site.github_repo }}/tree/master/bookkeeper-benchmark) | A benchmarking suite for measuring BookKeeper performance
[`bookkeeper-stats`]({{ site.github_repo }}/tree/master/bookkeeper-stats) | A BookKeeper stats library
[`bookkeeper-stats-providers`]({{ site.github_repo }}/tree/master/bookkeeper-stats-providers) | BookKeeper stats providers
