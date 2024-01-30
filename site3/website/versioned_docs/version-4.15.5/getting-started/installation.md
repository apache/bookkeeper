---
id: installation
title: BookKeeper installation
---

You can install BookKeeper either by [downloading](#download) a [GZipped](http://www.gzip.org/) tarball package, using the [Docker image](https://hub.docker.com/r/apache/bookkeeper/tags) or [cloning](#clone) the BookKeeper repository.

## Requirements

* [Unix environment](https://www.opengroup.org/membership/forums/platform/unix)
* [Java Development Kit 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later

## Download

You can download Apache BookKeeper releases from the [Download page](/releases).

## Clone

To build BookKeeper from source, clone the repository from the [GitHub mirror]({{ site.github_repo }}):

```shell
$ git clone {{ site.github_repo }}
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
