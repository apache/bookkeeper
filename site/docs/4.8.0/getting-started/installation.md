---
title: BookKeeper installation
subtitle: Download or clone BookKeeper and build it locally
next: ../run-locally
---

{% capture download_url %}http://apache.claz.org/bookkeeper/bookkeeper-{{ site.latest_release }}/bookkeeper-{{ site.latest_release }}-src.tar.gz{% endcapture %}

You can install BookKeeper either by [downloading](#download) a [GZipped](http://www.gzip.org/) tarball package or [cloning](#clone) the BookKeeper repository.

## Requirements

* [Unix environment](http://www.opengroup.org/unix)
* [Java Development Kit 1.6](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later
* [Maven 3.0](https://maven.apache.org/install.html) or later

## Download

You can download Apache BookKeeper releases from one of many [Apache mirrors](http://www.apache.org/dyn/closer.cgi/bookkeeper). Here's an example for the [apache.claz.org](http://apache.claz.org/bookkeeper) mirror:

```shell
$ curl -O {{ download_url }}
$ tar xvf bookkeeper-{{ site.latest_release }}-src.tar.gz
$ cd bookkeeper-{{ site.latest_release }}
```

## Clone

To build BookKeeper from source, clone the repository, either from the [GitHub mirror]({{ site.github_repo }}) or from the [Apache repository](http://git.apache.org/bookkeeper.git/):

```shell
# From the GitHub mirror
$ git clone {{ site.github_repo}}

# From Apache directly
$ git clone git://git.apache.org/bookkeeper.git/
```

## Build using Maven

Once you have the BookKeeper on your local machine, either by [downloading](#download) or [cloning](#clone) it, you can then build BookKeeper from source using Maven:

```shell
$ mvn package
```

> You can skip tests by adding the `-DskipTests` flag when running `mvn package`.

### Useful Maven commands

Some other useful Maven commands beyond `mvn package`:

Command | Action
:-------|:------
`mvn clean` | Removes build artifacts
`mvn compile` | Compiles JAR files from Java sources
`mvn compile spotbugs:spotbugs` | Compile using the Maven [SpotBugs](https://github.com/spotbugs/spotbugs-maven-plugin) plugin
`mvn install` | Install the BookKeeper JAR locally in your local Maven cache (usually in the `~/.m2` directory)
`mvn deploy` | Deploy the BookKeeper JAR to the Maven repo (if you have the proper credentials)
`mvn verify` | Performs a wide variety of verification and validation tasks
`mvn apache-rat:check` | Run Maven using the [Apache Rat](http://creadur.apache.org/rat/apache-rat-plugin/) plugin
`mvn compile javadoc:aggregate` | Build Javadocs locally
`mvn package assembly:single` | Build a complete distribution using the Maven [Assembly](http://maven.apache.org/plugins/maven-assembly-plugin/) plugin

## Package directory

The BookKeeper project contains several subfolders that you should be aware of:

Subfolder | Contains
:---------|:--------
[`bookkeeper-server`]({{ site.github_repo }}/tree/master/bookkeeper-server) | The BookKeeper server and client
[`bookkeeper-benchmark`]({{ site.github_repo }}/tree/master/bookkeeper-benchmark) | A benchmarking suite for measuring BookKeeper performance
[`bookkeeper-stats`]({{ site.github_repo }}/tree/master/bookkeeper-stats) | A BookKeeper stats library
[`bookkeeper-stats-providers`]({{ site.github_repo }}/tree/master/bookkeeper-stats-providers) | BookKeeper stats providers
