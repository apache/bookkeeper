---
title: Run BookKeeper locally
next: ../concepts
---

This tutorial guides you through every step of running BookKeeper locally.

## Install BookKeeper standalone

Before installation, you need to check system requirements.

### System requirement

* [Unix environment](http://www.opengroup.org/unix)
  
* [Java Development Kit 1.6](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later
  
* [Maven 3.0](https://maven.apache.org/install.html) or later

### Download BookKeeper

Use one of the following methods to download BookKeeper.

* Download from the [BookKeeper GitHub mirror]({{ site.github_repo }}) or the [BookKeeper Apache repository](http://git.apache.org/bookkeeper.git/).

    ```shell
    # From the GitHub mirror
    $ git clone https://github.com/apache/bookkeeper

    # From Apache directly
    $ git clone git://git.apache.org/bookkeeper.git/
    ```

* Download from one of the [Apache mirrors](http://www.apache.org/dyn/closer.cgi/bookkeeper). 

    **Example**

    Download from the [apache.claz.org](http://apache.claz.org/bookkeeper) mirror.

    ```shell
    $ curl -O {{ download_url }}
    $ tar xvf bookkeeper-{{ site.latest_release }}-src.tar.gz
    $ cd bookkeeper-{{ site.latest_release }}
    ```

* Download from the [BookKeeper download page](https://bookkeeper.apache.org/releases/).

* Download from the [BookKeeper release page](https://github.com/apache/bookkeeper/releases).

### Build BookKeeper

Once you have BookKeeper on your local machine, either by any download methods, you can build BookKeeper from source using Maven:

```shell
$ mvn package
```

> #### Tip
> 
> * If you want to skip tests, run `mvn package -DskipTests`.
>
> * Since 4.8.0, BookKeeper introduces `table service`. If you want to build and try `table service`, run `mvn package -Dstream`.
>
> * The following Maven commands are helpful for using BookKeeper.
>
>  Command | Description
> :-------|:------
> `mvn clean` | Remove build artifacts
> `mvn compile` | Compile JAR files from Java sources
> `mvn compile spotbugs:spotbugs` | Compile using the Maven [SpotBugs](https://github.com/spotbugs/spotbugs-maven-plugin) plugin
> `mvn install` | Install the BookKeeper JAR locally in your local Maven cache (usually in the `~/.m2` directory)
> `mvn deploy` | Deploy the BookKeeper JAR to the Maven repo (if you have the proper credentials)
> `mvn verify` | Performs a wide variety of verification and validation tasks
> `mvn apache-rat:check` | Run Maven using the [Apache Rat](http://creadur.apache.org/rat/apache-rat-plugin/) plugin
> `mvn compile javadoc:aggregate` | Build Javadocs locally
> `mvn -am -pl bookkeeper-dist/server package` | Build a server distribution using the Maven [Assembly](http://maven.apache.org/plugins/maven-assembly-plugin/) plugin

### Package directory

After BookKeeper has been built successfully, the BookKeeper project contains several subfolders, you should be aware of the following subfolders.

Subfolder | Description
:---------|:--------
[`bookkeeper-server`]({{ site.github_repo }}/tree/master/bookkeeper-server) | BookKeeper server and client
[`bookkeeper-benchmark`]({{ site.github_repo }}/tree/master/bookkeeper-benchmark) | A benchmarking suite for measuring BookKeeper performance
[`bookkeeper-stats`]({{ site.github_repo }}/tree/master/bookkeeper-stats) | BookKeeper stats library
[`bookkeeper-stats-providers`]({{ site.github_repo }}/tree/master/bookkeeper-stats-providers) | BookKeeper stats providers
[`stream`]({{ site.github_repo }}/tree/master/stream) | BookKeeper table service

## Start BookKeeper standalone

{% pop Bookies %} are individual BookKeeper servers. 

You can run an ensemble of bookies locally on a single machine using the [`localbookie`](../../reference/cli#bookkeeper-localbookie) command of the [`BookKeeper CLI tool`](../../reference/cli) and specify the desired number of bookies.

When you start an ensemble using `localbookie`, all bookies run in a single JVM process.

**Example**

Start an ensemble with 10 bookies.

```shell
$ bin/bookkeeper localbookie 10
```

## Use BookKeeper standalone

You can use BookKeeper via ledger API. 

For **how to use Java client for BookKeeper with ledger API**, see [Ledger API](../../api/ledger-api).

## Stop BookKeeper standalone

Press `Ctrl+C` to stop a local standalone BookKeeper.

