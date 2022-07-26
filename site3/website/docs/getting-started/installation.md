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



## Build using Maven

Once you have the BookKeeper on your local machine, either by [downloading](#download) or [cloning](#clone) it, you can then build BookKeeper from source using Maven:

```shell
$ mvn package
```

Since 4.8.0, bookkeeper introduces `table service`. If you would like to build and tryout table service, you can build it with `stream` profile.

```shell
$ mvn package -Dstream
```

> You can skip tests by adding the `-DskipTests` flag when running `mvn package`.

### Useful Maven commands

Some other useful Maven commands beyond `mvn package`:

| Command                                      | Action                                                                                                                |
|:---------------------------------------------|:----------------------------------------------------------------------------------------------------------------------|
| `mvn clean`                                  | Removes build artifacts                                                                                               |
| `mvn compile`                                | Compiles JAR files from Java sources                                                                                  |
| `mvn compile spotbugs:spotbugs`              | Compile using the Maven [SpotBugs](https://github.com/spotbugs/spotbugs-maven-plugin) plugin                          |
| `mvn install`                                | Install the BookKeeper JAR locally in your local Maven cache (usually in the `~/.m2` directory)                       |
| `mvn deploy`                                 | Deploy the BookKeeper JAR to the Maven repo (if you have the proper credentials)                                      |
| `mvn verify`                                 | Performs a wide variety of verification and validation tasks                                                          |
| `mvn apache-rat:check`                       | Run Maven using the [Apache Rat](http://creadur.apache.org/rat/apache-rat-plugin/) plugin                             |
| `mvn compile javadoc:aggregate`              | Build Javadocs locally                                                                                                |
| `mvn -am -pl bookkeeper-dist/server package` | Build a server distribution using the Maven [Assembly](http://maven.apache.org/plugins/maven-assembly-plugin/) plugin |

> You can enable `table service` by adding the `-Dstream` flag when running above commands.

## Package directory

The BookKeeper project contains several subfolders that you should be aware of:

| Subfolder                                                                                     | Contains                                                  |
|:----------------------------------------------------------------------------------------------|:----------------------------------------------------------|
| [`bookkeeper-server`]({{ site.github_repo }}/tree/master/bookkeeper-server)                   | The BookKeeper server and client                          |
| [`bookkeeper-benchmark`]({{ site.github_repo }}/tree/master/bookkeeper-benchmark)             | A benchmarking suite for measuring BookKeeper performance |
| [`bookkeeper-stats`]({{ site.github_repo }}/tree/master/bookkeeper-stats)                     | A BookKeeper stats library                                |
| [`bookkeeper-stats-providers`]({{ site.github_repo }}/tree/master/bookkeeper-stats-providers) | BookKeeper stats providers                                |
