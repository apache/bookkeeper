---
title: The BookKeeper Java client
javadoc_button: true
---

To get started with the Java client for BookKeeper

## Installation

The BookKeeper Java client library is available via [Maven Central](http://search.maven.org/) and can be installed using [Maven](#maven), [Gradle](#gradle), and other build tools.

### Maven

If you're using [Maven](https://maven.apache.org/) add this to your [`pom.xml`](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html) build configuration file:

```xml
<!-- in your <properties> block -->
<bookkeeper.version>{{ site.bk_version }}</bookkeeper.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.bookkeeper</groupId>
  <artifactId>bookkeeper-server</artifactId>
  <version>${bookkeeper.version}</version>
</dependency>
```

### Gradle

If you're using [Gradle](https://gradle.org/), add this to your [`build.gradle`](https://spring.io/guides/gs/gradle/) build configuration file:

```groovy
dependencies {
    compile group: 'org.apache.bookkeeper', name: 'bookkeeper', version: '{{ site.bk_version }}'
}

// Alternatively:
dependencies {
    compile 'org.apache.bookkeeper:bookkeeper-server:{{ site.bk_version }}'
}
```

## Creating a new client

The easiest way to create a new [`BookKeeper`](/api/org/apache/bookkeeper/client/BookKeeper) client is to pass in a ZooKeeper connection string as the sole constructor:

```java
try {
    String zkConnectionString = "127.0.0.1:2181";
    BookKeeper bkClient = new BookKeeper(zkConnectionString);
} catch (InterruptedException | IOException | KeeperException e) {
    e.printStackTrace();
}
```

There are, however, other ways that you can create a client object:

* By passing in a [`ClientConfiguration`](/api/org/apache/bookkeeper/conf/ClientConfiguration) object. Here's an example:

  ```java
  ClientConfiguration config = new ClientConfiguration();
  config.setZkServers(zkConnectionString);
  config.setAddEntryTimeout(2000);
  BookKeeper bkClient = new BookKeeper(config);
  ```

* By specifying a `ClientConfiguration` and a [`ZooKeeper`](http://zookeeper.apache.org/doc/current/api/org/apache/zookeeper/ZooKeeper.html) client object:

  ```java
  ClientConfiguration config = new ClientConfiguration();
  config.setAddEntryTimeout(5000);
  ZooKeeper zkClient = new ZooKeeper(/* client args */);
  BookKeeper bkClient = new BookKeeper(config, zkClient);
  ```

  > You can also pass in a Curator client object. TODO.

## Creating ledgers

## Adding entries to ledgers
