---
title: The BookKeeper Java client
javadoc_button: true
---

To get started with the Java client for BookKeeper, install the `bookkeeper-server` library as a dependency in your Java application.

> For a more in-depth tutorial that involves a real use case for BookKeeper, see the [Example application](../example-application) guide.

## Installation

The BookKeeper Java client library is available via [Maven Central](http://search.maven.org/) and can be installed using [Maven](#maven), [Gradle](#gradle), and other build tools.

### Maven

If you're using [Maven](https://maven.apache.org/), add this to your [`pom.xml`](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html) build configuration file:

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
    compile group: 'org.apache.bookkeeper', name: 'bookkeeper-server', version: '{{ site.bk_version }}'
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

* Using the `forConfig` method:

  ```java
  BookKeeper bkClient = BookKeeper.forConfig(conf).build();
  ```

## Creating ledgers

The easiest way to create a {% pop ledger %} using the Java client is via the `createLedger` method, which creates a new ledger synchronously and returns a [`LedgerHandle`](/api/org/apache/bookkeeper/client/LedgerHandle). You must specify at least a [`DigestType`](/api/org/apache/bookkeeper/client/BookKeeper.DigestType) and a password.

Here's an example:

```java
byte[] password = "some-password".getBytes();
LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.MAC, password);
```

You can also create ledgers asynchronously

### Create ledgers asynchronously

```java
class LedgerCreationCallback implements AsyncCallback.CreateCallback {
    public void createComplete(int returnCode, LedgerHandle handle, Object ctx) {
        System.out.println("Ledger successfully created");
    }
}

client.asyncCreateLedger(
        3,
        2,
        BookKeeper.DigestType.MAC,
        password,
        new LedgerCreationCallback(),
        "some context"
);
```

## Adding entries to ledgers

```java
long entryId = ledger.addEntry("Some entry data".getBytes());
```

### Add entries asynchronously

## Reading entries from ledgers

```java
Enumerator<LedgerEntry> entries = handle.readEntries(1, 99);
```

To read all possible entries from the ledger:

```java
Enumerator<LedgerEntry> entries =
  handle.readEntries(0, handle.getLastAddConfirmed());

while (entries.hasNextElement()) {
    LedgerEntry entry = entries.nextElement();
    System.out.println("Successfully read entry " + entry.getId());
}
```

## Deleting ledgers

{% pop Ledgers %} can also be deleted synchronously or asynchronously.

```java
long ledgerId = 1234;

try {
    bkClient.deleteLedger(ledgerId);
} catch (Exception e) {
  e.printStackTrace();
}
```

### Delete entries asynchronously

Exceptions thrown:

*

```java
class DeleteEntryCallback implements AsyncCallback.DeleteCallback {
    public void deleteComplete() {
        System.out.println("Delete completed");
    }
}
```
