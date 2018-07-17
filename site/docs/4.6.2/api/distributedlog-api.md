---
title: DistributedLog
subtitle: A higher-level API for managing BookKeeper entries
---

> DistributedLog began its life as a separate project under the Apache Foundation. It was merged into BookKeeper in 2017.

The DistributedLog API is an easy-to-use interface for managing BookKeeper entries that enables you to use BookKeeper without needing to interact with [ledgers](../ledger-api) directly.

DistributedLog (DL) maintains sequences of records in categories called *logs* (aka *log streams*). *Writers* append records to DL logs, while *readers* fetch and process those records.

## Architecture

The diagram below illustrates how the DistributedLog API works with BookKeeper:

![DistributedLog API]({{ site.baseurl }}img/distributedlog.png)

## Logs

A *log* in DistributedLog is an ordered, immutable sequence of *log records*.

The diagram below illustrates the anatomy of a log stream:

![DistributedLog log]({{ site.baseurl }}img/logs.png)

### Log records

Each log record is a sequence of bytes. Applications are responsible for serializing and deserializing byte sequences stored in log records.

Log records are written sequentially into a *log stream* and assigned with a a unique sequence number called a DLSN (<strong>D</strong>istributed<strong>L</strong>og <strong>S</strong>equence <strong>N</strong>umber).

In addition to a DLSN, applications can assign their own sequence number when constructing log records. Application-defined sequence numbers are known as *TransactionIDs* (or *txid*). Either a DLSN or a TransactionID can be used for positioning readers to start reading from a specific log record.

### Log segments

Each log is broken down into *log segments* that contain subsets of records. Log segments are distributed and stored in BookKeeper. DistributedLog rolls the log segments based on the configured *rolling policy*, which be either

* a configurable period of time (such as every 2 hours), or
* a configurable maximum size (such as every 128 MB).

The data in logs is divided up into equally sized log segments and distributed evenly across {% pop bookies %}. This allows logs to scale beyond a size that would fit on a single server and spreads read traffic across the cluster.

### Namespaces

Log streams that belong to the same organization are typically categorized and managed under a *namespace*. DistributedLog namespaces essentially enable applications to locate log streams. Applications can perform the following actions under a namespace:

* create streams
* delete streams
* truncate streams to a given sequence number (either a DLSN or a TransactionID)

## Writers

Through the DistributedLog API, writers write data into logs of their choice. All records are appended into logs in order. The sequencing is performed by the writer, which means that there is only one active writer for a log at any given time.

DistributedLog guarantees correctness when two writers attempt to write to the same log when a network partition occurs using a *fencing* mechanism in the log segment store.

### Write Proxy

Log writers are served and managed in a service tier called the *Write Proxy* (see the diagram [above](#architecture)). The Write Proxy is used for accepting writes from a large number of clients.

## Readers

DistributedLog readers read records from logs of their choice, starting with a provided position. The provided position can be either a DLSN or a TransactionID.

Readers read records from logs in strict order. Different readers can read records from different positions in the same log.

Unlike other pub-sub systems, DistributedLog doesn't record or manage readers' positions. This means that tracking is the responsibility of applications, as different applications may have different requirements for tracking and coordinating positions. This is hard to get right with a single approach. Distributed databases, for example, might store reader positions along with SSTables, so they would resume applying transactions from the positions store in SSTables. Tracking reader positions could easily be done at the application level using various stores (such as ZooKeeper, the filesystem, or key-value stores).

### Read Proxy

Log records can be cached in a service tier called the *Read Proxy* to serve a large number of readers. See the diagram [above](#architecture). The Read Proxy is the analogue of the [Write Proxy](#write-proxy).

## Guarantees

The DistributedLog API for BookKeeper provides a number of guarantees for applications:

* Records written by a [writer](#writers) to a [log](#logs) are appended in the order in which they are written. If a record **R1** is written by the same writer as a record **R2**, **R1** will have a smaller sequence number than **R2**.
* [Readers](#readers) see [records](#log-records) in the same order in which they are [written](#writers) to the log.
* All records are persisted on disk by BookKeeper before acknowledgements, which guarantees durability.
* For a log with a replication factor of N, DistributedLog tolerates up to N-1 server failures without losing any records.

## API

Documentation for the DistributedLog API can be found [here](https://bookkeeper.apache.org/distributedlog/docs/latest/user_guide/api/core).

> At a later date, the DistributedLog API docs will be added here.

<!--

The DistributedLog core library is written in Java and interacts with namespaces and logs directly.

### Installation

The BookKeeper Java client library is available via [Maven Central](http://search.maven.org/) and can be installed using [Maven](#maven), [Gradle](#gradle), and other build tools.

### Maven

If you're using [Maven](https://maven.apache.org/), add this to your [`pom.xml`](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html) build configuration file:

```xml
<-- in your <properties> block ->
<bookkeeper.version>{{ site.distributedlog_version }}</bookkeeper.version>

<-- in your <dependencies> block ->
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
    compile group: 'org.apache.bookkeeper', name: 'bookkeeper-server', version: '4.6.2'
}

// Alternatively:
dependencies {
    compile 'org.apache.bookkeeper:bookkeeper-server:4.6.2'
}
```

### Namespace API

A DL [namespace](#namespace) is a collection of [log streams](#log-streams). When using the DistributedLog API with BookKeeper, you need to provide your Java client with a namespace URI. That URI consists of three elements:

1. The `distributedlog-bk` scheme
1. A connection string for your BookKeeper cluster. You have three options for the connection string:
   * An entire ZooKeeper connection string, for example `zk1:2181,zk2:2181,zk3:2181`
   * A host and port for one node in your ZooKeeper cluster, for example `zk1:2181`. In general, it's better to provide a full ZooKeeper connection string.
   * If your ZooKeeper cluster can be discovered via DNS, you can provide the DNS name, for example `my-zookeeper-cluster.com`.
1. A path that points to the location where logs are stored. This could be a ZooKeeper [znode](https://zookeeper.apache.org/doc/current/zookeeperOver.html).
  
This is the general structure of a namespace URI:

```shell
distributedlog-bk://{connection-string}/{path}
```

Here are some example URIs:

```shell
distributedlog-bk://zk1:2181,zk2:2181,zk3:2181/my-namespace # Full ZooKeeper connection string
distributedlog-bk://localhost:2181/my-namespace             # Single ZooKeeper node
distributedlog-bk://my-zookeeper-cluster.com/my-namespace   # DNS name for ZooKeeper
```

#### Creating namespaces

In order to create namespaces, you need to use the command-line tool.

```shell
$ 
```

#### Using namespaces

Once you have a namespace URI, you can build a namespace instance, which will be used for operating streams. Use the `DistributedLogNamespaceBuilder` to build a `DistributedLogNamespace` object, passing in a `DistributedLogConfiguration`, a URI, and optionally a stats logger and a feature provider.

```java
DistributedLogConfiguration conf = new DistributedLogConfiguration();
URI uri = URI.create("distributedlog-bk://localhost:2181/my-namespace ");
DistributedLogNamespaceBuilder builder = DistributedLogNamespaceBuilder.newBuilder();
DistributedLogNamespace = builder
        .conf(conf)           // Configuration for the namespace
        .uri(uri)             // URI for the namespace
        .statsLogger(...)     // Stats logger for statistics
        .featureProvider(...) // Feature provider for controlling features
        .build();
```

### Log API

#### Creating logs

You can create a log by calling the `createLog` method on a `DistributedLogNamespace` object, passing in a name for the log. This creates the log under the namespace but does *not* return a handle for operating the log.

```java
DistributedLogNamespace namespace = /* Create namespace */;
try {
    namespace.createLog("test-log");
} catch (IOException e) }
    // Handle the log creation exception
}
```

#### Opening logs

A `DistributedLogManager` handle will be returned when opening a log using the `openLog` function, which takes the name of the log. This handle can be used for writing records to or reading records from the log.

> If the log doesn't exist and `createStreamIfNotExists` is set to `true` in the configuration, the log will be created automatically when writing the first record.

```java
DistributedLogConfiguration conf = new DistributedLogConfiguration();
conf.setCreateStreamIfNotExists(true);
DistributedLogNamespace namespace = DistributedLogNamespace.newBuilder()
        .conf(conf)
        // Other builder attributes
        .build();
DistributedLogManager logManager = namespace.openLog("test-log");
```

Sometimes, applications may open a log with a different configuration from the enclosing namespace. This can be done using the same `openLog` method:

```java
// Namespace configuration
DistributedLogConfiguration namespaceConf = new DistributedLogConfiguration();
conf.setRetentionPeriodHours(24);
URI uri = URI.create("distributedlog-bk://localhost:2181/my-namespace");
DistributedLogNamespace namespace = DistributedLogNamespace.newBuilder()
        .conf(namespaceConf)
        .uri(uri)
        // Other builder attributes
        .build();
// Log-specific configuration
DistributedLogConfiguration logConf = new DistributedLogConfiguration();
logConf.setRetentionPeriodHours(12);
DistributedLogManager logManager = namespace.openLog(
        "test-log",
        Optional.of(logConf),
        Optional.absent()
);
```

#### Deleting logs

The `DistributedLogNamespace` class provides `deleteLog` function that can be used to delete logs. When you delete a lot, the client library will attempt to acquire a lock on the log before deletion. If the log is being written to by an active writer, deletion will fail (as the other writer currently holds the lock).

```java
try {
    namespace.deleteLog("test-log");
} catch (IOException e) {
    // Handle exception
}
```

#### Checking for the existence of a log

Applications can check whether a log exists by calling the `logExists` function.

```java
if (namespace.logExists("test-log")) {
  // Perform some action when the log exists
} else {
  // Perform some action when the log doesn't exist
}
```

#### Listing logs

Applications can retrieve a list of all logs under a namespace using the `getLogs` function.

```java
Iterator<String> logs = namespace.getLogs();
while (logs.hasNext()) {
  String logName = logs.next();
  // Do something with the log name, such as print
}
```

### Writer API

You can write to DistributedLog logs either [synchronously](#writing-to-logs-synchronously) using the `LogWriter` class or [asynchronously](#writing-to-logs-asynchronously) using the `AsyncLogWriter` class.

#### Immediate flush

By default, records are buffered rather than being written immediately. You can disable this behavior and make DL writers write ("flush") entries immediately by adding the following to your configuration object:

```java
conf.setImmediateFlushEnabled(true);
conf.setOutputBufferSize(0);
conf.setPeriodicFlushFrequencyMilliSeconds(0);
```

#### Immediate locking

By default, DL writers can write to a log stream when other writers are also writing to that stream. You can override this behavior and disable other writers from writing to the stream by adding this to your configuration:

```java
conf.setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE);
```

#### Writing to logs synchronously

To write records to a log synchronously, you need to instantiate a `LogWriter` object using a `DistributedLogManager`. Here's an example:

```java
DistributedLogNamespace namespace = /* Some namespace object */;
DistributedLogManager logManager = namespace.openLog("test-log");
LogWriter writer = logManager.startLogSegmentNonPartitioned();
```

> The DistributedLog library enforces single-writer semantics by deploying a ZooKeeper locking mechanism. If there is only one active writer, subsequent calls to `startLogSegmentNonPartitioned` will fail with an `OwnershipAcquireFailedException`.

Log records represent the data written to a log stream. Each log record is associated with an application-defined [TransactionID](#log-records). This ID must be non decreasing or else writing a record will be rejected with `TransactionIdOutOfOrderException`. The application is allowed to bypass the TransactionID sanity checking by setting `maxIdSanityCheck` to `false` in the configuration. System time and atomic numbers are good candidates for TransactionID.

```java
long txid = 1L;
byte[] data = "some byte array".getBytes();
LogRecord record = new LogRecord(txid, data);
```

Your application can write either a single record, using the `write` method, or many records, using the `writeBulk` method.

```java
// Single record
writer.write(record);

// Bulk write
List<LogRecord> records = Lists.newArrayList();
records.add(record);
writer.writeBulk(records);
```

The write calls return immediately after the records are added into the output buffer of writer. This means that the data isn't guaranteed to be durable until the writer explicitly calls `setReadyToFlush` and `flushAndSync`. Those two calls will first transmit buffered data to the backend, wait for transmit acknowledgements (acks), and commit the written data to make them visible to readers.

```java
// Flush the records
writer.setReadyToFlush();

// Commit the records to make them visible to readers
writer.flushAndSync();
```

Log streams in DistributedLog are endless streams *unless they are sealed*. Endless in this case means that writers can keep writing records to those streams, readers can keep reading from the end of those streams, and the process never stops. Your application can seal a log stream using the `markEndOfStream` method:

```java
writer.markEndOfStream();
```

#### Writing to logs asynchronously

In order to write to DistributedLog logs asynchronously, you need to create an `AsyncLogWriter` instread of a `LogWriter`.

```java
DistributedLogNamespace namespace = /* Some namespace object */;
DistributedLogManager logManager = namespace.openLog("test-async-log");
AsyncLogWriter asyncWriter = logManager.startAsyncLogSegmentNonPartitioned();
```

All writes to `AsyncLogWriter` are non partitioned. The futures representing write results are only satisfied when the data is durably persisted in the stream. A [DLSN](#log-records) will be returned for each write, which is used to represent the position (aka offset) of the record in the log stream. All the records added in order are guaranteed to be persisted in order. Here's an example of an async writer that gathers a list of futures representing multiple async write results:

```java
List<Future<DLSN>> addFutures = Lists.newArrayList();
for (long txid = 1L; txid <= 100L; txid++) {
    byte[] data = /* some byte array */;
    LogRecord record = new LogRecord(txid, data);
    addFutures.add(asyncWriter.write(record));
}
List<DLSN> addResults = Await.result(Future.collect(addFutures));
```

The `AsyncLogWriter` also provides a method for truncating a stream to a given DLSN. This is useful for building replicated state machines that need explicit controls on when the data can be deleted.

```java
DLSN truncateDLSN = /* some DLSN */;
Future<DLSN> truncateFuture = asyncWriter.truncate(truncateDLSN);

// Wait for truncation result
Await.result(truncateFuture);
```

##### Register a listener

Instead of returning a future from write operations, you can also set up a listener that performs assigned actions upon success or failure of the write. Here's an example:

```java
asyncWriter.addEventListener(new FutureEventListener<DLSN>() {
    @Override
    public void onFailure(Throwable cause) {
        // Execute if the attempt fails
    }

    @Override
    public void onSuccess(DLSN value) {
        // Execute if the attempt succeeds
    }
});
```

##### Close the writer

You can close an async writer when you're finished with it like this:

```java
FutureUtils.result(asyncWriter.asyncClose());
```

<!--
TODO: Reader API
-->
