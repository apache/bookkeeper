---
title: The Ledger API
---

The ledger API is a lower-level API for BookKeeper that enables you to interact with {% pop ledgers %} directly.

## The Java ledger API client

To get started with the Java client for BookKeeper, install the `bookkeeper-server` library as a dependency in your Java application.

> For a more in-depth tutorial that involves a real use case for BookKeeper, see the [Example application](#example-application) guide.

## Installation

The BookKeeper Java client library is available via [Maven Central](http://search.maven.org/) and can be installed using [Maven](#maven), [Gradle](#gradle), and other build tools.

### Maven

If you're using [Maven](https://maven.apache.org/), add this to your [`pom.xml`](https://maven.apache.org/guides/introduction/introduction-to-the-pom.html) build configuration file:

```xml
<!-- in your <properties> block -->
<bookkeeper.version>4.7.2</bookkeeper.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.bookkeeper</groupId>
  <artifactId>bookkeeper-server</artifactId>
  <version>${bookkeeper.version}</version>
</dependency>
```

BookKeeper uses google [protobuf](https://github.com/google/protobuf/tree/master/java) and [guava](https://github.com/google/guava) libraries
a lot. If your application might include different versions of protobuf or guava introduced by other dependencies, you can choose to use the
shaded library, which relocate classes of protobuf and guava into a different namespace to avoid conflicts.

```xml
<!-- in your <properties> block -->
<bookkeeper.version>4.7.2</bookkeeper.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.bookkeeper</groupId>
  <artifactId>bookkeeper-server-shaded</artifactId>
  <version>${bookkeeper.version}</version>
</dependency>
```

### Gradle

If you're using [Gradle](https://gradle.org/), add this to your [`build.gradle`](https://spring.io/guides/gs/gradle/) build configuration file:

```groovy
dependencies {
    compile group: 'org.apache.bookkeeper', name: 'bookkeeper-server', version: '4.7.2'
}

// Alternatively:
dependencies {
    compile 'org.apache.bookkeeper:bookkeeper-server:4.7.2'
}
```

Similarly as using maven, you can also configure to use the shaded jars.

```groovy
// use the `bookkeeper-server-shaded` jar
dependencies {
    compile 'org.apache.bookkeeper:bookkeeper-server-shaded:{{ site.latest-version }}'
}
```

## Connection string

When interacting with BookKeeper using the Java client, you need to provide your client with a connection string, for which you have three options:

* Provide your entire ZooKeeper connection string, for example `zk1:2181,zk2:2181,zk3:2181`.
* Provide a host and port for one node in your ZooKeeper cluster, for example `zk1:2181`. In general, it's better to provide a full connection string (in case the ZooKeeper node you attempt to connect to is down).
* If your ZooKeeper cluster can be discovered via DNS, you can provide the DNS name, for example `my-zookeeper-cluster.com`.

## Creating a new client

In order to create a new [`BookKeeper`](../javadoc/org/apache/bookkeeper/client/BookKeeper) client object, you need to pass in a [connection string](#connection-string). Here is an example client object using a ZooKeeper connection string:

```java
try {
    String connectionString = "127.0.0.1:2181"; // For a single-node, local ZooKeeper cluster
    BookKeeper bkClient = new BookKeeper(connectionString);
} catch (InterruptedException | IOException | KeeperException e) {
    e.printStackTrace();
}
```

> If you're running BookKeeper [locally](../../getting-started/run-locally), using the [`localbookie`](../../reference/cli#bookkeeper-localbookie) command, use `"127.0.0.1:2181"` for your connection string, as in the example above.

There are, however, other ways that you can create a client object:

* By passing in a [`ClientConfiguration`](../javadoc/org/apache/bookkeeper/conf/ClientConfiguration) object. Here's an example:

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

The easiest way to create a {% pop ledger %} using the Java client is via the `createLedger` method, which creates a new ledger synchronously and returns a [`LedgerHandle`](../javadoc/org/apache/bookkeeper/client/LedgerHandle). You must specify at least a [`DigestType`](../javadoc/org/apache/bookkeeper/client/BookKeeper.DigestType) and a password.

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

### Reading entries after the LastAddConfirmed range

`readUnconfirmedEntries` allowing to read after the LastAddConfirmed range.
It lets the client read without checking the local value of LastAddConfirmed, so that it is possible to read entries for which the writer has not received the acknowledge yet.
For entries which are within the range 0..LastAddConfirmed, BookKeeper guarantees that the writer has successfully received the acknowledge.
For entries outside that range it is possible that the writer never received the acknowledge and so there is the risk that the reader is seeing entries before the writer and this could result in a consistency issue in some cases.
With this method you can even read entries before the LastAddConfirmed and entries after it with one call, the expected consistency will be as described above.

```java
Enumerator<LedgerEntry> entries =
  handle.readUnconfirmedEntries(0, lastEntryIdExpectedToRead);

while (entries.hasNextElement()) {
    LedgerEntry entry = entries.nextElement();
    System.out.println("Successfully read entry " + entry.getId());
}
```

## Deleting ledgers

{% pop Ledgers %} can be deleted synchronously which may throw exception:

```java
long ledgerId = 1234;

try {
    bkClient.deleteLedger(ledgerId);
} catch (Exception e) {
  e.printStackTrace();
}
```

### Delete entries asynchronously

{% pop Ledgers %} can also be deleted asynchronously:

```java
class DeleteEntryCallback implements AsyncCallback.DeleteCallback {
    public void deleteComplete() {
        System.out.println("Delete completed");
    }
}
bkClient.asyncDeleteLedger(ledgerID, new DeleteEntryCallback(), null);
```

## Simple example

> For a more involved BookKeeper client example, see the [example application](#example-application) below.

In the code sample below, a BookKeeper client:

* creates a ledger
* writes entries to the ledger
* closes the ledger (meaning no further writes are possible)
* re-opens the ledger for reading
* reads all available entries

```java
// Create a client object for the local ensemble. This
// operation throws multiple exceptions, so make sure to
// use a try/catch block when instantiating client objects.
BookKeeper bkc = new BookKeeper("localhost:2181");

// A password for the new ledger
byte[] ledgerPassword = /* some sequence of bytes, perhaps random */;

// Create a new ledger and fetch its identifier
LedgerHandle lh = bkc.createLedger(BookKeeper.DigestType.MAC, ledgerPassword);
long ledgerId = lh.getId();

// Create a buffer for four-byte entries
ByteBuffer entry = ByteBuffer.allocate(4);

int numberOfEntries = 100;

// Add entries to the ledger, then close it
for (int i = 0; i < numberOfEntries; i++){
	entry.putInt(i);
	entry.position(0);
	lh.addEntry(entry.array());
}
lh.close();

// Open the ledger for reading
lh = bkc.openLedger(ledgerId, BookKeeper.DigestType.MAC, ledgerPassword);

// Read all available entries
Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries - 1);

while(entries.hasMoreElements()) {
	ByteBuffer result = ByteBuffer.wrap(ls.nextElement().getEntry());
	Integer retrEntry = result.getInt();

    // Print the integer stored in each entry
    System.out.println(String.format("Result: %s", retrEntry));
}

// Close the ledger and the client
lh.close();
bkc.close();
```

Running this should return this output:

```shell
Result: 0
Result: 1
Result: 2
# etc
```

## Example application

This tutorial walks you through building an example application that uses BookKeeper as the replicated log. The application uses the [BookKeeper Java client](../java-client) to interact with BookKeeper.

> The code for this tutorial can be found in [this GitHub repo](https://github.com/ivankelly/bookkeeper-tutorial/). The final code for the `Dice` class can be found [here](https://github.com/ivankelly/bookkeeper-tutorial/blob/master/src/main/java/org/apache/bookkeeper/Dice.java).

### Setup

Before you start, you will need to have a BookKeeper cluster running locally on your machine. For installation instructions, see [Installation](../../getting-started/installation).

To start up a cluster consisting of six {% pop bookies %} locally:

```shell
$ bookkeeper-server/bin/bookkeeper localbookie 6
```

You can specify a different number of bookies if you'd like.

### Goal

The goal of the dice application is to have

* multiple instances of this application,
* possibly running on different machines,
* all of which display the exact same sequence of numbers.

In other words, the log needs to be both durable and consistent, regardless of how many {% pop bookies %} are participating in the BookKeeper ensemble. If one of the bookies crashes or becomes unable to communicate with the other bookies in any way, it should *still* display the same sequence of numbers as the others. This tutorial will show you how to achieve this.

To begin, download the base application, compile and run it.

```shell
$ git clone https://github.com/ivankelly/bookkeeper-tutorial.git
$ mvn package
$ mvn exec:java -Dexec.mainClass=org.apache.bookkeeper.Dice
```

That should yield output that looks something like this:

```
[INFO] Scanning for projects...
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building tutorial 1.0-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- exec-maven-plugin:1.3.2:java (default-cli) @ tutorial ---
[WARNING] Warning: killAfter is now deprecated. Do you need it ? Please comment on MEXEC-6.
Value = 4
Value = 5
Value = 3
```

### The base application

The application in this tutorial is a dice application. The `Dice` class below has a `playDice` function that generates a random number between 1 and 6 every second, prints the value of the dice roll, and runs indefinitely.

```java
public class Dice {
    Random r = new Random();

    void playDice() throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            System.out.println("Value = " + (r.nextInt(6) + 1));
        }
    }
}
```

When you run the `main` function of this class, a new `Dice` object will be instantiated and then run indefinitely:

```java
public class Dice {
    // other methods

    public static void main(String[] args) throws InterruptedException {
        Dice d = new Dice();
        d.playDice();
    }
}
```

### Leaders and followers (and a bit of background)

To achieve this common view in multiple instances of the program, we need each instance to agree on what the next number in the sequence will be. For example, the instances must agree that 4 is the first number and 2 is the second number and 5 is the third number and so on. This is a difficult problem, especially in the case that any instance may go away at any time, and messages between the instances can be lost or reordered.

Luckily, there are already algorithms to solve this. Paxos is an abstract algorithm to implement this kind of agreement, while Zab and Raft are more practical protocols. This video gives a good overview about how these algorithms usually look. They all have a similar core.

It would be possible to run the Paxos to agree on each number in the sequence. However, running Paxos each time can be expensive. What Zab and Raft do is that they use a Paxos-like algorithm to elect a leader. The leader then decides what the sequence of events should be, putting them in a log, which the other instances can then follow to maintain the same state as the leader.

Bookkeeper provides the functionality for the second part of the protocol, allowing a leader to write events to a log and have multiple followers tailing the log. However, bookkeeper does not do leader election. You will need a zookeeper or raft instance for that purpose.

### Why not just use ZooKeeper?

There are a number of reasons:

1. Zookeeper's log is only exposed through a tree like interface. It can be hard to shoehorn your application into this.
2. A zookeeper ensemble of multiple machines is limited to one log. You may want one log per resource, which will become expensive very quickly.
3. Adding extra machines to a zookeeper ensemble does not increase capacity nor throughput.

Bookkeeper can be seen as a means of exposing ZooKeeper's replicated log to applications in a scalable fashion. ZooKeeper is still used by BookKeeper, however, to maintain consistency guarantees, though clients don't need to interact with ZooKeeper directly.

### Electing a leader

We'll use zookeeper to elect a leader. A zookeeper instance will have started locally when you started the localbookie application above. To verify it's running, run the following command.

```shell
$ echo stat | nc localhost 2181
Zookeeper version: 3.4.6-1569965, built on 02/20/2014 09:09 GMT
Clients:
 /127.0.0.1:59343[1](queued=0,recved=40,sent=41)
 /127.0.0.1:49354[1](queued=0,recved=11,sent=11)
 /127.0.0.1:49361[0](queued=0,recved=1,sent=0)
 /127.0.0.1:59344[1](queued=0,recved=38,sent=39)
 /127.0.0.1:59345[1](queued=0,recved=38,sent=39)
 /127.0.0.1:59346[1](queued=0,recved=38,sent=39)

Latency min/avg/max: 0/0/23
Received: 167
Sent: 170
Connections: 6
Outstanding: 0
Zxid: 0x11
Mode: standalone
Node count: 16
```

To interact with zookeeper, we'll use the Curator client rather than the stock zookeeper client. Getting things right with the zookeeper client can be tricky, and curator removes a lot of the pointy corners for you. In fact, curator even provides a leader election recipe, so we need to do very little work to get leader election in our application.

```java
public class Dice extends LeaderSelectorListenerAdapter implements Closeable {

    final static String ZOOKEEPER_SERVER = "127.0.0.1:2181";
    final static String ELECTION_PATH = "/dice-elect";

    ...

    Dice() throws InterruptedException {
        curator = CuratorFrameworkFactory.newClient(ZOOKEEPER_SERVER,
                2000, 10000, new ExponentialBackoffRetry(1000, 3));
        curator.start();
        curator.blockUntilConnected();

        leaderSelector = new LeaderSelector(curator, ELECTION_PATH, this);
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }
```

In the constructor for Dice, we need to create the curator client. We specify four things when creating the client, the location of the zookeeper service, the session timeout, the connect timeout and the retry policy.

The session timeout is a zookeeper concept. If the zookeeper server doesn't hear anything from the client for this amount of time, any leases which the client holds will be timed out. This is important in leader election. For leader election, the curator client will take a lease on ELECTION_PATH. The first instance to take the lease will become leader and the rest will become followers. However, their claim on the lease will remain in the cue. If the first instance then goes away, due to a crash etc., its session will timeout. Once the session times out, the lease will be released and the next instance in the queue will become the leader. The call to autoRequeue() will make the client queue itself again if it loses the lease for some other reason, such as if it was still alive, but it a garbage collection cycle caused it to lose its session, and thereby its lease. I've set the lease to be quite low so that when we test out leader election, transitions will be quite quick. The optimum length for session timeout depends very much on the use case. The other parameters are the connection timeout, i.e. the amount of time it will spend trying to connect to a zookeeper server before giving up, and the retry policy. The retry policy specifies how the client should respond to transient errors, such as connection loss. Operations that fail with transient errors can be retried, and this argument specifies how often the retries should occur.

Finally, you'll have noticed that Dice now extends LeaderSelectorListenerAdapter and implements Closeable. Closeable is there to close the resource we have initialized in the constructor, the curator client and the leaderSelector. LeaderSelectorListenerAdapter is a callback that the leaderSelector uses to notify the instance that it is now the leader. It is passed as the third argument to the LeaderSelector constructor.

```java
    @Override
    public void takeLeadership(CuratorFramework client)
            throws Exception {
        synchronized (this) {
            leader = true;
            try {
                while (true) {
                    this.wait();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                leader = false;
            }
        }
    }
```

takeLeadership() is the callback called by LeaderSelector when the instance is leader. It should only return when the instance wants to give up leadership. In our case, we never do so we wait on the current object until we're interrupted. To signal to the rest of the program that we are leader we set a volatile boolean called leader to true. This is unset after we are interrupted.

```java
    void playDice() throws InterruptedException {
        while (true) {
            while (leader) {
                Thread.sleep(1000);
                System.out.println("Value = " + (r.nextInt(6) + 1)
                                   + ", isLeader = " + leader);
            }
        }
    }
```

Finally, we modify the `playDice` function to only generate random numbers when it is the leader.

Run two instances of the program in two different terminals. You'll see that one becomes leader and prints numbers and the other just sits there.

Now stop the leader using Control-Z. This will pause the process, but it won't kill it. You will be dropped back to the shell in that terminal. After a couple of seconds, the session timeout, you will see that the other instance has become the leader. Zookeeper will guarantee that only one instance is selected as leader at any time.

Now go back to the shell that the original leader was on and wake up the process using fg. You'll see something like the following:

```shell
...
...
Value = 4, isLeader = true
Value = 4, isLeader = true
^Z
[1]+  Stopped                 mvn exec:java -Dexec.mainClass=org.apache.bookkeeper.Dice
$ fg
mvn exec:java -Dexec.mainClass=org.apache.bookkeeper.Dice
Value = 3, isLeader = true
Value = 1, isLeader = false
```

## New API

Since 4.6 BookKeeper provides a new client API which leverages Java8 [CompletableFuture](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html) facility.
[WriteHandle](../javadoc/org/apache/bookkeeper/client/api/WriteHandle), [WriteAdvHandle](../javadoc/org/apache/bookkeeper/client/api/WriteAdvHandle), [ReadHandle](../javadoc/org/apache/bookkeeper/client/api/ReadHandle) are introduced for replacing the generic [LedgerHandle](../javadoc/org/apache/bookkeeper/client/LedgerHandle).

> All the new API now is available in `org.apache.bookkeeper.client.api`. You should only use interfaces defined in this package.

*Beware* that this API in 4.6 is still experimental API and can be subject to changes in next minor releases.

### Create a new client

In order to create a new [`BookKeeper`](../javadoc/org/apache/bookkeeper/client/api/BookKeeper) client object, you need to construct a [`ClientConfiguration`](../javadoc/org/apache/bookkeeper/conf/ClientConfiguration) object and set a [connection string](#connection-string) first, and then use [`BookKeeperBuilder`](../javadoc/org/apache/bookkeeper/client/api/BookKeeperBuilder) to build the client.

Here is an example building the bookkeeper client.

```java
// construct a client configuration instance
ClientConfiguration conf = new ClientConfiguration();
conf.setZkServers(zkConnectionString);
conf.setZkLedgersRootPath("/path/to/ledgers/root");

// build the bookkeeper client
BookKeeper bk = BookKeeper.newBuilder(conf)
    .statsLogger(...)
    ...
    .build();

```

### Create ledgers

the easiest way to create a {% pop ledger %} using the java client is via the [`createbuilder`](../javadoc/org/apache/bookkeeper/client/api/createbuilder). you must specify at least
a [`digesttype`](../javadoc/org/apache/bookkeeper/client/api/digesttype) and a password.

here's an example:

```java
BookKeeper bk = ...;

byte[] password = "some-password".getBytes();

WriteHandle wh = bk.newCreateLedgerOp()
    .withDigestType(DigestType.CRC32)
    .withPassword(password)
    .withEnsembleSize(3)
    .withWriteQuorumSize(3)
    .withAckQuorumSize(2)
    .execute()          // execute the creation op
    .get();             // wait for the execution to complete
```

A [`WriteHandle`](../javadoc/org/apache/bookkeeper/client/api/WriteHandle) is returned for applications to write and read entries to and from the ledger.

### Write flags

You can specify behaviour of the writer by setting [`WriteFlags`](../javadoc/org/apache/bookkeeper/client/api/WriteFlag) at ledger creation type.
These flags are applied only during write operations and are not recorded on metadata.


Available write flags:

| Flag  | Explanation  | Notes |
:---------|:------------|:-------
DEFERRED_SYNC | Writes are acknowledged early, without waiting for
guarantees of durability | Data will be only written to the OS page cache, without forcing an fsync.

```java
BookKeeper bk = ...;

byte[] password = "some-password".getBytes();

WriteHandle wh = bk.newCreateLedgerOp()
    .withDigestType(DigestType.CRC32)
    .withPassword(password)
    .withEnsembleSize(3)
    .withWriteQuorumSize(3)
    .withAckQuorumSize(2)
    .withWriteFlags(DEFERRED_SYNC)
    .execute()          // execute the creation op
    .get();             // wait for the execution to complete
```


### Append entries to ledgers

The [`WriteHandle`](../javadoc/org/apache/bookkeeper/client/api/WriteHandle) can be used for applications to append entries to the ledgers.

```java
WriteHandle wh = ...;

CompletableFuture<Long> addFuture = wh.append("Some entry data".getBytes());

// option 1: you can wait for add to complete synchronously
try {
    long entryId = FutureUtils.result(addFuture.get());
} catch (BKException bke) {
    // error handling
}

// option 2: you can process the result and exception asynchronously
addFuture
    .thenApply(entryId -> {
        // process the result
    })
    .exceptionally(cause -> {
        // handle the exception
    })

// option 3: bookkeeper provides a twitter-future-like event listener for processing result and exception asynchronously
addFuture.whenComplete(new FutureEventListener() {
    @Override
    public void onSuccess(long entryId) {
        // process the result
    }
    @Override
    public void onFailure(Throwable cause) {
        // handle the exception
    }
});
```

The append method supports three representations of a bytes array: the native java `byte[]`, java nio `ByteBuffer` and netty `ByteBuf`.
It is recommended to use `ByteBuf` as it is more gc friendly.

### Open ledgers

You can open ledgers to read entries. Opening ledgers is done by [`openBuilder`](../javadoc/org/apache/bookkeeper/client/api/openBuilder). You must specify the ledgerId and the password
in order to open the ledgers.

here's an example:

```java
BookKeeper bk = ...;

long ledgerId = ...;
byte[] password = "some-password".getBytes();

ReadHandle rh = bk.newOpenLedgerOp()
    .withLedgerId(ledgerId)
    .withPassword(password)
    .execute()          // execute the open op
    .get();             // wait for the execution to complete
```

A [`ReadHandle`](../javadoc/org/apache/bookkeeper/client/api/ReadHandle) is returned for applications to read entries to and from the ledger.

#### Recovery vs NoRecovery

By default, the [`openBuilder`](../javadoc/org/apache/bookkeeper/client/api/openBuilder) opens the ledger in a `NoRecovery` mode. You can open the ledger in `Recovery` mode by specifying
`withRecovery(true)` in the open builder.

```java
BookKeeper bk = ...;

long ledgerId = ...;
byte[] password = "some-password".getBytes();

ReadHandle rh = bk.newOpenLedgerOp()
    .withLedgerId(ledgerId)
    .withPassword(password)
    .withRecovery(true)
    .execute()
    .get();

```

**What is the difference between "Recovery" and "NoRecovery"?**

If you are opening a ledger in "Recovery" mode, it will basically fence and seal the ledger -- no more entries are allowed
to be appended to it. The writer which is currently appending entries to the ledger will fail with [`LedgerFencedException`](../javadoc/org/apache/bookkeeper/client/api/BKException.Code#LedgerFencedException).

In constrat, opening a ledger in "NoRecovery" mode, it will not fence and seal the ledger. "NoRecovery" mode is usually used by applications to tailing-read from a ledger.

### Read entries from ledgers

The [`ReadHandle`](../javadoc/org/apache/bookkeeper/client/api/ReadHandle) returned from the open builder can be used for applications to read entries from the ledgers.

```java
ReadHandle rh = ...;

long startEntryId = ...;
long endEntryId = ...;
CompletableFuture<LedgerEntries> readFuture = rh.read(startEntryId, endEntryId);

// option 1: you can wait for read to complete synchronously
try {
    LedgerEntries entries = FutureUtils.result(readFuture.get());
} catch (BKException bke) {
    // error handling
}

// option 2: you can process the result and exception asynchronously
readFuture
    .thenApply(entries -> {
        // process the result
    })
    .exceptionally(cause -> {
        // handle the exception
    })

// option 3: bookkeeper provides a twitter-future-like event listener for processing result and exception asynchronously
readFuture.whenComplete(new FutureEventListener<>() {
    @Override
    public void onSuccess(LedgerEntries entries) {
        // process the result
    }
    @Override
    public void onFailure(Throwable cause) {
        // handle the exception
    }
});
```

Once you are done with processing the [`LedgerEntries`](../javadoc/org/apache/bookkeeper/client/api/LedgerEntries), you can call `#close()` on the `LedgerEntries` instance to
release the buffers held by it.

Applications are allowed to read any entries between `0` and [`LastAddConfirmed`](../javadoc/org/apache/bookkeeper/client/api/ReadHandle.html#getLastAddConfirmed). If the applications
attempts to read entries beyond `LastAddConfirmed`, they will receive [`IncorrectParameterException`](../javadoc/org/apache/bookkeeper/client/api/BKException.Code#IncorrectParameterException).

### Read unconfirmed entries from ledgers

`readUnconfirmed` is provided the mechanism for applications to read entries beyond `LastAddConfirmed`. Applications should be aware of `readUnconfirmed` doesn't provide any
repeatable read consistency.

```java
CompletableFuture<LedgerEntries> readFuture = rh.readUnconfirmed(startEntryId, endEntryId);
```

### Tailing Reads

There are two methods for applications to achieve tailing reads: `Polling` and `Long-Polling`.

#### Polling

You can do this in synchronous way:

```java
ReadHandle rh = ...;

long startEntryId = 0L;
long nextEntryId = startEntryId;
int numEntriesPerBatch = 4;
while (!rh.isClosed() || nextEntryId <= rh.getLastAddConfirmed()) {
    long lac = rh.getLastAddConfirmed();
    if (nextEntryId > lac) {
        // no more entries are added
        Thread.sleep(1000);

        lac = rh.readLastAddConfirmed().get();
        continue;
    }

    long endEntryId = Math.min(lac, nextEntryId + numEntriesPerBatch - 1);
    LedgerEntries entries = rh.read(nextEntryId, endEntryId).get();

    // process the entries

    nextEntryId = endEntryId + 1;
}
```

#### Long Polling

```java
ReadHandle rh = ...;

long startEntryId = 0L;
long nextEntryId = startEntryId;
int numEntriesPerBatch = 4;
while (!rh.isClosed() || nextEntryId <= rh.getLastAddConfirmed()) {
    long lac = rh.getLastAddConfirmed();
    if (nextEntryId > lac) {
        // no more entries are added
        try (LastConfirmedAndEntry lacAndEntry = rh.readLastAddConfirmedAndEntry(nextEntryId, 1000, false).get()) {
            if (lacAndEntry.hasEntry()) {
                // process the entry

                ++nextEntryId;
            }
        }
    } else {
        long endEntryId = Math.min(lac, nextEntryId + numEntriesPerBatch - 1);
        LedgerEntries entries = rh.read(nextEntryId, endEntryId).get();

        // process the entries
        nextEntryId = endEntryId + 1;
    }
}
```

### Delete ledgers

{% pop Ledgers %} can be deleted by using [`DeleteBuilder`](../javadoc/org/apache/bookkeeper/client/api/DeleteBuilder).

```java
BookKeeper bk = ...;
long ledgerId = ...;

bk.newDeleteLedgerOp()
    .withLedgerId(ledgerId)
    .execute()
    .get();
```
