---
title: BookKeeper concepts and architecture
subtitle: What drives BookKeeper and how it's built
prev: /docs/getting-started/run-locally
next: /docs/applications/java-client
---

BookKeeper is a service used for persistent storage of log streams of records. BookKeeper replicates stored records across multiple servers.

## Basic terms

In BookKeeper:

* each unit of a log is an [*entry*](#entries) (aka record)
* streams of log entries are called [*ledgers*](#ledgers)
* individual servers storing ledgers of entries are called [*bookies*](#bookies)

BookKeeper is designed to be reliable and resilient to a wide variety of failures. Bookies can crash, corrupt data, or discard data, but as long as there are enough bookies behaving correctly in the cluster the service as a whole will behave correctly.

## Entries

BookKeeper *entries* are sequences of bytes that are written to [ledgers](#ledgers). Each entry has the following fields:

Field | Java type | Description
:-----|:----------|:-----------
Ledger number | `long` | The ID of the ledger to which the entry has been written
Entry number | `long` | The unique ID of the entry
Last confirmed (LC) | `long` | The ID of the last recorded entry
Data | `byte[]` | The entry's data (written by the client application)
Authentication code | `byte[]` | The message auth code, which includes *all* other fields in the entry

> Entries contain the actual data written to ledgers, along with some important metadata.

## Ledgers

Ledgers are sequences of entries, while each entry is a sequence of bytes. Entries are written sequentially to a ledger and at most once. Consequently, ledgers have *append-only* semantics.

> Ledgers are the basic unit of storage in BookKeeper.

## Clients

BookKeeper clients execute operations on ledgers, such as creating and writing ledgers.

At a high level, a bookkeeper client receives entries from a client application and stores it to sets of bookies, and there are a few advantages in having such a service:

We can use hardware that is optimized for such a service. We currently believe that such a system has to be optimized only for disk I/O;
We can have a pool of servers implementing such a log system, and shared among a number of servers;
We can have a higher degree of replication with such a pool, which makes sense if the hardware necessary for it is cheaper compared to the one the application uses.

> BookKeeper clients create and write data to ledgers.

## Bookies

A bookie is an individual BookKeeper storage server. Bookies store the content of ledgers. For any given ledger L, we call an ensemble the group of bookies storing the content of L. For performance, we store on each bookie of an ensemble only a fragment of a ledger. That is, we stripe when writing entries to a ledger such that each entry is written to sub-group of bookies of the ensemble.

> Bookies are servers that handle ledgers.

### Motivation

The initial motivation for BookKeeper comes was the [NameNode](https://wiki.apache.org/hadoop/NameNode) in the [Hadoop Distributed File System](https://wiki.apache.org/hadoop/HDFS) (HDFS).

Namenodes log operations in a reliable fashion so that recovery is possible in the case of crashes. We have found the applications for BookKeeper extend far beyond HDFS, however. Essentially, any application that requires an append storage can replace their implementations with BookKeeper. BookKeeper has the advantage of writing efficiently, replicating for fault tolerance, and scaling throughput with the number of servers through striping.

## Metadata storage service

BookKeeper requires a metadata storage service to store information related to [ledgers](#ledgers) and available bookies. We currently use ZooKeeper for such a task.

## Data management in bookies

Bookies manage data in a [log-structured](https://en.wikipedia.org/wiki/Log-structured_file_system) way, which is implemented using three types of files: [journals](#journals), [entry logs](#entry-logs), and [index files](#index-files).

### Journals

A journal file contains the BookKeeper transaction logs. Before any update takes place, a bookie ensures that a transaction describing the update is written to non-volatile storage. A new journal file is created once the bookie starts or the older journal file reaches the journal file size threshold.

### Entry logs

An entry log file manages the written entries received from BookKeeper clients. Entries from different ledgers are aggregated and written sequentially, while their offsets are kept as pointers in LedgerCache for fast lookup. A new entry log file is created once the bookie starts or the older entry log file reaches the entry log size threshold. Old entry log files are removed by the Garbage Collector Thread once they are not associated with any active ledger.

#### Log device

### Index files

An index file is created for each ledger, which comprises a header and several fixed-length index pages, recording the offsets of data stored in entry log files.

Since updating index files would introduce random disk I/O, for performance reasons, index files are updated lazily by a Sync Thread running in the background. Before index pages are persisted to disk, they are gathered in LedgerCache for lookup.

### Ledger cache

A memory pool caches ledger index pages, which more efficiently manage disk head scheduling.

#### Ledger device

### Data flush

Ledger index pages are flushed to index files in the following two cases:

LedgerCache memory reaches its limit. There is no more space available to hold newer index pages. Dirty index pages will be evicted from LedgerCache and persisted to index files.
A background thread Sync Thread is responsible for flushing index pages from LedgerCache to index files periodically.
Besides flushing index pages, Sync Thread is responsible for rolling journal files in case that journal files use too much disk space.

The data flush flow in Sync Thread is as follows:

Records a LastLogMark in memory. The LastLogMark contains two parts: first one is txnLogId (file id of a journal) and the second one is txnLogPos (offset in a journal). The LastLogMark indicates that those entries before it have been persisted to both index and entry log files.
Flushes dirty index pages from LedgerCache to index file, and flushes entry log files to ensure all buffered entries in entry log files are persisted to disk.
Ideally, a bookie just needs to flush index pages and entry log files that contains entries before LastLogMark. There is no such information in LedgerCache and Entry Log mapping to journal files, though. Consequently, the thread flushes LedgerCache and Entry Log entirely here, and may flush entries after the LastLogMark. Flushing more is not a problem, though, just redundant.
Persists LastLogMark to disk, which means entries added before LastLogMark whose entry data and index page were also persisted to disk. It is the time to safely remove journal files created earlier than txnLogId.
If the bookie has crashed before persisting LastLogMark to disk, it still has journal files containing entries for which index pages may not have been persisted. Consequently, when this bookie restarts, it inspects journal files to restore those entries; data isn't lost.
Using the above data flush mechanism, it is safe for the Sync Thread to skip data flushing when the bookie shuts down. However, in Entry Logger, it uses BufferedChannel to write entries in batches and there might be data buffered in BufferedChannel upon a shut down. The bookie needs to ensure Entry Logger flushes its buffered data during shutting down. Otherwise, Entry Log files become corrupted with partial entries.

As described above, EntryLogger#flush is invoked in the following two cases:
* in Sync Thread : used to ensure entries added before LastLogMark are persisted to disk.
* in ShutDown : used to ensure its buffered data persisted to disk to avoid data corruption with partial entries.

### Data compaction

In bookie server, entries of different ledgers are interleaved in entry log files. A bookie server runs a Garbage Collector thread to delete un-associated entry log files to reclaim disk space. If a given entry log file contains entries from a ledger that has not been deleted, then the entry log file would never be removed and the occupied disk space never reclaimed. In order to avoid such a case, a bookie server compacts entry log files in Garbage Collector thread to reclaim disk space.

There are two kinds of compaction running with different frequency, which are Minor Compaction and Major Compaction. The differences of Minor Compaction and Major Compaction are just their threshold value and compaction interval.

Threshold : Size percentage of an entry log file occupied by those undeleted ledgers. Default minor compaction threshold is 0.2, while major compaction threshold is 0.8.
Interval : How long to run the compaction. Default minor compaction is 1 hour, while major compaction threshold is 1 day.
NOTE: if either Threshold or Interval is set to less than or equal to zero, then compaction is disabled.

The data compaction flow in Garbage Collector Thread is as follows:

Garbage Collector thread scans entry log files to get their entry log metadata, which records a list of ledgers comprising an entry log and their corresponding percentages.
With the normal garbage collection flow, once the bookie determines that a ledger has been deleted, the ledger will be removed from the entry log metadata and the size of the entry log reduced.
If the remaining size of an entry log file reaches a specified threshold, the entries of active ledgers in the entry log will be copied to a new entry log file.
Once all valid entries have been copied, the old entry log file is deleted.

## ZooKeeper metadata

BookKeeper requires a ZooKeeper installation for storing [ledger](#ledger) metadata. Whenever you construct a [`BookKeeper`](/api/org/apache/bookkeeper/client/BookKeeper) client object, you need to pass a list of ZooKeeper servers as a parameter to the constructor, like this:

```java
String zkConnectionString = "127.0.0.1:2181";
BookKeeper bkClient = new BookKeeper(zkConnectionString);
```

> For more info on using the BookKeeper Java client, see [this guide](../../applications/java-client).

## Ledger manager

A *ledger manager* handles ledgers' metadata (which is stored in ZooKeeper). BookKeeper offers two types of ledger managers: the [flat ledger manager](#flat-ledger-manager) and the [hierarchical ledger manager](#hierarchical-ledger-manager). Both ledger managers extend the [`AbstractZkLedgerManager`](/api/org/apache/bookkeeper/meta/AbstractZkLedgerManager) abstract class.

> #### Use the flat ledger manager in most cases
> The flat ledger manager is the default and is recommended for nearly all use cases. The hierarchical ledger manager is better suited only for managing very large numbers of BookKeeper ledgers (> 50,000).

### Flat ledger manager

The *flat ledger manager*, implemented in the [`FlatLedgerManager`](/api/org/apache/bookkeeper/meta/FlatLedgerManager.html) class, stores all ledgers' metadata in child nodes of a single ZooKeeper path. The flat ledger manager creates [sequential nodes](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#Sequence+Nodes+--+Unique+Naming) to ensure the uniqueness of the ledger ID and prefixes all nodes with `L`. Bookie servers manage their own active ledgers in a hash map so that it's easy to find which ledgers have been deleted from ZooKeeper and then garbage collect them.

The flat ledger manager's garbage collection follow proceeds as follows:

* All existing ledgers are fetched from ZooKeeper (`zkActiveLedgers`)
* All ledgers currently active within the bookie are fetched (`bkActiveLedgers`)
* The currently actively ledgers are looped through to determine which ledgers don't currently exist in ZooKeeper. Those are then garbage collected.
* The *hierarchical ledger manager* stores ledgers' metadata in two-level [znodes](https://zookeeper.apache.org/doc/current/zookeeperOver.html#Nodes+and+ephemeral+nodes).

### Hierarchical ledger manager

The *hierarchical ledger manager*, implemented in the [`HierarchicalLedgerManager`](/api/org/apache/bookkeeper/meta/HierarchicalLedgerManager) class, first obtains a global unique ID from ZooKeeper using an [`EPHEMERAL_SEQUENTIAL`](https://zookeeper.apache.org/doc/current/api/org/apache/zookeeper/CreateMode.html#EPHEMERAL_SEQUENTIAL) znode. Size ZooKeeper's sequence counter has a format of `%10d` (10 digits with 0 padding, for example `<path>0000000001`), the hierarchical ledger manager splits the generated ID into 3 parts:

```shell
{level1 (2 digits)}{level2 (4 digits)}{level3 (4 digits)}
```

These three parts are used to form the actual ledger node path to store ledger metadata:

```shell
{ledgers_root_path}/{level1}/{level2}/L{level3}
```

For example, ledger 0000000001 is split into three parts, 00, 0000, and 00001 and stored in znode `/{ledgers_root_path}/00/0000/L0001`. Each znode could have as many 10,000 ledgers, which avoids the problem of the child list being larger than the maximum ZooKeeper packet size (which is the [limitation](https://issues.apache.org/jira/browse/BOOKKEEPER-39) that initially prompted the creation of the hierarchical ledger manager).
