---
title: Using AutoRecovery
---

When a {% pop bookie %} crashes, all {% pop ledgers %} on that bookie become under-replicated. In order to bring all ledgers in your BookKeeper cluster back to full replication, you'll need to *recover* the data from any offline bookies. There are two ways to recover bookies' data:

1. Using [manual recovery](#manual-recovery)
1. Automatically, using [*AutoRecovery*](#autorecovery)

## Manual recovery

You can manually recover failed bookies using the [`bookkeeper`](../../reference/cli) command-line tool. You need to specify:

* that the `org.apache.bookkeeper.tools.BookKeeperTools` class needs to be run
* an IP and port for your BookKeeper cluster's ZooKeeper ensemble
* the IP and port for the failed bookie

Here's an example:

```bash
$ bookkeeper-server/bin/bookkeeper org.apache.bookkeeper.tools.BookKeeperTools \
  zk1.example.com:2181 \ # IP and port for ZooKeeper
  192.168.1.10:3181      # IP and port for the failed bookie
```

If you wish, you can also specify which bookie you'd like to rereplicate to. Here's an example:

```bash
$ bookkeeper-server/bin/bookkeeper org.apache.bookkeeper.tools.BookKeeperTools \
  zk1.example.com:2181 \ # IP and port for ZooKeeper
  192.168.1.10:3181 \    # IP and port for the failed bookie
  192.168.1.11:3181      # IP and port for the bookie to rereplicate to
```

## AutoRecovery

AutoRecovery is a process that:

* detects when a {% pop bookie %} in your BookKeeper cluster has become unavailable, and
* rereplicates all the {% pop ledgers %} that were stored on that bookie.

AutoRecovery can be run in two ways:

1. On dedicated nodes in your BookKeeper cluster
1. 

## Running AutoRecovery nodes

## Disable AutoRecovery

You can disable AutoRecovery at any time, for example during maintenance. Disabling AutoRecovery ensures that bookies' data isn't unnecessarily rereplicated when the bookie is only taken down for a short period of time, for example when the bookie is being updated or the configuration if being changed.

You can disable AutoRecover using the [`bookkeeper`](../../reference/cli#bookkeeper-shell-autorecovery) CLI tool:

```bash
$ bookkeeper-server/bin/bookkeeper shell autorecovery -disable
```

Once disabled, you can reenable AutoRecovery using the [`enable`](../../reference/cli#bookkeeper-shell-autorecovery) shell command:

```bash
$ bookkeeper-server/bin/bookkeeper shell autorecovery -enable
```

## Configuration

There are a handful of AutoRecovery-related configs in the [`bk_server.conf`](../../reference/config) configuration file. For a listing of those configs, see [AutoRecovery settings](../../reference/config#autorecovery-settings).

## AutoRecovery architecture

AutoRecovery has two components:

1. The [**auditor**](#auditor) (see the [`Auditor`]({{ site.baseurl }}javadoc/org/apache/bookkeeper/replication/Auditor.html) class) is a singleton node that watches bookies to see if they fail and creates rereplication tasks for the ledgers on failed bookies.
1. The [**replication worker**](#replication-worker) (see the [`ReplicationWorker`]({{ site.baseurl }}javadoc/org/apache/bookkeeper/replication/ReplicationWorker.html) class) runs on each bookie and executes rereplication tasks provided by the auditor.

Both of these components run as threads in the [`AutoRecoveryMain`]({{ site.baseurl }}javadoc/org/apache/bookkeeper/replication/AutoRecoveryMain) process, which runs on each bookie in the cluster. All recovery nodes participate in leader election---using ZooKeeper---to decide which node becomes the auditor. Nodes that fail to become the auditor watch the elected auditor and run an election process again if they see that the auditor node has failed.

### Auditor

The auditor watches all bookies in the cluster that are registered with ZooKeeper. Bookies register with ZooKeeper at startup. If the bookie crashes or is killed, the bookie's registration in ZooKeeper disappears and the auditor is notified of the change in the list of registered bookies.

When the auditor sees that a bookie has disappeared, it immediately scans the complete {% pop ledger %} list to find ledgers that have data stored on the failed bookie. Once it has a list of ledgers for that bookie, the auditor will publish a rereplication task for each ledger under the `/underreplicated/` [znode](https://zookeeper.apache.org/doc/current/zookeeperOver.html) in ZooKeeper.

### Replication Worker

Each replication worker in a BookKeeper cluster watches for tasks