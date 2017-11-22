---
title: Using AutoRecovery
---

When a {% pop bookie %} crashes, all {% pop ledgers %} on that bookie become under-replicated. In order to bring all ledgers in your BookKeeper cluster back to full replication, you'll need to *recover* the data from any offline bookies. There are two ways to recover bookies' data:

1. Using [manual recovery](#manual-recovery)
1. Automatically, using [*AutoRecovery*](#autorecovery)

## Manual recovery

You can manually recover failed bookies using the [`bookkeeper`](../../reference/cli) command-line tool. You need to specify:

* the `shell recover` option 
* an IP and port for your BookKeeper cluster's ZooKeeper ensemble
* the IP and port for the failed bookie

Here's an example:

```bash
$ bookkeeper-server/bin/bookkeeper shell recover \
  zk1.example.com:2181 \ # IP and port for ZooKeeper
  192.168.1.10:3181      # IP and port for the failed bookie
```

If you wish, you can also specify which bookie you'd like to rereplicate to. Here's an example:

```bash
$ bookkeeper-server/bin/bookkeeper shell recover \
  zk1.example.com:2181 \ # IP and port for ZooKeeper
  192.168.1.10:3181 \    # IP and port for the failed bookie
  192.168.1.11:3181      # IP and port for the bookie to rereplicate to
```

### The manual recovery process

When you initiate a manual recovery process, the following happens:

1. The client (the process running ) reads the metadata of active ledgers from ZooKeeper.
1. The ledgers that contain fragments from the failed bookie in their ensemble are selected.
1. A recovery process is initiated for each ledger in this list and the rereplication process is run for each ledger.
1. Once all the ledgers are marked as fully replicated, bookie recovery is finished.

## AutoRecovery

AutoRecovery is a process that:

* automatically detects when a {% pop bookie %} in your BookKeeper cluster has become unavailable and then
* rereplicates all the {% pop ledgers %} that were stored on that bookie.

AutoRecovery can be run in two ways:

1. On dedicated nodes in your BookKeeper cluster
1. On the same machines on which your bookies are running

## Running AutoRecovery

You can start up AutoRecovery using the [`autorecovery`](../../reference/cli#bookkeeper-autorecovery) command of the [`bookkeeper`](../../reference/cli) CLI tool.

```bash
$ bookkeeper-server/bin/bookkeeper autorecovery
```

> The most important thing to ensure when starting up AutoRecovery is that the ZooKeeper connection string specified by the [`zkServers`](../../reference/config#zkServers) parameter points to the right ZooKeeper cluster.

If you start up AutoRecovery on a machine that is already running a bookie, then the AutoRecovery process will run alongside the bookie on a separate thread.

You can also start up AutoRecovery on a fresh machine if you'd like to create a dedicated cluster of AutoRecovery nodes.

## Configuration

There are a handful of AutoRecovery-related configs in the [`bk_server.conf`](../../reference/config) configuration file. For a listing of those configs, see [AutoRecovery settings](../../reference/config#autorecovery-settings).

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

## AutoRecovery architecture

AutoRecovery has two components:

1. The [**auditor**](#auditor) (see the [`Auditor`](../../api/javadoc/org/apache/bookkeeper/replication/Auditor.html) class) is a singleton node that watches bookies to see if they fail and creates rereplication tasks for the ledgers on failed bookies.
1. The [**replication worker**](#replication-worker) (see the [`ReplicationWorker`](../../api/javadoc/org/apache/bookkeeper/replication/ReplicationWorker.html) class) runs on each bookie and executes rereplication tasks provided by the auditor.

Both of these components run as threads in the [`AutoRecoveryMain`](../../api/javadoc/org/apache/bookkeeper/replication/AutoRecoveryMain) process, which runs on each bookie in the cluster. All recovery nodes participate in leader election---using ZooKeeper---to decide which node becomes the auditor. Nodes that fail to become the auditor watch the elected auditor and run an election process again if they see that the auditor node has failed.

### Auditor

The auditor watches all bookies in the cluster that are registered with ZooKeeper. Bookies register with ZooKeeper at startup. If the bookie crashes or is killed, the bookie's registration in ZooKeeper disappears and the auditor is notified of the change in the list of registered bookies.

When the auditor sees that a bookie has disappeared, it immediately scans the complete {% pop ledger %} list to find ledgers that have data stored on the failed bookie. Once it has a list of ledgers for that bookie, the auditor will publish a rereplication task for each ledger under the `/underreplicated/` [znode](https://zookeeper.apache.org/doc/current/zookeeperOver.html) in ZooKeeper.

### Replication Worker

Each replication worker watches for tasks being published by the auditor on the `/underreplicated/` znode in ZooKeeper. When a new task appears, the replication worker will try to get a lock on it. If it cannot acquire the lock, it will try the next entry. The locks are implemented using ZooKeeper ephemeral znodes.

The replication worker will scan through the rereplication task's ledger for fragments of which its local bookie is not a member. When it finds fragments matching this criterion, it will replicate the entries of that fragment to the local bookie. If, after this process, the ledger is fully replicated, the ledgers entry under /underreplicated/ is deleted, and the lock is released. If there is a problem replicating, or there are still fragments in the ledger which are still underreplicated (due to the local bookie already being part of the ensemble for the fragment), then the lock is simply released.

If the replication worker finds a fragment which needs rereplication, but does not have a defined endpoint (i.e. the final fragment of a ledger currently being written to), it will wait for a grace period before attempting rereplication. If the fragment needing rereplication still does not have a defined endpoint, the ledger is fenced and rereplication then takes place.

This avoids the situation in which a client is writing to a ledger and one of the bookies goes down, but the client has not written an entry to that bookie before rereplication takes place. The client could continue writing to the old fragment, even though the ensemble for the fragment had changed. This could lead to data loss. Fencing prevents this scenario from happening. In the normal case, the client will try to write to the failed bookie within the grace period, and will have started a new fragment before rereplication starts.

You can configure this grace period using the [`openLedgerRereplicationGracePeriod`](../../reference/config#openLedgerRereplicationGracePeriod) parameter.

### The rereplication process

The ledger rereplication process happens in these steps:

1. The client goes through all ledger fragments in the ledger, selecting those that contain the failed bookie.
1. A recovery process is initiated for each ledger fragment in this list.
   1. The client selects a bookie to which all entries in the ledger fragment will be replicated; In the case of autorecovery, this will always be the local bookie.
   1. The client reads entries that belong to the ledger fragment from other bookies in the ensemble and writes them to the selected bookie.
   1. Once all entries have been replicated, the zookeeper metadata for the fragment is updated to reflect the new ensemble.
   1. The fragment is marked as fully replicated in the recovery tool.
1. Once all ledger fragments are marked as fully replicated, the ledger is marked as fully replicated.
  
