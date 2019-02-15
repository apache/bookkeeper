---
title: BookKeeper administration
subtitle: A guide to deploying and administering BookKeeper
---

This document is a guide to deploying, administering, and maintaining BookKeeper. It also discusses [best practices](#best-practices) and [common problems](#common-problems).

## Requirements

A typical BookKeeper installation consists of an ensemble of {% pop bookies %} and a ZooKeeper quorum. The exact number of bookies depends on the quorum mode that you choose, desired throughput, and the number of clients using the installation simultaneously.

The minimum number of bookies depends on the type of installation:

* For *self-verifying* entries you should run at least three bookies. In this mode, clients store a message authentication code along with each {% pop entry %}.
* For *generic* entries you should run at least four

There is no upper limit on the number of bookies that you can run in a single ensemble.

### Performance

To achieve optimal performance, BookKeeper requires each server to have at least two disks. It's possible to run a bookie with a single disk but performance will be significantly degraded.

### ZooKeeper

There is no constraint on the number of ZooKeeper nodes you can run with BookKeeper. A single machine running ZooKeeper in [standalone mode](https://zookeeper.apache.org/doc/current/zookeeperStarted.html#sc_InstallingSingleMode) is sufficient for BookKeeper, although for the sake of higher resilience we recommend running ZooKeeper in [quorum mode](https://zookeeper.apache.org/doc/current/zookeeperStarted.html#sc_RunningReplicatedZooKeeper) with multiple servers.

## Starting and stopping bookies

You can run bookies either in the foreground or in the background, using [nohup](https://en.wikipedia.org/wiki/Nohup). You can also run [local bookies](#local-bookie) for development purposes.

To start a bookie in the foreground, use the [`bookie`](../../reference/cli#bookkeeper-bookie) command of the [`bookkeeper`](../../reference/cli#bookkeeper) CLI tool:

```shell
$ bin/bookkeeper bookie
```

To start a bookie in the background, use the [`bookkeeper-daemon.sh`](../../reference/cli#bookkeeper-daemon.sh) script and run `start bookie`:

```shell
$ bin/bookkeeper-daemon.sh start bookie
```

### Local bookies

The instructions above showed you how to run bookies intended for production use. If you'd like to experiment with ensembles of bookies locally, you can use the [`localbookie`](../../reference/cli#bookkeeper-localbookie) command of the `bookkeeper` CLI tool and specify the number of bookies you'd like to run.

This would spin up a local ensemble of 6 bookies:

```shell
$ bin/bookkeeper localbookie 6
```

> When you run a local bookie ensemble, all bookies run in a single JVM process.

## Configuring bookies

There's a wide variety of parameters that you can set in the bookie configuration file in `bookkeeper-server/conf/bk_server.conf` of your [BookKeeper installation](../../reference/config). A full listing can be found in [Bookie configuration](../../reference/config).

Some of the more important parameters to be aware of:

Parameter | Description | Default
:---------|:------------|:-------
`bookiePort` | The TCP port that the bookie listens on | `3181`
`zkServers` | A comma-separated list of ZooKeeper servers in `hostname:port` format | `localhost:2181`
`journalDirectory` | The directory where the [log device](../../getting-started/concepts#log-device) stores the bookie's write-ahead log (WAL) | `/tmp/bk-txn`
`ledgerDirectories` | The directories where the [ledger device](../../getting-started/concepts#ledger-device) stores the bookie's ledger entries (as a comma-separated list) | `/tmp/bk-data`

> Ideally, the directories specified `journalDirectory` and `ledgerDirectories` should be on difference devices.

## Logging

BookKeeper uses [slf4j](http://www.slf4j.org/) for logging, with [log4j](https://logging.apache.org/log4j/2.x/) bindings enabled by default.

To enable logging for a bookie, create a `log4j.properties` file and point the `BOOKIE_LOG_CONF` environment variable to the configuration file. Here's an example:

```shell
$ export BOOKIE_LOG_CONF=/some/path/log4j.properties
$ bin/bookkeeper bookie
```

## Upgrading

From time to time you may need to make changes to the filesystem layout of bookies---changes that are incompatible with previous versions of BookKeeper and require that directories used with previous versions are upgraded. If a filesystem upgrade is required when updating BookKeeper, the bookie will fail to start and return an error like this:

```
2017-05-25 10:41:50,494 - ERROR - [main:Bookie@246] - Directory layout version is less than 3, upgrade needed
```

BookKeeper provides a utility for upgrading the filesystem. You can perform an upgrade using the [`upgrade`](../../reference/cli#bookkeeper-upgrade) command of the `bookkeeper` CLI tool. When running `bookkeeper upgrade` you need to specify one of three flags:

Flag | Action
:----|:------
`--upgrade` | Performs an upgrade
`--rollback` | Performs a rollback to the initial filesystem version
`--finalize` | Marks the upgrade as complete

### Upgrade pattern

A standard upgrade pattern is to run an upgrade...

```shell
$ bin/bookkeeper upgrade --upgrade
```

...then check that everything is working normally, then kill the bookie. If everything is okay, finalize the upgrade...

```shell
$ bin/bookkeeper upgrade --finalize
```

...and then restart the server:

```shell
$ bin/bookkeeper bookie
```

If something has gone wrong, you can always perform a rollback:

```shell
$ bin/bookkeeper upgrade --rollback
```

## Formatting

You can format bookie metadata in ZooKeeper using the [`metaformat`](../../reference/cli#bookkeeper-shell-metaformat) command of the [BookKeeper shell](../../reference/cli#the-bookkeeper-shell).

By default, formatting is done in interactive mode, which prompts you to confirm the format operation if old data exists. You can disable confirmation using the `-nonInteractive` flag. If old data does exist, the format operation will abort *unless* you set the `-force` flag. Here's an example:

```shell
$ bin/bookkeeper shell metaformat
```

You can format the local filesystem data on a bookie using the [`bookieformat`](../../reference/cli#bookkeeper-shell-bookieformat) command on each bookie. Here's an example:

```shell
$ bin/bookkeeper shell bookieformat
```

> The `-force` and `-nonInteractive` flags are also available for the `bookieformat` command.

## AutoRecovery

For a guide to AutoRecovery in BookKeeper, see [this doc](../autorecovery).

## Missing disks or directories

Accidentally replacing disks or removing directories can cause a bookie to fail while trying to read a ledger fragment that, according to the ledger metadata, exists on the bookie. For this reason, when a bookie is started for the first time, its disk configuration is fixed for the lifetime of that bookie. Any change to its disk configuration, such as a crashed disk or an accidental configuration change, will result in the bookie being unable to start. That will throw an error like this:

```
2017-05-29 18:19:13,790 - ERROR - [main:BookieServer314] – Exception running bookie server : @
org.apache.bookkeeper.bookie.BookieException$InvalidCookieException
.......at org.apache.bookkeeper.bookie.Cookie.verify(Cookie.java:82)
.......at org.apache.bookkeeper.bookie.Bookie.checkEnvironment(Bookie.java:275)
.......at org.apache.bookkeeper.bookie.Bookie.<init>(Bookie.java:351)
```

If the change was the result of an accidental configuration change, the change can be reverted and the bookie can be restarted. However, if the change *cannot* be reverted, such as is the case when you want to add a new disk or replace a disk, the bookie must be wiped and then all its data re-replicated onto it.

1. Increment the [`bookiePort`](../../reference/config#bookiePort) parameter in the [`bk_server.conf`](../../reference/config)
1. Ensure that all directories specified by [`journalDirectory`](../../reference/config#journalDirectory) and [`ledgerDirectories`](../../reference/config#ledgerDirectories) are empty.
1. [Start the bookie](#starting-and-stopping-bookies).
1. Run the following command to re-replicate the data:

   ```bash
   $ bin/bookkeeper shell recover \
     <zkserver> \
     <oldbookie> \
     <newbookie>
   ```

   The ZooKeeper server, old bookie, and new bookie, are all identified by their external IP and `bookiePort` (3181 by default). Here's an example:

   ```bash
   $ bin/bookkeeper shell recover \
     zk1.example.com \
     192.168.1.10:3181 \
     192.168.1.10:3181
   ```

   See the [AutoRecovery](../autorecovery) documentation for more info on the re-replication process.
