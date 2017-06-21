---
title: BookKeeper administration
---

This document is a guide to deploying, administering, and maintaining BookKeeper. It also discusses [best practices](#best-practices) and [common problems](#common-problems).

## Requirements

A typically BookKeeper installation comprises a set of {% pop bookies %} and a ZooKeeper quorum. The exact number of bookies depends on the quorum mode that you choose, desired throughput, and the number of clients using the installation simultaneously.

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
$ bookkeeper-server/bin/bookkeeper bookie
```

To start a bookie in the background, use the [`bookkeeper-daemon.sh`](../../reference/cli#bookkeeper-daemon.sh) script and run `start bookie`:

```shell
$ bookkeeper-server/bin/bookkeeper-daemon.sh start bookie
```

### Local bookies

The instructions above showed you how to run bookies intended for production use. If you'd like to experiment with ensembles of bookies locally, you can use the [`localbookie`](../../reference/cli#bookkeeper-localbookie) command of the `bookkeeper` CLI tool and specify the number of bookies you'd like to run.

This would spin up a local ensemble of 6 bookies:

```shell
$ bookkeeper-server/bin/bookkeeper localbookie 6
```

> When you run a local bookie ensemble, all bookies run in a single JVM process.

## Configuring bookies

[Configuration](../../reference/config)
