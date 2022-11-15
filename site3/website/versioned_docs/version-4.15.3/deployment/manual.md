---
id: manual
title: Manual deployment
---

A BookKeeper cluster consists of two main components:

* A [ZooKeeper](#zookeeper-setup) cluster that is used for configuration- and coordination-related tasks
* An [ensemble](#starting-up-bookies) of bookies

## ZooKeeper setup

We won't provide a full guide to setting up a ZooKeeper cluster here. We recommend that you consult [this guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html) in the official ZooKeeper documentation.

## Cluster metadata setup

Once your ZooKeeper cluster is up and running, there is some metadata that needs to be written to ZooKeeper, so you need to modify the bookie's configuration to make sure that it points to the right ZooKeeper cluster.

On each bookie host, you need to [download](../getting-started/installation#download) the BookKeeper package as a tarball. Once you've done that, you need to configure the bookie by setting values in the `bookkeeper-server/conf/bk_server.conf` config file. The one parameter that you will absolutely need to change is the `zkServers` parameter, which you will need to set to the ZooKeeper connection string for your ZooKeeper cluster. Here's an example:

```properties
zkServers=100.0.0.1:2181,100.0.0.2:2181,100.0.0.3:2181
```

> A full listing of configurable parameters available in `bookkeeper-server/conf/bk_server.conf` can be found in the [Configuration](../reference/config) reference manual.

Once the bookie's configuration is set, you can set up cluster metadata for the cluster by running the following command from any bookie in the cluster:

```shell
$ bookkeeper-server/bin/bookkeeper shell metaformat
```

You can run in the formatting 

> The `metaformat` command performs all the necessary ZooKeeper cluster metadata tasks and thus only needs to be run *once* and from *any* bookie in the BookKeeper cluster.

Once cluster metadata formatting has been completed, your BookKeeper cluster is ready to go!

## Starting up bookies

  

Before you start up your bookies, you should make sure that all bookie hosts have the correct configuration, then you can start up as many bookies as you'd like to form a cluster by using the [`bookie`](../reference/cli#bookkeeper-bookie) command of the [`bookkeeper`](../reference/cli#bookkeeper) CLI tool:

```shell
$ bookkeeper-server/bin/bookkeeper bookie
```


### System requirements

The number of bookies you should run in a BookKeeper cluster depends on the quorum mode that you've chosen, the desired throughput, and the number of clients using the cluster simultaneously.

Quorum type | Number of bookies
:-----------|:-----------------
Self-verifying quorum | 3
Generic | 4

Increasing the number of bookies will enable higher throughput, and there is **no upper limit** on the number of bookies. 

<!--
## AutoRecovery

[this guide](../admin/autorecovery)
-->
