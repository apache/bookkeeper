---
title: Deploying BookKeeper on DC/OS
subtitle: Get up and running easily on an Apache Mesos cluster
logo: img/dcos-logo.png
---

[DC/OS](https://dcos.io/) (the <strong>D</strong>ata<strong>C</strong>enter <strong>O</strong>perating <strong>S</strong>ystem) is a distributed operating system used for deploying and managing applications and systems on [Apache Mesos](http://mesos.apache.org/). DC/OS is an open-source tool created and maintained by [Mesosphere](https://mesosphere.com/).

BookKeeper is available as a [DC/OS package](http://universe.dcos.io/#/package/bookkeeper/version/latest) from the [Mesosphere DC/OS Universe](http://universe.dcos.io/#/packages).

## Prerequisites

In order to run BookKeeper on DC/OS, you will need:

* DC/OS version [1.8](https://dcos.io/docs/1.8/) or higher
* A DC/OS cluster with at least three nodes
* The [DC/OS CLI tool](https://dcos.io/docs/1.8/usage/cli/install/) installed

Each node in the cluster must have at least:

* 1 CPU
* 1 GB of memory
* 10 GB of total persistent disk storage

## Installing BookKeeper

```shell
$ dcos package install bookkeeper --yes
```

This command will:

* Install the `bookkeeper` subcommand for the `dcos` CLI tool
* Start a single {% pop bookie %} on the Mesos cluster with the [default configuration](../../reference/config)

The bookie that is automatically started up uses the host mode of the network and by default exports the service at `agent_ip:3181`.

### Services

To watch BookKeeper start up, click on the **Services** tab in the DC/OS [user interface](https://docs.mesosphere.com/latest/gui/).

![DC/OS services]({{ site.baseurl }}img/dcos/services.png)

### Tasks

To see which tasks have started, click on the BookKeeper service.

![DC/OS tasks]({{ site.baseurl }}img/dcos/tasks.png)

### Scale

Once the first {% pop bookie %} has started up, you can click on the **Scale** tab to scale up your BookKeeper ensemble by adding more bookies (or removing bookies to scale the ensemble down).

![DC/OS scale]({{ site.baseurl }}img/dcos/scale.png)

### ZooKeeper Exhibitor

ZooKeeper contains the information for all bookies in the ensemble. When deployed on DC/OS, BookKeeper uses a ZooKeeper instance provided by DC/OS. You can access a visual UI for ZooKeeper using [Exhibitor](https://github.com/soabase/exhibitor/wiki), which is available at [http://master.dcos/exhibitor](http://master.dcos/exhibitor).

![ZooKeeper Exhibitor]({{ site.baseurl }}img/dcos/exhibitor.png)

You should see a listing of IP/host information for all bookies under `messaging/bookkeeper/ledgers/available`.

## Client connections

To connect to bookies running on DC/OS using clients, you need to specify the ZooKeeper connection string for DC/OS's ZooKeeper cluster:

```
master.mesos:2181
```

This is the *only* ZooKeeper host/port you need to include in your connection string.

Here's an example using the [Java client](../../api/ledger-api#the-java-ledger-api-client):

```java
BookKeeper bkClient = new BookKeeper("master.mesos:2181");
```

## Configuring BookKeeper

You can configure BookKeeper for DC/OS using a JSON file.

```shell
$ dcos package install bookkeeper \
  --config=my-bk-dcos-config.json
```

> For a full listing of parameters that you can set for BookKeeper, see the [Configuration](../../reference/config) reference doc.

You can then fetch the current configuration for BookKeeper at any time using the `package describe` command:

```shell
$ dcos package describe bookkeeper \
  --config
```

## Uninstalling BookKeeper

You can shut down and uninstall the `bookkeeper` from DC/OS at any time using the `package uninstall` command:

```shell
$ dcos package uninstall bookkeeper
Uninstalled package [bookkeeper] version [{{ site.bk_version }}]
Thank you for using bookkeeper.
```
