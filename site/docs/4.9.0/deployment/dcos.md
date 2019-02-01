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

Each node in your DC/OS-managed Mesos cluster must have at least:

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

> If you run `dcos package install bookkeeper` without setting the `--yes` flag, the install will run in interactive mode. For more information on the `package install` command, see the [DC/OS docs](https://docs.mesosphere.com/latest/cli/command-reference/dcos-package/dcos-package-install/).

### Services

To watch BookKeeper start up, click on the **Services** tab in the DC/OS [user interface](https://docs.mesosphere.com/latest/gui/) and you should see the `bookkeeper` package listed:

![DC/OS services]({{ site.baseurl }}img/dcos/services.png)

### Tasks

To see which tasks have started, click on the `bookkeeper` service and you'll see an interface that looks like this;

![DC/OS tasks]({{ site.baseurl }}img/dcos/tasks.png)

## Scaling BookKeeper

Once the first {% pop bookie %} has started up, you can click on the **Scale** tab to scale up your BookKeeper ensemble by adding more bookies (or scale down the ensemble by removing bookies).

![DC/OS scale]({{ site.baseurl }}img/dcos/scale.png)

## ZooKeeper Exhibitor

ZooKeeper contains the information for all bookies in the ensemble. When deployed on DC/OS, BookKeeper uses a ZooKeeper instance provided by DC/OS. You can access a visual UI for ZooKeeper using [Exhibitor](https://github.com/soabase/exhibitor/wiki), which is available at [http://master.dcos/exhibitor](http://master.dcos/exhibitor).

![ZooKeeper Exhibitor]({{ site.baseurl }}img/dcos/exhibitor.png)

You should see a listing of IP/host information for all bookies under the `messaging/bookkeeper/ledgers/available` node.

## Client connections

To connect to bookies running on DC/OS using clients running within your Mesos cluster, you need to specify the ZooKeeper connection string for DC/OS's ZooKeeper cluster:

```
master.mesos:2181
```

This is the *only* ZooKeeper host/port you need to include in your connection string. Here's an example using the [Java client](../../api/ledger-api#the-java-ledger-api-client):

```java
BookKeeper bkClient = new BookKeeper("master.mesos:2181");
```

If you're connecting using a client running outside your Mesos cluster, you need to supply the public-facing connection string for your DC/OS ZooKeeper cluster.

## Configuring BookKeeper

By default, the `bookkeeper` package will start up a BookKeeper ensemble consisting of one {% pop bookie %} with one CPU, 1 GB of memory, and a 70 MB persistent volume.

You can supply a non-default configuration when installing the package using a JSON file. Here's an example command:

```shell
$ dcos package install bookkeeper \
  --options=/path/to/config.json
```

You can then fetch the current configuration for BookKeeper at any time using the `package describe` command:

```shell
$ dcos package describe bookkeeper \
  --config
```

### Available parameters

> Not all [configurable parameters](../../reference/config) for BookKeeper are available for BookKeeper on DC/OS. Only the parameters show in the table below are available.

Param | Type | Description | Default
:-----|:-----|:------------|:-------
`name` | String | The name of the DC/OS service. | `bookkeeper`
`cpus` | Integer | The number of CPU shares to allocate to each {% pop bookie %}. The minimum is 1. | `1` |
`instances` | Integer | The number of {% pop bookies %} top run. The minimum is 1. | `1`
`mem` | Number | The memory, in MB, to allocate to each BookKeeper task | `1024.0` (1 GB)
`volume_size` | Number | The persistent volume size, in MB | `70`
`zk_client` | String | The connection string for the ZooKeeper client instance | `master.mesos:2181`
`service_port` | Integer | The BookKeeper export service port, using `PORT0` in Marathon | `3181`

### Example JSON configuration

Here's an example JSON configuration object for BookKeeper on DC/OS:

```json
{
  "instances": 5,
  "cpus": 3,
  "mem": 2048.0,
  "volume_size": 250
}
```

If that configuration were stored in a file called `bk-config.json`, you could apply that configuration upon installating the BookKeeper package using this command:

```shell
$ dcos package install bookkeeper \
  --options=./bk-config.json
```

## Uninstalling BookKeeper

You can shut down and uninstall the `bookkeeper` from DC/OS at any time using the `package uninstall` command:

```shell
$ dcos package uninstall bookkeeper
Uninstalled package [bookkeeper] version [4.9.0]
Thank you for using bookkeeper.
```
