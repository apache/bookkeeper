# Apache BookKeeper docker-compose

## Requirements

* Docker >= 20.10
* Docker Compose >= 2.12.0

## Quick start

```bash
$ git clone https://github.com/apache/bookkeeper.git
$ cd bookkeeper/deploy/docker-compose
$ docker compose pull # Get the latest Docker images
$ docker compose up -d
$ cd ../../
$ bin/bkctl bookies list
$ bin/bkctl ledger simpletest
```

## Access Apache BookKeeper cluster

### Ledger Service

You can use `zk://localhost:2181/ledgers` as metadataServiceUri to access ledger storage service.

```bash
$ bin/bkctl -u 'zk://localhost:2181/ledgers' ledger simpletest 
```

### DistributedLog

You can use `distributedlog://localhost:2181/distributedlog` as dlog uri to access ledger storage service
using [distributedlog](https://bookkeeper.apache.org/docs/api/distributedlog-api/) API.

```bash
$ bin/dlog tool create -u 'distributedlog://localhost:2181/distributedlog' --prefix test-stream -e 0-99
```

## Customize Apache BookKeeper Cluster

### Install Helm

[Helm](https://helm.sh) is used as a template render engine.

See how to [install Helm](https://helm.sh/docs/intro/install/).

### Bring up Apache BookKeeper cluster

```bash
$ git clone https://github.com/apache/bookkeeper.git
$ cd bookkeeper/deploy/docker-compose
$ vi compose/values.yaml # custom cluster size, docker image, port mapping etc
$ helm template compose > generated-docker-compose.yaml
$ docker compose -f generated-docker-compose.yaml pull # Get the latest Docker images
$ docker compose -f generated-docker-compose.yaml up -d
```
