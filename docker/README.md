
# What is Apache Bookkeeper?

Apache ZooKeeper is a software project of the Apache Software Foundation, providing a replicated log service which can be used to build replicated state machines. A log contains a sequence of events which can be applied to a state machine. BookKeeper guarantees that each replica state machine will see all the same entries, in the same order.

> [Apache Bookkeeper](http://bookkeeper.apache.org/)


# How to use this image

Bookkeeper needs [Zookeeper](https://zookeeper.apache.org/) in order to preserve its state and publish its bookies (bookkepeer servers). The client only need to connect to a Zookkeeper server in the ensamble in order to obtain the list of Bookkeeper servers.

## TL;DR

If you just want to see things working, you can play with Makefile hosted in this project and check its targets for a fairly complex set up example:
```
git clone https://github.com/apache/bookkeeper
cd bookkeeper/docker
make run-demo
```
While, if you don't have access to a X environment, e.g. on default MacOS, It has to run the last command manually in 6 terminals respectively.
```
make run-zk
make run-bk BOOKIE=1
make run-bk BOOKIE=2
make run-bk BOOKIE=3
make run-dice
make run-dice
```
This will do all the following steps and start up a working ensemble with two dice applications.

## Step by step

The simplest way to let Bookkeeper servers publish themselves with a name, which could be resolved consistently across container runs, is through creation of a [docker network](https://docs.docker.com/engine/reference/commandline/network_create/):
```
docker network create "my-bookkeeper-network"
```
Then we can start a Zookeeper (from [Zookeeper official image](https://hub.docker.com/_/zookeeper/)) server in standalone mode on that network:
```
docker run -d \
    --network "my-bookkeeper-network" \
    --name "my-zookeeper" \
    --hostname "my-zookeeper" \
    zookeeper
```
And initialize the metadata store that bookies will use to store information:
```
docker run -it --rm \
    --network "my-bookkeeper-network" \
    --env ZK_URL=my-zookeeper:2181 \
    bookkeeper \
    bookkeeper shell metaformat
```
Now we can start our Bookkeeper ensemble (e.g. with three bookies):
```
docker run -it\
    --network "my-bookkeeper-network" \
    --env ZK_URL=my-zookeeper:2181 \
    --name "bookie1" \
    --hostname "bookie1" \
    bookkeeper
```
And so on for "bookie2" and "bookie3". We have now our fully functional ensemble, ready to accept clients.

In order to play with our freshly created ensemble, you can use the simple application taken from [Bookkeeper Tutorial](http://bookkeeper.apache.org/docs/master/bookkeeperTutorial.html) and packaged in a [docker image](https://github.com/caiok/bookkeeper-tutorial) for convenience.

This application check if it can be leader, if yes start to roll a dice and book this rolls on bookkeeper, otherwise it will start to follow the leader rolls. If leader stops, follower will try to become leader and so on.

Start a dice application (you can run it several times to view the behavior in a concurrent environment):
```
docker run -it --rm \
    --network "my-bookkeeper-network" \
    --env ZK_URL=my-zookkeeper:2181 \
    caiok/bookkeeper-tutorial
```

## Configuration

Bookkeeper configuration is located in `/opt/bookkeeper/conf` in the docker container, it is a copy of [these files](https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/conf) in bookkeeper repo.

There are 2 ways to set bookkeeper configuration:

1, Apply setted (e.g. docker -e kk=vv) environment variables into configuration files. Environment variable names is in format "BK_originalName", in which "originalName" is the key in config files.

2, If you are able to handle your local volumes, use `docker --volume` command to bind-mount your local configure volumes to `/opt/bookkeeper/conf`.

Example showing how to use your own configuration files:
```
$ docker run --name bookie1 -d \
    -v $(local_configure_dir):/opt/bookkeeper/conf/ \   < == use 2nd approach, mount dir contains config_files
    -e BK_bookiePort=3181 \                             < == use 1st approach, set bookiePort
    -e BK_zkServers=zk-server1:2181,zk-server2:2181 \   < == use 1st approach, set zookeeper servers
    -e BK_journalPreAllocSizeMB=32 \                    < == use 1st approach, set journalPreAllocSizeMB in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf)
    bookkeeper
```

### Override rules for bookkeeper configuration
If you have applied several ways to set the same config target, e.g. the environment variable names contained in [these files](https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/conf) and conf_file in /opt/bookkeeper/conf/.

Then the override rules is as this:

Environment variable names contained in [these files](https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/conf), e.g. `zkServers`

    Override

Values in /opt/bookkeeper/conf/conf_files.

Take above example, if in docker instance you have bind-mount your config file as /opt/bookkeeper/conf/bk_server.conf, and in it contains key-value pair: `zkServers=zk-server3:2181`, then the value that take effect finally is `zkServers=zk-server1:2181,zk-server2:2181`

Because

`-e BK_zkServers=zk-server1:2181,zk-server2:2181` will override key-value pair: `zkServers=zk-server3:2181`, which contained in /opt/bookkeeper/conf/bk_server.conf.


### Environment variable names that mostly used for your configuration.

#### `BK_bookiePort`

This variable allows you to specify the port on which Bookkeeper should listen for incoming connections.

This will override `bookiePort` in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf).

Default value is "3181".

#### `BK_zkServers`

This variable allows you to specify a list of machines of the Zookeeper ensemble. Each entry has the form of `host:port`. Entries are separated with a comma.

This will override `zkServers` in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf).

Default value is "127.0.0.1:2181"

#### `BK_zkLedgersRootPath`

This variable allows you to specify the root directory bookkeeper will use on Zookeeper to store ledgers metadata.

This will override `zkLedgersRootPath ` in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf).

Default value is "/bookkeeper/ledgers"

#### `BK_CLUSTER_ROOT_PATH`

This variable allows you to specify the root directory bookkeeper will use on Zookeeper.

Default value is empty - " ". so ledgers dir in zookeeper will be at "/ledgers" by default. You could set it as that you want, e.g. "/bookkeeper"

#### `BK_DATA_DIR`
This variable allows you to specify where to store data in docker instance.

This could be override by env vars "BK_journalDirectory", "BK_ledgerDirectories", "BK_indexDirectories"  and also `journalDirectory`, `ledgerDirectories`, `indexDirectories` in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf).

Default value is "/data/bookkeeper", which contains volumes `/data/bookkeeper/journal`, `/data/bookkeeper/ledger` and `/data/bookkeeper/index` to hold Bookkeeper data in docker.


### Configure files under /opt/bookkeeper/conf
These files is originally un-tared from the bookkeeper building binary, such as [bookkeeper-server-4.4.0-bin.tar.tgz](https://archive.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-4.4.0-src.tar.gz), and it comes from [these files](https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/conf) in bookkeeper repo.

Usually we could config files bk_server.conf, bkenv.sh, log4j.properties, and log4j.shell.properties. Please read and understand them before you do the configuration.


### Caveats

Be careful where you put the transaction log (journal). A dedicated transaction log device is key to consistent good performance. Putting the log on a busy device will adversely effect performance.

Here is some useful and graceful command the could be used to replace the default command, once you want to delete the cookeis and do auto recovery:
```
/bookkeeper/bookkeeper-server/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie
/bookkeeper/bookkeeper-server/bin/bookkeeper autorecovery
```
Use them, and replace the default [CMD] when you wanted to do things other than start a bookie.

# License

View [license information](https://github.com/apache/bookkeeper/blob/master/LICENSE) for the software contained in this image.
