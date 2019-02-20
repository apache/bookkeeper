
# What is Apache Bookkeeper?

Apache Bookkeeper is a software project of the Apache Software Foundation, providing a replicated log service which can be used to build replicated state machines. A log contains a sequence of events which can be applied to a state machine. BookKeeper guarantees that each replica state machine will see all the same entries, in the same order.

> [Apache Bookkeeper](http://bookkeeper.apache.org/)


# How to use this image

Bookkeeper needs [Zookeeper](https://zookeeper.apache.org/) in order to preserve its state and publish its bookies (Bookkeeper servers). The client only need to connect to a Zookeeper server in the ensamble in order to obtain the list of Bookkeeper servers.
## standalone BookKeeper cluster
Just like running a BookKeeper cluster in one machine(http://bookkeeper.apache.org/docs/latest/getting-started/run-locally/), you can run a standalone BookKeeper in one docker container, the command is:
```
docker run -it \
     --env JAVA_HOME=/usr/lib/jvm/jre-1.8.0 \
     --entrypoint "/bin/bash" \
     apache/bookkeeper \
     -c "/opt/bookkeeper/bin/bookkeeper localbookie 3"
```
Note: you can first start the container, and then execute "bin/bookkeeper localbookie 3" in the container.  
After that, you can execute BookKeeper shell command(http://bookkeeper.apache.org/docs/latest/reference/cli/) to test the cluster, you need first log into the container, use command below:
```
docker exec -it <container id or name> bash
```
then run test command, such as:
```
 ./bin/bookkeeper shell listbookies -rw
 
 ./bin/bookkeeper shell simpletest
 
```
## TL;DR -- BookKeeper cluster 

If you want to setup cluster, you can play with Makefile or docker-compose hosted in this project and check its targets for a fairly complex set up example:

### Docker compose

```
git clone https://github.com/apache/bookkeeper
cd bookkeeper/docker
docker-compose up -d
```

and it spawns and ensemble of 3 bookies with 1 dice:

```
bookie1_1    | 2017-12-08 23:18:11,315 - INFO  -
[bookie-io-1:BookieRequestHandler@51] - Channel connected  [id: 0x405d690e,
L:/172.19.0.3:3181 - R:/172.19.0.6:34922]
bookie2_1    | 2017-12-08 23:18:11,326 - INFO  -
[bookie-io-1:BookieRequestHandler@51] - Channel connected  [id: 0x7fa8645d,
L:/172.19.0.4:3181 - R:/172.19.0.6:38862]
dice_1       | Value = 1, epoch = 5, leading
dice_1       | Value = 2, epoch = 5, leading
dice_1       | Value = 4, epoch = 5, leading
dice_1       | Value = 3, epoch = 5, leading
```

If you want to see how it behaves with more dices you only need to use
`docker-compose up -d --scale dice=3`:

```sh
dice_3       | Value = 3, epoch = 5, following
dice_2       | Value = 3, epoch = 5, following
dice_1       | Value = 2, epoch = 5, leading
dice_3       | Value = 2, epoch = 5, following
dice_2       | Value = 2, epoch = 5, following
dice_1       | Value = 1, epoch = 5, leading
dice_3       | Value = 2, epoch = 5, following
dice_2       | Value = 2, epoch = 5, following
```

You can scale the numbers of bookkeepers too selecting one of the bookies and
using `docker-compose up -d --scale bookie1=3`

Remember to shutdown the docker-compose service with `docker-compose down` to
remove the containers and avoid errors with leftovers in next executions of the
service.


### Makefile

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
Note: If you want to restart from scratch the cluster, please remove all its data using `sudo rm -rf /tmp/test_bk`, else the Zookeeper connection maybe fail. 

## Step by step

The simplest way to let Bookkeeper servers publish themselves with a name, which could be resolved consistently across container runs, is through creation of a [docker network](https://docs.docker.com/engine/reference/commandline/network_create/):
```
docker network create "bk_network"
```
Then we can start a Zookeeper (from [Zookeeper official image](https://hub.docker.com/_/zookeeper/)) server in standalone mode on that network:
```
docker run -d \
    --network "bk_network" \
    --name "test_zookeeper" \
    --hostname "test_zookeeper" \
    zookeeper
```
And initialize the metadata store that bookies will use to store information (This step is necessary when we setup the BookKeeper cluster first time, but is not necessary when using our current BookKeeper Docker image, because we have done this work when we start the first bookie):
```
docker run -it --rm \
    --network "bk_network" \
    --env BK_zkServers=test_zookeeper:2181 \
    apache/bookkeeper \
    bookkeeper shell metaformat
```
Now we can start our Bookkeeper ensemble (e.g. with three bookies):
```
docker run -it\
    --network "bk_network" \
    --env BK_zkServers=test_zookeeper:2181 \
    --name "bookie1" \
    --hostname "bookie1" \
    apache/bookkeeper
```
And so on for "bookie2" and "bookie3". We have now our fully functional ensemble, ready to accept clients.

In order to play with our freshly created ensemble, you can use the simple application taken from [Bookkeeper Tutorial](http://bookkeeper.apache.org/docs/master/bookkeeperTutorial.html) and packaged in a [docker image](https://github.com/caiok/bookkeeper-tutorial) for convenience.

This application check if it can be leader, if yes start to roll a dice and book this rolls on Bookkeeper, otherwise it will start to follow the leader rolls. If leader stops, follower will try to become leader and so on.

Start a dice application (you can run it several times to view the behavior in a concurrent environment):
```
docker run -it --rm \
    --network "bk_network" \
    --env ZOOKEEPER_SERVERS=test_zookeeper:2181 \
    caiok/bookkeeper-tutorial
```
## Configuration

Bookkeeper configuration is located in `/opt/bookkeeper/conf` in the docker container, it is a copy of [these files](https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/conf) in Bookkeeper repo.

There are 2 ways to set Bookkeeper configuration:

1, Apply setted (e.g. docker -e kk=vv) environment variables into configuration files. Environment variable names is in format "BK_originalName", in which "originalName" is the key in config files.

2, If you are able to handle your local volumes, use `docker --volume` command to bind-mount your local configure volumes to `/opt/bookkeeper/conf`.

Example showing how to use your own configuration files:
```
$ docker run --name bookie1 -d \
    -v $(local_configure_dir):/opt/bookkeeper/conf/ \   < == use 2nd approach, mount dir contains config_files
    -e BK_bookiePort=3181 \                             < == use 1st approach, set bookiePort
    -e BK_zkServers=zk-server1:2181,zk-server2:2181 \   < == use 1st approach, set zookeeper servers
    -e BK_journalPreAllocSizeMB=32 \                    < == use 1st approach, set journalPreAllocSizeMB in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf)
    apache/bookkeeper
```

### Override rules for Bookkeeper configuration
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

This variable allows you to specify the root directory Bookkeeper will use on Zookeeper to store ledgers metadata.

This will override `zkLedgersRootPath ` in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf).

Default value is "/bookkeeper/ledgers"

#### `BK_CLUSTER_ROOT_PATH`

This variable allows you to specify the root directory Bookkeeper will use on Zookeeper.

Default value is empty - " ". so ledgers dir in zookeeper will be at "/ledgers" by default. You could set it as that you want, e.g. "/bookkeeper"

#### `BK_DATA_DIR`
This variable allows you to specify where to store data in docker instance.

This could be override by env vars "BK_journalDirectory", "BK_ledgerDirectories", "BK_indexDirectories"  and also `journalDirectory`, `ledgerDirectories`, `indexDirectories` in [bk_server.conf](https://github.com/apache/bookkeeper/blob/master/bookkeeper-server/conf/bk_server.conf).

Default value is "/data/bookkeeper", which contains volumes `/data/bookkeeper/journal`, `/data/bookkeeper/ledger` and `/data/bookkeeper/index` to hold Bookkeeper data in docker.


### Configure files under /opt/bookkeeper/conf
These files is originally un-tared from the bookkeeper building binary, such as [bookkeeper-server-4.4.0-bin.tar.tgz](https://archive.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-4.4.0-src.tar.gz), and it comes from [these files](https://github.com/apache/bookkeeper/tree/master/bookkeeper-server/conf) in Bookkeeper repo.

Usually we could config files bk_server.conf, bkenv.sh, log4j.properties, and log4j.shell.properties. Please read and understand them before you do the configuration.


### Caveats

Be careful where you put the transaction log (journal). A dedicated transaction log device is key to consistent good performance. Putting the log on a busy device will adversely effect performance.

Here is some useful and graceful command the could be used to replace the default command, once you want to delete the cookies and do auto recovery:
```
/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie
/bookkeeper/bin/bookkeeper autorecovery
```
Use them, and replace the default [CMD] when you wanted to do things other than start a bookie.

# License

View [license information](https://github.com/apache/bookkeeper/blob/master/LICENSE) for the software contained in this image.
