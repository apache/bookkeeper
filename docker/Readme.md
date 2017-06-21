# Supported tags and respective `Dockerfile` links

* `4.4.0`, `4.4`, `latest` [(4.4.0/Dockerfile)](https://github.com/caiok/bookkeeper-docker/blob/master/4.4.0/Dockerfile)

# What is Apache Bookkeeper?

Apache ZooKeeper is a software project of the Apache Software Foundation, providing a replicated log service which can be used to build replicated state machines. A log contains a sequence of events which can be applied to a state machine. BookKeeper guarantees that each replica state machine will see all the same entries, in the same order.

> [Apache Bookkeeper](http://bookkeeper.apache.org/)


# How to use this image

Bookkeeper needs [Zookeeper](https://zookeeper.apache.org/) in order to preserve its state and publish its bookies (bookkepeer servers). The client only need to connect to a Zookkeeper server in the ensamble in order to obtain the list of Bookkeeper servers.

## TL;DR

If you just want to see things working, you can play with Makefile hosted in this project and check its targets for a fairly complex set up example:

	git clone https://github.com/caiok/bookkeeper-docker
	cd bookkeeper-docker
	make run-demo

This will do all the following steps and start up a working ensamble with two dice applications.
You only need GNU Make 4.0 or above and a X terminal emulator (or, if you don't have access to a X enviornment, you can play by hand all commands in "run-demo" target).


## Step by step

This means that Bookkeeper servers need to publish themselves with a name that should be resolved consistently across container runs.

The simplest way to achieve this is through creation of a docker network:

	docker network create "my-bookkeeper-network"

Then we can start a Zookeeper (from [Zookeeper official image](https://hub.docker.com/_/zookeeper/)) server in standalone mode on that network:

	docker run -d \
		--network "my-bookkeeper-network" \
		--name "my-zookeeper" \
		--hostname "my-zookeeper" \
		zookeeper

And initialize the filesystem that bookies will use to store informations:

	docker run -it --rm \
		--network "my-bookkeeper-network" \
		--env ZK_SERVERS=my-zookeeper:2181 \
		bookkeeper \
		bookkeeper shell metaformat

Where the last line is the command is going to be executed in the bookkeeper container). Now we can start our Bookkeeper ensamble (e.g. with three bookies):

	docker run -it\
		--network "my-bookkeeper-network" \
		--env ZK_SERVERS=my-zookeeper:2181 \
		--name "bookie1" \
		--hostname "bookie1" \
		bookkeeper

And so on for "bookie2" and "bookie3". We have now our fully functional ensamble, ready to accept clients. 

In order to play with our freshly created ensamble, you can use the simple application taken from [Bookkeeper Tutorial](http://bookkeeper.apache.org/docs/master/bookkeeperTutorial.html) and packaged in a docker image for conveniece (you may be interested in [see its source code](https://github.com/caiok/bookkeeper-tutorial)). This application check if he can be leader, if yes start to roll a dice and book this rolls on bookkeeper, otherwise it will start to follow the leader rolls. If leader stops, follower will try to become leader and so on.

Start a dice application (you can run it several times to view the behavior in a cuncurrent environment):
	
	docker run -it --rm \
		--network "my-bookkeeper-network" \
		--env ZOOKEEPER_SERVERS=my-zookkeeper:2181 \
		caiok/bookkeeper-tutorial


## Configuration

Bookkeeper configuration is located in `${BK_DIR}/conf`. When run script is executed it copies all contents of `/conf` dir to `${BK_DIR}/conf`, then apply several substitutions in `${BK_DIR}/conf/bk_server.conf` with values contained in environment variables.

Some of these values should be left unchanged (e.g. the container directories where bookkeeper stores data) unless you are sure of what you are doing. The environment variables that you possibly need to change (and that necessarily need to specify even if you pass your own bk conf files) are listed below.

Example showing how to use your own configuration files:

	$ docker run --name bookie1 -d \
		-v $(pwd)/bk_server.conf:/conf/bk_server.conf \
		-v $(pwd)/<file>.conf:/conf/<file>.conf \
		-e BK_PORT=3181 \
		-e ZK_SERVERS=zk-server1:2181,zk-server2:2181 \
		 bookkeeper

### `BK_PORT`

This variable allows you to specify the port on which Bookkeeper should listen for incoming connections.

### `ZK_SERVERS`

This variable allows you to specify a list of machines of the Zookeeper ensemble. Each entry has the form of `host:port`. Entries are separated with a comma. 

### `BK_LEDGERS_PATH`

This variable allows you to specify the root directory bookkeeper will use on Zookeeper.

### Caveats

If you pass one of these environment variables, the corresponding value in bk_server.conf will be rewritten, regardless of whether you have passed bk_server.con via volume mount.

When run starts, the option `useHostNameAsBookieID=true` is always setted (no ways to change this behavior yet). Open an issue on Github if this creates inconvenients.


## Where to store data

This image is configured with volumes at `/data/journal`, `/data/ledger` and `/data/index` to hold Bookkeeper data.

> Be careful where you put the transaction log (journal). A dedicated transaction log device is key to consistent good performance. Putting the log on a busy device will adversely effect performance.

# License

View [license information](https://github.com/apache/bookkeeper/blob/master/LICENSE) for the software contained in this image.
