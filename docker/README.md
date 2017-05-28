# docker-distributedlog

Dockerfile for [BookKeeper](http://bookkeeper.apache.org/).

Some environment variables for docker image:

|  *name*  |  *description*  |  *default value*  | 
|----------|-----------------|-------------------|
| BOOKIE_PORT     | bookie service port      |  3181 |
| ZK_URL          | zookeeper server         |  127.0.0.1:2181 |
| BK_DIR          | bookie storage directory |  /bk, journal dir: /bk/journal, ledgers dir: /bk/ledgers, index dir: /bk/index |
| BK_CLUSTER_NAME | bookkeeper cluster name  |  /bookkeeper, and this make bookeeper ledgers endpoint in zookeeper is : /${BK_CLUSTER_NAME}/ledgers  |

An example command to start a bookie instance:
```
$ docker run -t -i -e "ZK_URL=192.168.100.100:2181" bookkeeper/bookie /bin/bash
```
