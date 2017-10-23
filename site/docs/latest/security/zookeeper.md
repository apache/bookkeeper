---
title: ZooKeeper Authentication
prev: ../sasl
---

## New Clusters

To enable `ZooKeeper` authentication on Bookies or Clients, there are two necessary steps:

1. Create a `JAAS` login file and set the appropriate system property to point to it as described in [GSSAPI (Kerberos)](../sasl#notes).
2. Set the configuration property `zkEnableSecurity` in each bookie to `true`.

The metadata stored in `ZooKeeper` is such that only certain clients will be able to modify and read the corresponding znodes.
The rationale behind this decision is that the data stored in ZooKeeper is not sensitive, but inappropriate manipulation of znodes can cause cluster
disruption.

## Migrating Clusters

If you are running a version of BookKeeper that does not support security or simply with security disabled, and you want to make the cluster secure,
then you need to execute the following steps to enable ZooKeeper authentication with minimal disruption to your operations.

1. Perform a rolling restart setting the `JAAS` login file, which enables bookie or clients to authenticate. At the end of the rolling restart,
    bookies (or clients) are able to manipulate znodes with strict ACLs, but they will not create znodes with those ACLs.
2. Perform a second rolling restart of bookies, this time setting the configuration parameter `zkEnableSecurity` to true, which enables the use
    of secure ACLs when creating znodes.
3. Currently we don't have provide a tool to set acls on old znodes. You are recommended to set it manually using ZooKeeper tools.

It is also possible to turn off authentication in a secured cluster. To do it, follow these steps:

1. Perform a rolling restart of bookies setting the `JAAS` login file, which enable bookies to authenticate, but setting `zkEnableSecurity` to `false`.
    At the end of rolling restart, bookies stop creating znodes with secure ACLs, but are still able to authenticate and manipulate all znodes.
2. You can use ZooKeeper tools to manually reset all ACLs under the znode set in `zkLedgersRootPath`, which defaults to `/ledgers`.
3. Perform a second rolling restart of bookies, this time omitting the system property that sets the `JAAS` login file.

## Migrating the ZooKeeper ensemble

It is also necessary to enable authentication on the `ZooKeeper` ensemble. To do it, we need to perform a rolling restart of the ensemble and
set a few properties. Please refer to the ZooKeeper documentation for more details.

1. [Apache ZooKeeper Documentation](http://zookeeper.apache.org/doc/r3.4.6/zookeeperProgrammers.html#sc_ZooKeeperAccessControl)
2. [Apache ZooKeeper Wiki](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zookeeper+and+SASL)
