---
id: upgrade
title: Upgrade
---

> If you have questions about upgrades (or need help), please feel free to reach out to us by [mailing list](/community/mailing-lists) or [Slack Channel](/community/slack).

## Overview

Consider the below guidelines in preparation for upgrading.

- Always back up all your configuration files before upgrading.
- Read through the documentation and draft an upgrade plan that matches your specific requirements and environment before starting the upgrade process.
    Put differently, don't start working through the guide on a live cluster. Read guide entirely, make a plan, then execute the plan.
- Pay careful consideration to the order in which components are upgraded. In general, you need to upgrade bookies first and then upgrade your clients.
- If autorecovery is running along with bookies, you need to pay attention to the upgrade sequence.
- Read the release notes carefully for each release. They contain not only information about noteworthy features, but also changes to configurations
    that may impact your upgrade.
- Always upgrade one or a small set of bookies to canary new version before upgraing all bookies in your cluster.

## Canary

It is wise to canary an upgraded version in one or small set of bookies before upgrading all bookies in your live cluster.

You can follow below steps on how to canary a upgraded version:

1. Stop a Bookie.
2. Upgrade the binary and configuration.
3. Start the Bookie in `ReadOnly` mode. This can be used to verify if the Bookie of this new version can run well for read workload.
4. Once the Bookie is running at `ReadOnly` mode successfully for a while, restart the Bookie in `Write/Read` mode.
5. After step 4, the Bookie will serve both write and read traffic.

### Rollback Canaries

If problems occur during canarying an upgraded version, you can simply take down the problematic Bookie node. The remain bookies in the old cluster
will repair this problematic bookie node by autorecovery. Nothing needs to be worried about.

## Upgrade Steps

Once you determined a version is safe to upgrade in a few nodes in your cluster, you can perform following steps to upgrade all bookies in your cluster.

1. Determine if autorecovery is running along with bookies. If yes, check if the clients (either new clients with new binary or old clients with new configurations)
are allowed to talk to old bookies; if clients are not allowed to talk to old bookies, please [disable autorecovery](../reference/cli/#autorecovery-1) during upgrade.
2. Decide on performing a rolling upgrade or a downtime upgrade.
3. Upgrade all Bookies (more below)
4. If autorecovery was disabled during upgrade, [enable autorecovery](../reference/cli/#autorecovery-1).
5. After all bookies are upgraded, build applications that use `BookKeeper client` against the new bookkeeper libraries and deploy the new versions.

### Upgrade Bookies

In a rolling upgrade scenario, upgrade one Bookie at a time. In a downtime upgrade scenario, take the entire cluster down, upgrade each Bookie, then start the cluster.

For each Bookie:

1. Stop the bookie.
2. Upgrade the software (either new binary or new configuration)
2. Start the bookie.

## Upgrade Guides

We describe the general upgrade method in Apache BookKeeper as above. We will cover the details for individual versions.

### 4.16.x to 4.17.x upgrade

4.17.x includes multiple important features, improvements, bug fixes and some dependencies CVE fixes.Some dependencies
has been upgrade:

* bc-fips
* commons-codec
* commons-compress
* datasketches
* grpc and protobuf
* guava
* jetty
* netty
* rocksdb
* snappy-java
* zookeeper

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new setting in 4.17.0

##### Deprecated Settings

There are no common settings deprecated at 4.17.0.

##### Changed Settings

There are no setting changed in 4.17.0

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.17.0

##### Changed Settings

There are no changed setting in 4.17.0

#### Client Configuration Changes

##### New Settings

There are no new setting in 4.17.0

### 4.15.x to 4.16.x upgrade

4.16.x includes multiple important features, improvements, bug fixes and some dependencies CVE fixes.

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

| Name               | Description                                         |
|--------------------|-----------------------------------------------------|
| Counter.addLatency | count the time and convert the time to milliseconds |

##### Deprecated Settings

There are no common settings deprecated at 4.16.0.

##### Changed Settings

| Old_Name                             | New_name                     | Notes                                                                                                                      |
|--------------------------------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| Counter.add                          | Counter.addCount             |                                                                                                                            |
| bookkeeper_server_ADD_ENTRY_REQUEST  | bookkeeper_server_ADD_ENTRY  | When using V2 protocol, the bookkeeper_server_ADD_ENTRY_REQUEST and bookkeeper_server_READ_ENTRY_REQUEST stats do not work |
| bookkeeper_server_READ_ENTRY_REQUEST | bookkeeper_server_READ_ENTRY | When using V2 protocol, the bookkeeper_server_ADD_ENTRY_REQUEST and bookkeeper_server_READ_ENTRY_REQUEST stats do not work |

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.16.0

##### Changed Settings

There are no changed setting in 4.16.0

#### Client Configuration Changes

##### New Settings

There are no new setting in 4.16.0

### 4.14.x to 4.15.x upgrade

4.15.x includes many upgrades to third party libraries marked with CVEs,
adds more configuration options, extends REST API,
adds an option to run without journal, improves memory utilization and stability.

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

| Name                                                    | Description                                                       |
|---------------------------------------------------------|-------------------------------------------------------------------|
| clientTcpUserTimeoutMillis                              | Add TCP_USER_TIMEOUT to Epoll channel config                      |
| auditorMaxNumberOfConcurrentOpenLedgerOperations        | Add auditor get ledger throttle to avoid auto recovery zk session |
| auditorAcquireConcurrentOpenLedgerOperationsTimeOutMSec | Add auditor get ledger throttle to avoid auto recovery zk session |

##### Deprecated Settings

There are no common settings deprecated at 4.15.0.

##### Changed Settings

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.15.0

##### Changed Settings

There are no changed setting in 4.15.0

#### Client Configuration Changes

##### New Settings

| Name                  | Description                                                               |
|-----------------------|---------------------------------------------------------------------------|
| ledgerMetaDataVersion | Bookkeeper-Client config to write ledger metadata with configured version |

### 4.13.x to 4.14.x upgrade

4.14.x adds FIPS compliance, improves compaction logic and the Stream Storage,
improves data reliability in the recovery scenarios,
fixes multiple bugs and brings critical dependencies up-to-date. The Bookkeeper FIPS compliant has been made by default.
Some dependencies have been upgraded:

- lombok (required for Java 16 support)
- netty
- rocksdb

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new settings at 4.14.0.

##### Deprecated Settings

There are no common settings deprecated at 4.14.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.14.0.

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.14.0

##### Changed Settings

There are no changed setting in 4.14.0

#### Client Configuration Changes

There are no client configuration changes in 4.14.0

### 4.12.x to 4.13.x upgrade

The new version 4.13 improves reliability of the Stream Storage,
brings additional configuration options for the Stream Storage and Prometheus HTTP Server,
fixes multiple bugs and brings critical dependencies up-to-date. Some dependencies have been upgraded: the protobuf has
been upgraded to 3.14.0,GRPC has been upgraded to 1.33,Netty has been upgraded to 4.1.50Final and dropwizard has been
upgraded to 3.2.5

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new settings at 4.13.0.

##### Deprecated Settings

There are no common settings deprecated at 4.13.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.13.0.

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.13.0

##### Changed Settings

There are no changed setting in 4.13.0

#### Client Configuration Changes

There are no client configuration changes in 4.13.0

### 4.11.x to 4.12.x upgrade

There are no changes making on the wire protocol, on metadata and on persisted data on disks by default during the
upgrade of 4.12.0.
With BookKeeper 4.12.0 we are making a step toward better deployment on environments with dynamic network addresses with
BP-41.
We are also enhancing the new Client API by adding features that were still missing, like the ability of queryng for
ledger metadata.

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new settings at 4.12.0.

##### Deprecated Settings

There are no common settings deprecated at 4.12.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.12.x.

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.12.0

##### Changed Settings

There are no changed setting in 4.12.0

#### Client Configuration Changes

There are no client configuration changes in 4.12.0

### 4.10.x to 4.11.x upgrade

4.11.x brings a trove of enhancements over the previous version, this update includes a multitude of bug fixes,
performance improvements, and new features that are set to elevate the bookkeeper experience. The version of Zookkeeper
has been upgraded from 3.14.13 to 3.5.7, and the bookkeeper-server now relies on the org.apache.httpcomponents-http-core
version 4.4.9 for improved HTTP functionalities.

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new settings at 4.11.0.

##### Deprecated Settings

There are no common settings deprecated at 4.11.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.11.0.

#### Server Configuration Changes

##### Deprecated Settings

There are no setting deprecated in 4.11.0

##### Changed Settings

The default values of following settings are changed since 4.11.0.

There are no changed setting in 4.11.0

#### Client Configuration Changes

There are no client configuration changes in 4.11.0

### 4.9.x to 4.10.x upgrade

In 4.10.x some significant updates have been made to handle the ledger metadata to enhance data consistency and access
efficiency. The default bookie script JDK version has been set to 11 and some bugs has been fixed

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new settings at 4.10.0.

##### Deprecated Settings

There are no common settings deprecated at 4.10.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.10.0.

#### Server Configuration Changes

##### Deprecated Settings

There are no deprecated setting at 4.10.0

##### New Setting

Following settings are added since 4.10.0.

| Name                | Description                                      |
|---------------------|--------------------------------------------------|
| flushWhenQueueEmpty | control when data is flushed from memory to disk |

##### Changed Settings

The default values of following settings are changed since 4.10.0.

There are no changed setting in 4.10.0

#### Client Configuration Changes

There are no client configuration changes in 4.10.0

### 4.8.x to 4.9.x upgrade

In 4.9.x some significant updates have been made to handle the ledger metadata to enhance data consistency and access
efficiency. Consequently, the current journal format version has been upgraded to 6, and the FileInfo header version has
been upgraded to 1. However, since the default configuration values of 'journalFormatVersionToWrite' and '
fileInfoFormatVersionToWrite' are set to older versions, this feature is off by default. To enable this feature, those
config values should be set to the current versions. Once this is enabled, you cannot roll back to previous Bookie
versions (4.8.x and older), as the older version code would not be able to deal with the explicitLac entry in the
Journal file while replaying the journal and reading the header of Index files / FileInfo would fail reading Index files
with the newer FileInfo version. So, in summary, it is a non-rollbackable feature, and it applies even if explicitLac is
not in used.

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

There are no new settings deprecated at 4.9.0.

##### Deprecated Settings

There are no common settings deprecated at 4.9.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.9.0.

#### Server Configuration Changes

##### New Settings

Following settings are added since 4.9.0.

| Name               | Description                                     |
|--------------------|-------------------------------------------------|
| serverNumIOThreads | configures the number of IO threads for bookies |

##### Changed Settings

The default values of following settings are changed since 4.9.0.

| Name                         | Old Default Value | New Default Value | Notes |
|------------------------------|-------------------|-------------------|-------|
| fileInfoFormatVersionToWrite | 0                 | 1                 |       |
| journalFormatVersionToWrite  | 5                 | 6                 |       |

#### Client Configuration Changes

##### New Settings

Following settings are newly added in 4.9.0.

| Name         | Default Value                               | Description                                    |
|--------------|---------------------------------------------|------------------------------------------------|
| numIoThreads | 2 * Runtime.getRunTime().availableProcessed | configures the number of IO threads for client |

### 4.7.x to 4.8.x upgrade

In 4.8.x a new feature is added to persist explicitLac in FileInfo and explicitLac entry in Journal. (Note: Currently
this feature is not available if your ledgerStorageClass is DbLedgerStorage, ISSUE #1533 is going to address it) Hence
current journal format version is bumped to 6 and current FileInfo header version is bumped to 1. But since default
config values of 'journalFormatVersionToWrite' and 'fileInfoFormatVersionToWrite' are set to older versions, this
feature is off by default. To enable this feature those config values should be set to current versions. Once this is
enabled then we cannot rollback to previous Bookie versions (4.7.x and older), since older version code would not be
able to deal with explicitLac entry in Journal file while replaying journal and also reading Header of Index files /
FileInfo would fail reading Index files with newer FileInfo version. So in summary, it is a non-rollbackable feature and
it applies even if explicitLac is not being used.

### 4.6.x to 4.7.x upgrade

There isn't any protocol related backward compabilities changes in 4.7.0. So you can follow the general upgrade sequence to upgrade from 4.6.x to 4.7.0.

However, we list a list of changes that you might want to know.

#### Common Configuration Changes

This section documents the common configuration changes that applied for both clients and servers.

##### New Settings

Following settings are newly added in 4.7.0.

| Name | Default Value | Description |
|------|---------------|-------------|
| allowShadedLedgerManagerFactoryClass | false | The allows bookkeeper client to connect to a bookkeeper cluster using a shaded ledger manager factory |
| shadedLedgerManagerFactoryClassPrefix | `dlshade.` | The shaded ledger manager factory prefix. This is used when `allowShadedLedgerManagerFactoryClass` is set to true |
| metadataServiceUri | null | metadata service uri that bookkeeper is used for loading corresponding metadata driver and resolving its metadata service location |
| permittedStartupUsers | null | The list of users are permitted to run the bookie process. Any users can run the bookie process if it is not set |

##### Deprecated Settings

There are no common settings deprecated at 4.7.0.

##### Changed Settings

There are no common settings whose default value are changed at 4.7.0.

#### Server Configuration Changes

##### New Settings

Following settings are newly added in 4.7.0.

| Name | Default Value | Description |
|------|---------------|-------------|
| verifyMetadataOnGC | false | Whether the bookie is configured to double check the ledgers' metadata prior to garbage collecting them |
| auditorLedgerVerificationPercentage  | 0 | The percentage of a ledger (fragment)'s entries will be verified by Auditor before claiming a ledger (fragment) is missing |
| numHighPriorityWorkerThreads | 8 | The number of threads that should be used for high priority requests (i.e. recovery reads and adds, and fencing). If zero, reads are handled by Netty threads directly. |
| useShortHostName | false | Whether the bookie should use short hostname or [FQDN](https://en.wikipedia.org/wiki/Fully_qualified_domain_name) hostname for registration and ledger metadata when useHostNameAsBookieID is enabled. |
| minUsableSizeForEntryLogCreation | 1.2 * `logSizeLimit` | Minimum safe usable size to be available in ledger directory for bookie to create entry log files (in bytes). |
| minUsableSizeForHighPriorityWrites | 1.2 * `logSizeLimit` | Minimum safe usable size to be available in ledger directory for bookie to accept high priority writes even it is in readonly mode. |

##### Deprecated Settings

Following settings are deprecated since 4.7.0.

| Name | Description |
|------|-------------|
| registrationManagerClass | The registration manager class used by server to discover registration manager. It is replaced by `metadataServiceUri`. |


##### Changed Settings

The default values of following settings are changed since 4.7.0.

| Name | Old Default Value | New Default Value | Notes |
|------|-------------------|-------------------|-------|
| numLongPollWorkerThreads | 10 | 0 | If the number of threads is zero or negative, bookie can fallback to use read threads for long poll. This allows not creating threads if application doesn't use long poll feature. |

#### Client Configuration Changes

##### New Settings

Following settings are newly added in 4.7.0.

| Name | Default Value | Description |
|------|---------------|-------------|
| maxNumEnsembleChanges | Integer.MAX\_VALUE | The max allowed ensemble change number before sealing a ledger on failures |
| timeoutMonitorIntervalSec | min(`addEntryTimeoutSec`, `addEntryQuorumTimeoutSec`, `readEntryTimeoutSec`) | The interval between successive executions of the operation timeout monitor, in seconds |
| ensemblePlacementPolicyOrderSlowBookies | false | Flag to enable/disable reordering slow bookies in placement policy |

##### Deprecated Settings

Following settings are deprecated since 4.7.0.

| Name | Description |
|------|-------------|
| clientKeyStoreType | Replaced by `tlsKeyStoreType` |
| clientKeyStore | Replaced by `tlsKeyStore` |
| clientKeyStorePasswordPath | Replaced by `tlsKeyStorePasswordPath` |
| clientTrustStoreType | Replaced by `tlsTrustStoreType` |
| clientTrustStore | Replaced by `tlsTrustStore` |
| clientTrustStorePasswordPath | Replaced by `tlsTrustStorePasswordPath` |
| registrationClientClass | The registration client class used by client to discover registration service. It is replaced by `metadataServiceUri`. |

##### Changed Settings

The default values of following settings are changed since 4.7.0.

| Name | Old Default Value | New Default Value | Notes |
|------|-------------------|-------------------|-------|
| enableDigestTypeAutodetection | false | true | Autodetect the digest type and passwd when opening a ledger. It will ignore the provided digest type, but still verify the provided passwd. |

### 4.5.x to 4.6.x upgrade

There isn't any protocol related backward compabilities changes in 4.6.x. So you can follow the general upgrade sequence to upgrade from 4.5.x to 4.6.x.

### 4.4.x to 4.5.x upgrade

There isn't any protocol related backward compabilities changes in 4.5.0. So you can follow the general upgrade sequence to upgrade from 4.4.x to 4.5.x.
However, we list a list of things that you might want to know.

1. 4.5.x upgrades netty from 3.x to 4.x. The memory usage pattern might be changed a bit. Netty 4 uses more direct memory. Please pay attention to your memory usage
    and adjust the JVM settings accordingly.
2. `multi journals` is a non-rollbackable feature. If you configure a bookie to use multiple journals on 4.5.x you can not roll the bookie back to use 4.4.x. You have
    to take a bookie out and recover it if you want to rollback to 4.4.x.

If you are planning to upgrade a non-secured cluster to a secured cluster enabling security features in 4.5.0, please read [BookKeeper Security](../security/overview) for more details.
