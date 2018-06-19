---
title: Upgrade
---

> If you have questions about upgrades (or need help), please feel free to reach out to us by [mailing list]({{ site.baseurl }}community/mailing-lists) or [Slack Channel]({{ site.baseurl }}community/slack).

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
are allowed to talk to old bookies; if clients are not allowed to talk to old bookies, please [disable autorecovery](../../reference/cli/#autorecovery-1) during upgrade.
2. Decide on performing a rolling upgrade or a downtime upgrade.
3. Upgrade all Bookies (more below)
4. If autorecovery was disabled during upgrade, [enable autorecovery](../../reference/cli/#autorecovery-1).
5. After all bookies are upgraded, build applications that use `BookKeeper client` against the new bookkeeper libraries and deploy the new versions.

### Upgrade Bookies

In a rolling upgrade scenario, upgrade one Bookie at a time. In a downtime upgrade scenario, take the entire cluster down, upgrade each Bookie, then start the cluster.

For each Bookie:

1. Stop the bookie. 
2. Upgrade the software (either new binary or new configuration)
2. Start the bookie.

## Upgrade Guides

We describes the general upgrade method in Apache BookKeeper as above. We will cover the details for individual versions.

### 4.7.0 to 4.7.1 upgrade

There isn't any protocol related backward compabilities changes in 4.7.1. So you can follow the general upgrade sequence to upgrade from 4.7.0 to 4.7.1.

### 4.6.x to 4.7.0 upgrade

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

If you are planning to upgrade a non-secured cluster to a secured cluster enabling security features in 4.5.0, please read [BookKeeper Security](../../security/overview) for more details.
