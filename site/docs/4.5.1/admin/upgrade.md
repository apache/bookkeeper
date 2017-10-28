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

### 4.5.0 to 4.5.1 upgrade

There isn't any incompatible protocol changes in 4.5.1. So you can follow the general upgrade sequence to upgrade from 4.5.0 to 4.5.1.