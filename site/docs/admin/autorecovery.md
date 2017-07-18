---
title: Setting up AutoRecovery
---

When a {% pop bookie %} in a BookKeeper cluster goes down, 

*AutoRecovery*


nodes are special nodes in a BookKeeper cluster that

[`bk_server.conf`](../../reference/config#autorecovery-settings)

AutoRecovery can be run in two ways:

1. 

## Running AutoRecovery nodes

## Disable AutoRecovery

You can disable AutoRecovery at any time, for example during maintenance. Disabling AutoRecovery ensures that bookies' data isn't unnecessarily rereplicated when the bookie is only taken down for a short period of time, for example when the bookie is being updated or the configuration if being changed.

You can disable AutoRecover using the [`bookkeeper`](../../reference/cli#bookkeeper-shell-autorecovery) CLI tool:

```bash
$ bookkeeper-server/bin/bookkeeper shell autorecovery -disable
```

Once disabled, you can reenable AutoRecovery using the [`enable`](../../reference/cli#bookkeeper-shell-autorecovery) shell command:

```bash
$ bookkeeper-server/bin/bookkeeper shell autorecovery -enable
```

