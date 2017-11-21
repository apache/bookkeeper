---
title: Run bookies locally
prev: ../installation
next: ../concepts
toc_disable: true
---

{% pop Bookies %} are individual BookKeeper servers. You can run an ensemble of bookies locally on a single machine using the [`localbookie`](../../reference/cli#bookkeeper-localbookie) command of the `bookkeeper` CLI tool and specifying the number of bookies you'd like to include in the ensemble.

This would start up an ensemble with 10 bookies:

```shell
$ bookkeeper-server/bin/bookkeeper localbookie 10
```

> When you start up an ensemble using `localbookie`, all bookies run in a single JVM process.
