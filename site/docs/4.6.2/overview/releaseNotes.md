---
title: Apache BookKeeper 4.6.2 Release Notes
---

This is the ninth release of BookKeeper as an Apache Top Level Project!

The 4.6.2 release is a bugfix release which fixes a bunch of issues reported from users of 4.6.1.

Apache BookKeeper users are encouraged to upgrade to 4.6.2. The technical details of this release are summarized
below.

## Highlights

- Fix performance regression is using Netty > 4.1.12, see [https://github.com/apache/bookkeeper/pull/1108](https://github.com/apache/bookkeeper/pull/1108)

- Enhance performances on Prometheus stats provider, see [https://github.com/apache/bookkeeper/pull/1081](https://github.com/apache/bookkeeper/pull/1081)

- Save memory resources on client by retaining for less time references to data to write, see [https://github.com/apache/bookkeeper/issues/1063](https://github.com/apache/bookkeeper/issues/1063)

- Fix a problem on Java 9/10 with the 'shaded' artifacts, due to a bug in Maven Shade Plugin, see [https://github.com/apache/bookkeeper/pull/1144](https://github.com/apache/bookkeeper/pull/1144)

- Fix Journal stats names, see [https://github.com/apache/bookkeeper/pull/1250](https://github.com/apache/bookkeeper/pull/1250)

### Dependencies Upgrade

There is no dependency upgrade since 4.6.0, and since 4.6.1 we distribute a 'shaded' version of main artifacts, see [Ledger API](../ledger-api)

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.2+is%3Aclosed](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.2+is%3Aclosed)
