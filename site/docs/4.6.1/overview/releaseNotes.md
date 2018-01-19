---
title: Apache BookKeeper 4.6.1 Release Notes
---

This is the eighth release of BookKeeper as an Apache Top Level Project!

The 4.6.1 release is a bugfix release which fixes a bunch of issues reported from users of 4.6.0.

Apache BookKeeper users are encouraged to upgrade to 4.6.1. The technical details of this release are summarized
below.

## Highlights

- Fix critical bug on index persistence manager, see [https://github.com/apache/bookkeeper/pull/913](https://github.com/apache/bookkeeper/pull/913)

- Fix critical bug to allow using versions of Netty newer than 4.1.2 on classpath, see [https://github.com/apache/bookkeeper/pull/996](https://github.com/apache/bookkeeper/pull/996)

- Enhance Java 9 compatibility, see [https://github.com/apache/bookkeeper/issues/326](https://github.com/apache/bookkeeper/issues/326)

- New option to track task execution time, see [https://github.com/apache/bookkeeper/issues/931](https://github.com/apache/bookkeeper/issues/931)

- Distribute a version of BookKeeper which embeds and relocates Guava and Protobuf, see [https://github.com/apache/bookkeeper/issues/922](https://github.com/apache/bookkeeper/issues/922)

- Add description for the new error code "Too many requests", see [https://github.com/apache/bookkeeper/pull/921](https://github.com/apache/bookkeeper/pull/921)

### Dependencies Upgrade

There is no dependency upgrade since 4.6.0, but now we distribute a 'shaded' version of main artifacts, see [Ledger API](../ledger-api)

## Full list of changes

- [https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.1+is%3Aclosed](https://github.com/apache/bookkeeper/issues?utf8=%E2%9C%93&q=label%3Arelease%2F4.6.1+is%3Aclosed)
