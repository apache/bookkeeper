---
title: BookKeeper API
---

BookKeeper offers a few APIs that applications can use to interact with it:

* The [ledger API](../ledger-api) is a lower-level API that enables you to interact with {% pop ledgers %} directly
* The [Ledger Advanced API)(../ledger-adv-api) is an advanced extension to [Ledger API](../ledger-api) to provide more flexibilities to applications.
* The [DistributedLog API](../distributedlog-api) is a higher-level API that provides convenient abstractions.

## Trade-offs

The `Ledger API` provides direct access to ledgers and thus enables you to use BookKeeper however you'd like.

However, in most of use cases, if you want a `log stream`-like abstraction, it requires you to manage things like tracking list of ledgers,
managing rolling ledgers and data retention on your own. In such cases, you are recommended to use [DistributedLog API](../distributedlog-api),
with semantics resembling continous log streams from the standpoint of applications.
