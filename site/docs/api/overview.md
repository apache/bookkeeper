---
title: The ledger API vs. the DistributedLog API
---

BookKeeper offers two APIs that applications can use to interact with it:

* The [ledger API](../ledger-api) is a lower-level API that enables you to interact with ledgers directly
* The [DistributedLog API](../distributedlog-api)

## Trade-offs

The advantage of the ledger API is that it provides direct access to ledgers and thus enables you to use BookKeeper however you'd like. The disadvantage is that it requires you to manage things like leader election.

The advantage of the DistributedLog API is that it's easier to use, acting like a simple key/value store from the standpoint of applications.
