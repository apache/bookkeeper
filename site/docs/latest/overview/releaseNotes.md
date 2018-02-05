---
title: Apache BookKeeper 4.7.0-SNAPSHOT Release Notes
---

Apache BookKeeper {{ site.latest_version }} is still under developement.

If you want to learn the progress of `{{ site.latest_version }}`, you can do:

- Track the progress by following the [issues](https://github.com/apache/bookkeeper/issues) on Github.
- Or [subscribe](mailto:dev-subscribe@bookkeeper.apache.org) the [dev@bookkeeper.apache.org](mailto:dev@bookkeeper.apache.org)
    to join development discussions, propose new ideas and connect with contributors.
- Or [join us on Slack](https://apachebookkeeper.herokuapp.com/) to connect with Apache BookKeeper committers and contributors.

### Existing API changes

- The default ledger manager factory is changed from FlatLedgerManagerFactory to HierarchicalLedgerManagerFactory if `ledgerManagerFactoryClass`
  is specified. If you have a cluster running with older versions and no `ledgerManagerFactoryClass` is set before, you need to set
  `ledgerManagerFactoryClass` explicitly to `org.apache.bookkeeper.meta.FlatLedgerManagerFactory` in your bookie configuration before upgrade.
