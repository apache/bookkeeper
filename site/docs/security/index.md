---
title: BookKeeper Security
---

In the 4.5.0 release, the BookKeeper community added a number of features that can be used, together or separately, to secure a BookKeeper cluster.
The following security measures are currently supported:

1. Authentication of connections to bookies from clients, using either [TLS](./tls) or [SASL (Kerberos)](./sasl).
2. Authentication of connections from clients, bookies, autorecovery daemons to [ZooKeeper](./zookeeper), when using zookeeper based ledger managers.
3. Encryption of data transferred between bookies and clients, between bookies and autorecovery daemons using [TLS](./tls).
<!-- TODO: authorization is not supported yet
4. Authorization of read / write operations by clients
5. Authorization is pluggable and integration with external authorization services is supported
-->

It’s worth noting that security is optional - non-secured clusters are supported, as well as a mix of authenticated, unauthenticated, encrypted and non-encrypted clients.

(TBD: generate a toc ?)
