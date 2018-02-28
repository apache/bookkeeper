---
title: "BP-28: use etcd as metadata store"
issue: https://github.com/apache/bookkeeper/<issue-number>
state: 'Under Discussion'
release: "N/A"
---

### Motivation

Currently bookkeeper uses zookeeper as the metadata store. However there is a couple of issues with current approach, especially using zookeeper.

These issues includes:

1. You need to allocate special nodes for zookeeper. These nodes need to be treated specially, and have their own monitoring.
   Ops need to understand both bookies and zookeeper.
2. ZooKeeper is the scalability bottleneck. ZooKeeper doesn’t scale writes as you add nodes. This means that if your bookkeeper
   cluster reaches the maximum write throughput that ZK can sustain, you’ve reached the maximum capacity of your cluster, and there’s nothing you
   can do (except buy bigger hardware for your special nodes).
3. ZooKeeper enforces you into its programming model. In general, its programming model is not too bad. However it becomes problematic when
   the scale goes up (e.g. the number of clients and watcher increase). The issues usually comes from _session expires_ and _watcher_.
  - *Session Expires*: For simplicity, ZooKeeper ties session state directly with connection state. So when a connection is broken, a session is usually expired (unless it reconnects before session expires), and when a session is expired, the underlying connection can not be used anymore, the application has to close the connection and re-establish a new client (a new connection). It is understandable that it makes zookeeper development easy. However in reality, it means if you can not establish a session, you can’t use this connection and you have to create new connections. Once your zookeeper cluster is in a bad state (e.g. network issue or jvm gc), the whole cluster is usually unable to recover because of the connection storm introduced by session expires.
  - *Watchers*: The zookeeper watcher is one time watcher, applications can’t reliably use it to get updates. In order to set a watcher, you have to read a znode or get children. Imagine such a use case, clients are watching a list of znodes (e.g. list of bookies), when those clients expire, they have to get the list of znodes in order to rewatch the list, even the list is never changed.
  - The combination of session expires and watchers is often the root cause of critical zookeeper outages.

This proposal is to explore other existing systems such as etcd as the metadata store. Using Etcd doesn't address concerns #1, however it might potentially
address concern #2 and #3 to some extend. And if you are running bookkeeper in k8s, there is already an Etcd instance available. It can become easier to run
bookkeeper on k8s if we can use Etcd as the metadata store.

NOTE: this proposal has some overlaps on goals/vision with the distributed k/v store work (a separate BP will be sent out soon). However they are not conflicting with each other.
Both proposals are exploring a better metadata storage solution for bookkeeper.

### Public Interfaces

A new metadata store module `metadata-store-etcd` will be added to bookkeeper. This module will be implementing all the required metadata interfaces:

These interfaces include:

- RegistrationClient
- RegistrationManager
- LayoutManager
- LedgerIdGenerator
- LedgerManager
- LedgerUnderreplicatedManager
- LedgerManagerFactory

### Proposed Changes

Since Etcd provides a key/value model rather than a tree-like structure. So the metadata will be organized in a key/value way as below:

- `scope`: the prefix used for prefixing all the keys used for storing the metadata for a given cluster. The `scope` is effectively same as `zkLedgerRootPath`.
- `scope`/LAYOUT: key for storing layout data
- `scope`/INSTANCEID: key for storing instance id
- `scope`/IDGEN/`<bucket>`: key for id generation. `<bucket>` is to allow concurrent id generation.
- `scope`/cookies/`<bookieid>`: key for storing a bookie's cookie
- `scope`/available/readwrite/`<bookieid>`: key for registering readwrite bookie. (lease will be applied to this key)
- `scope`/available/readonly/`<bookieid>`: key for registering readonly bookie. (lease will be applied to this key)
- `scope`/ledgers/`<ledgerid>`: key for storing a ledger metadata. `<ledgerid>` is `String.format("%19d", ledgerId)`, pre-padding with 0s to make sure ledgers are stored in order.

#### Registration Manager

- Bookie Register: write to key "`scope`/available/readwrite/`<bookieid>`" with a keepalive lease.
- Bookie Unregister: delete key "`scope`/available/readwrite/`<bookieid>`".

#### Registration Client

- Get readwrite bookies: range operation to fetch keys between "`scope`/available/readwrite/" and "`scope`/available/readwrite_end/".
- Get readonly bookies: range operation to fetch keys between "`scope`/available/readonly/" and "`scope`/available/readonly_end/".
- Watch readwrite bookies: watch operation to watch keys between "`scope`/available/readwrite/" and "`scope`/available/readwrite_end/".
- Watch readonly bookies: watch operation to watch keys between "`scope`/available/readonly/" and "`scope`/available/readonly_end/". 

#### Layout Manager

- Create layout: a txn operation: put layout data to key "`scope`/LAYOUT" when this key doesn't exist.
- Delete layout: a delete operation on "`scope`/LAYOUT"
- Read layout: a get operation on "`scope`/LAYOUT"

#### Ledger Id Generator

- Id generation: get key, increment by 1 and then update with a txn operation.

#### Ledger Manager

- Create Ledger: a txn operation to put ledger metadata to key "`scope`/ledgers/`<ledgerid>`" when this key doesn't exist
- Write Ledger: a txn operation to put ledger metadata to key "`scope`/ledgers/`<ledgerid>`" when key version matches
- Delete Ledger: a delete operation on key "`scope`/ledgers/`<ledgerid>`"
- Read Ledger: a get operation on key "`scope`/ledgers/`<ledgerid>`"
- Iteration: a range operation fetch keys between "`scope`/ledgers/" and "`scope`/ledgers_end/"

### Compatibility, Deprecation, and Migration Plan

- This is a new metadata store. No impacts to existing users.
- We are not planning to implement any migration tools any time soon. This only for new users or setting up new clusters.

### Test Plan

- Unit tests for all the metadata manager implementation.
- End-to-end integration tests
- Jepsen tests

### Rejected Alternatives

N/A
