---
title: "BP-26: Move distributedlog library as part of bookkeeper"
issue: https://github.com/apache/bookkeeper/1024
state: 'Accepted'
release: "N/A"
---

### Motivation

DistributedLog is an extension of Apache BookKeeper, which offers *reopenable* log streams as its storage primitives.
It is tightly built over bookkeeper ledgers, and provides an easier-to-use abstraction and api to use. Applications
can use *named* log streams rather than *numbered* ledgers to store their data. For example, users can use log streams
as files to storge objects, checkpoints and other more general filesystem related use cases.

Moving the distributedlog core library as part of bookkeeper would have following benefits:

- It provides more generic "reopenable" log abstraction. It lowers the barrier for people to use bookkeeper to store
  data, and bring in more use cases into bookkeeper ecosystem.
- Using ledgers to build continous log stream has been a pattern that been reimplemented multiple times at multiple places,
  from older projects like HDFS namenode log manager, Hedwig to the newer projects like DistributedLog and Pulsar.
- Most of the distributedlog usage is using the distributedlog library which only depends Apache BookKeeper and there is no
  additional components introduced. To simplify those usages, it is better to release distributedlog library along with
  bookkeeper. It provides a better integration and release procedure.

This proposal proposes "moving the distributedlog library code base as part of bookkeeper and continuing the library
development in bookkeeper".

### Public Interfaces

This is a new library moved in bookkeeper. It will *NOT* touch any existing bookkeeper modules and ledger api.

### Proposed Changes

This proposal will *ONLY* move following library-only modules from distributedlog repo:

- distributedlog-core: the log stream library that build over bookkeeper ledgr api. It doesn't introduce any service components. Library only.
- distributedlog-io/dlfs: A hdfs filesystem api wrapper over the log stream api, to provide filesystem-like usage over bookkeeper.

This proposal will *NOT* move other service components like "distributedlog-proxy".

The steps to make this change are described as following:

- the proposed modules (`distributedlog-core` and `distributedlog-io/dlfs`) will be moved under `stream/distributedlog` directory at apache bookkeeper repo.
- a new "stream" profile will be added to the root `pom.xml` file. The distributedlog module will only be build when "-Pstream" is specified
  in the maven build command. This allows users who only use ledger api skip building distributedlog module.
- the distributedlog api, javadoc api and some tutorials will be integrated with current bookkeeper website to provide integrated experiences
  when users browse bookkeeper website.

### Compatibility, Deprecation, and Migration Plan

This doesn't change existing modules or api. so no compatibility, deprecation and migration plan for bookkeeper users.

For distributedlog users, the distributedlog library will begin release under groupId `org.apache.bookkeeper` instead of `org.apache.distributedlog`.
API documentation will be updated to reflect this.

### Test Plan

N/A

### Rejected Alternatives

N/A
