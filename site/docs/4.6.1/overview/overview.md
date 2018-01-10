---
title: Apache BookKeeper&trade; 4.6.1
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

This documentation is for Apache BookKeeper&trade; version `4.6.1`.

Apache BookKeeper&trade; is a scalable, fault tolerant and low latency storage service optimized for realtime workloads.
It offers `durability`, `replication` and `strong consistency` as essentials for building reliable real-time applications.

It is suitable for being used in following scenerios:

- [WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) (Write-Ahead-Logging), e.g. HDFS [namenode](https://hadoop.apache.org/docs/r2.5.2/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html#BookKeeper_as_a_Shared_storage_EXPERIMENTAL).
- Message Store, e.g. [Apache Pulsar](https://pulsar.incubator.apache.org/).
- Offset/Cursor Store, e.g. Apache Pulsar.
- Object/Blob Store, e.g. storing snapshots to replicated state machines.

Learn more about Apache BookKeeper&trade; and what it can do for your organization:

- [Apache BookKeeper 4.6.1 Release Notes](../releaseNotes)
- [Java API docs](../../api/javadoc)

Or start using Apache BookKeeper today.

### Users 

- **Concepts**: Start with [concepts](../../getting-started/concepts). This will help you to fully understand
    the other parts of the documentation, including the setup, integration and operation guides.
- **Getting Started**: Install [Apache BookKeeper](../../getting-started/installation) and run bookies [locally](../../getting-started/run-locally)
- **API**: Read the [API](../../api/overview) documentation to learn how to use Apache BookKeeper to build your applications.
- **Deployment**: The [Deployment Guide](../../deployment/manual) shows how to deploy Apache BookKeeper to production clusters.

### Administrators

- **Operations**: The [Admin Guide](../../admin/bookies) shows how to run Apache BookKeeper on production, what are the production
    considerations and best practices.

### Contributors

- **Details**: Learn [design details](../../development/protocol) to know more internals.
