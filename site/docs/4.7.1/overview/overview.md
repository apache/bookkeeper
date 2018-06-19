---
title: Apache BookKeeper&trade; 4.7.1
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

This documentation is for Apache BookKeeper&trade; version 4.7.1.

Apache BookKeeper&trade; is a scalable, fault-tolerant, low-latency storage service optimized for real-time workloads. It offers durability, replication, and strong consistency as essentials for building reliable real-time applications.

BookKeeper is suitable for a wide variety of use cases, including:

Use case | Example
:--------|:-------
[WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) (write-ahead logging) | The HDFS [namenode](https://hadoop.apache.org/docs/r2.5.2/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html#BookKeeper_as_a_Shared_storage_EXPERIMENTAL)
Message storage | [Apache Pulsar](http://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#persistent-storage)
Offset/cursor storage | [Apache Pulsar](http://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#persistent-storage)
Object/[BLOB](https://en.wikipedia.org/wiki/Binary_large_object) storage | Storing snapshots to replicated state machines

Learn more about Apache BookKeeper&trade; and what it can do for your organization:

- [Apache BookKeeper 4.7.1 Release Notes](../releaseNotes)
- [Java API docs](../../api/javadoc)

Or start [using](../../getting-started/installation) Apache BookKeeper today.

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
