---
title: Apache BookKeeper&trade; 4.6.0-SNAPSHOT Documentation 
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

This documentation is for Apache BookKeeper version **{{ site.latest_version }}**.

Apache BookKeeper is a scalable, fault-tolerant, and low-latency log storage service optimized for real-time workloads. It offers durability, replication, and strong consistency as essentials for building reliable real-time applications.

BookKeeper is ideally suited for scenarios like:

- [Write-ahead logging](https://en.wikipedia.org/wiki/Write-ahead_logging) (WAL), for example the HDFS [NameNode](https://hadoop.apache.org/docs/r2.5.2/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html#BookKeeper_as_a_Shared_storage_EXPERIMENTAL)
- Durable storage for messaging systems, for example [Apache Pulsar](https://pulsar.incubator.apache.org/) (incubating)
- Offset/cursor store, for example Apache Pulsar
- Object/blob Store, for example storing snapshots to replicated state machines

Learn more about Apache BookKeeper&trade; and what it can do for your organization:

- [Apache BookKeeper {{ site.latest_version }} Release Notes](../releaseNotes)
- [Java API docs](../../api/javadoc)

Or start using Apache BookKeeper today.

## Guide to the documentation

You can find a full index of the BookKeeper documentation in the sidebar on the left. But if you're not sure where to start:

### BookKeeper users

- **Concepts** --- Start with the [concepts and architecture](../../getting-started/concepts) documentation. This will help you to fully understand
    other parts of the documentation, including the setup, integration, and operations guides.
- **Getting started** --- Install [Apache BookKeeper](../../getting-started/installation) and run bookies [locally](../../getting-started/run-locally).
- **API** --- Read the [API](../../api/overview) documentation to learn how to use Apache BookKeeper to build your applications.
- **Deployment** --- The [Deployment Guide](../../deployment/manual) shows you how to deploy Apache BookKeeper to production clusters.

### BookKeeper administrators

- **Operations** --- The [Admin Guide](../../admin/bookies) shows you how to run Apache BookKeeper in production and discusses best practices.

### BookKeeper contributors

- **Details** --- Learn [design details](../../development/protocol) to better understand the internals of BookKeeper.
