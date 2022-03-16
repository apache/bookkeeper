---
id: metrics
title: Metric collection
---

BookKeeper enables metrics collection through a variety of [stats providers](#stats-providers).

## Stats providers

BookKeeper has stats provider implementations for these sinks:

Provider | Provider class name
:--------|:-------------------
[Codahale Metrics](https://mvnrepository.com/artifact/org.apache.bookkeeper.stats/codahale-metrics-provider) | `org.apache.bookkeeper.stats.CodahaleMetricsProvider`
[Prometheus](https://prometheus.io/) | `org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider`

> The [Codahale Metrics]({{ site.github_master }}/bookkeeper-stats-providers/codahale-metrics-provider) stats provider is the default provider.

## Enabling stats providers in bookies

Two stats-related [configuration parameters](../reference/config/) are available for bookies:

Parameter | Description | Default
:---------|:------------|:-------
`enableStatistics` | Whether statistics are enabled for the bookie | `false`
`statsProviderClass` | The stats provider class used by the bookie | `org.apache.bookkeeper.stats.CodahaleMetricsProvider`


To enable stats:

* set the `enableStatistics` parameter to `true`
* set `statsProviderClass` to the desired provider (see the [table above](#stats-providers) for a listing of classes)

<!-- ## Enabling stats in the bookkeeper library

TODO
-->
