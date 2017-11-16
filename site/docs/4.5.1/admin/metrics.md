---
title: Metric collection
---

BookKeeper enables metrics collection through a variety of [stats providers](#stats-providers).

> For a full listing of available metrics, see the [Metrics](../../reference/metrics) reference doc.

## Stats providers

BookKeeper has stats provider implementations for four five sinks:

Provider | Provider class name
:--------|:-------------------
[Codahale Metrics](https://mvnrepository.com/artifact/org.apache.bookkeeper.stats/codahale-metrics-provider) | `org.apache.bookkeeper.stats.CodahaleMetricsProvider`
[Prometheus](https://prometheus.io/) | `org.apache.bookkeeper.stats.PrometheusMetricsProvider`
[Finagle](https://twitter.github.io/finagle/guide/Metrics.html) | `org.apache.bookkeeper.stats.FinagleStatsProvider`
[Ostrich](https://github.com/twitter/ostrich) | `org.apache.bookkeeper.stats.OstrichProvider`
[Twitter Science Provider](https://mvnrepository.com/artifact/org.apache.bookkeeper.stats/twitter-science-provider) | `org.apache.bookkeeper.stats.TwitterStatsProvider`

> The [Codahale Metrics]({{ site.github_master }}/bookkeeper-stats-providers/codahale-metrics-provider) stats provider is the default provider.

## Enabling stats providers in bookies

There are two stats-related [configuration parameters](../../reference/config#statistics) available for bookies:

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
