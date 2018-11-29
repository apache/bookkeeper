---
title: "BP-36: Stats documentation annotation"
issue: https://github.com/apache/bookkeeper/<issue-number>
state: "Accepted"
release: "4.9.0"
---

### Motivation

A common ask from people using bookkeeper is how they can monitor bookies and bookkeeper clients, what kind of metrics that bookkeeper exposes
and what are the important metrics. Currently bookkeeper doesn't provide any metrics page for guiding people on monitoring bookkeeper services.

In order to help people on this, we need to provide a few documentation pages about metrics. However if we just write such pages, those pages
can quickly get out-of-dated when code is changed. The proposal here is to seek a programming way for generating metrics related pages.

### Public Interfaces

Introduced a `StatsDoc` annotation.

```bash
/**
 * Documenting the stats.
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface StatsDoc {

    /**
     * The name of the category to group stats together.
     *
     * @return name of the stats category.
     */
    String category() default "";

    /**
     * The scope of this stats.
     *
     * @return scope of this stats
     */
    String scope() default "";

    /**
     * The name of this stats
     *
     * @return name of this stats
     */
    String name();

    /**
     * The help message of this stats
     *
     * @return help message of this stats
     */
    String help();

    /**
     * The parent metric name.
     *
     * <p>It can used for analyzing the relationships
     * between the metrics, especially for the latency metrics.
     *
     * @return the parent metric name
     */
    default String parent() { return ""; }

    /**
     * The metric name of an operation that happens
     * after the operation of this metric.
     *
     * <p>similar as {@link #parent()}, it can be used for analyzing
     * the dependencies between metrics.
     *
     * @return the metric name of an operation that happens after the operation of this metric.
     */
    default String happensAfter() { return ""; }


}
```

The `StatsDoc` annotation provides a way to annotate metrics we added to bookkeeper.

- category: which category that the metric belongs to. e.g. server, or client.
- scope: the scope of the metric. e.g. `bookie` scope.
- name: the name of the metric.
- help: the description of the metric.

### Proposed Changes

In addition to the `StatsDoc` annotation, bookkeeper should provide a tool for generating the metrics yaml file
for documenting all annotated metrics.

```bash
bin/stats-doc-gen
```

Example output:

```yaml
"server":
  "bookie_BOOKIE_READ_ENTRY_BYTES":
    "description": |-
      bytes stats of ReadEntry on a bookie
    "type": |-
      OPSTATS
  "bookie_WRITE_BYTES":
    "description": |-
      total bytes written to a bookie
    "type": |-
      COUNTER
  "bookie_BOOKIE_ADD_ENTRY":
    "description": |-
      operations stats of AddEntry on a bookie
    "type": |-
      OPSTATS
  "bookie_BOOKIE_RECOVERY_ADD_ENTRY":
    "description": |-
      operation stats of RecoveryAddEntry on a bookie
    "type": |-
      OPSTATS
  "bookie_BOOKIE_ADD_ENTRY_BYTES":
    "description": |-
      bytes stats of AddEntry on a bookie
    "type": |-
      OPSTATS
  "bookie_BOOKIE_FORCE_LEDGER":
    "description": |-
      total force operations occurred on a bookie
    "type": |-
      COUNTER
  "bookie_READ_BYTES":
    "description": |-
      total bytes read from a bookie
    "type": |-
      COUNTER
  "bookie_BOOKIE_READ_ENTRY":
    "description": |-
      operation stats of ReadEntry on a bookie
    "type": |-
      OPSTATS
```

### Compatibility, Deprecation, and Migration Plan

It is a new feature, which doesn't have any compatibility impacts.

There is nothing deprecated.

There is nothing to migrate.

### Test Plan

Existing testing is good enough to cover code changes. No new tests are needed.

### Rejected Alternatives

Alternatively, we have to manually maintain the metrics page and update each time when a new metric is added.
