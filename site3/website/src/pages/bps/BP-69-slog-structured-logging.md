# BP-69: Adopt slog for structured logging

- Issue: [#4750](https://github.com/apache/bookkeeper/issues/4750)
- State: Under Discussion
- Release: N/A

### Motivation

BookKeeper today uses SLF4J across the codebase. Logs are unstructured lines with
positional `{}` placeholders, and the same identity attributes (ledger id, bookie
id, entry id, journal id, etc.) are repeated inline in every message string. This
has three concrete consequences:

1. **Poor queryability in log backends.** Modern log pipelines (Loki, Elastic,
   Splunk, OpenSearch, GCP Logging, CloudWatch) are indexed on structured
   key/value attributes. Grepping `"ledgerId=12345"` out of free-form messages is
   slow, lossy, and breaks whenever a log message is reworded.
2. **Boilerplate and inconsistency.** Every log statement restates the same
   identity context ("ledger 12345, entry 67, bookie 10.0.0.1:3181 ..."), with
   no consistent attribute naming. Two log sites that mean the same thing get
   formatted differently.
3. **`isDebugEnabled()` boilerplate.** To avoid `String.format`/toString cost on
   hot paths with debug disabled, the code is sprinkled with explicit level
   guards (hundreds of them), which obscure the actual logic.

A similar analysis was done for Apache Pulsar in
[PIP-467: Adopt slog for structured logging across Pulsar][pip-467]. PIP-467
reached the same conclusion, selected the `slog` library, was accepted, and the
migration of the entire Pulsar codebase is complete. Because Pulsar is
BookKeeper's largest user, a shared structured logging substrate lets
cross-project debugging chains stay structured and correlated end-to-end (more
on this in the "Cross-boundary logger context" section below, which is the main
API change this BP introduces for BookKeeper).

BookKeeper itself has a prior attempt at structured logging, the
`bookkeeper-slogger` module (`Slogger` API with `kv(key, value)` and `ctx()`).
It was introduced alongside `DbLedgerStorage`/`DirectEntryLogger` but was never
adopted broadly: only **5 main-source files** in `bookkeeper-server` use it today
(`DbLedgerStorage`, `DirectEntryLogger`, `DirectCompactionEntryLog`,
`DirectWriter`, `EntryLogIdsImpl`). The API is clunky (no debug/trace levels,
verbose `.kv().kv().info(...)` chains, no zero-overhead disabled levels), and
the SLF4J bridge is the only handler implementation. It is effectively dead
code. We propose to retire it.

### Public Interfaces

**New public interfaces** (`@Public`/`@Unstable`):

1. A `Map<String, Object>` parameter on the client-side builder API so the
   caller (Pulsar, a user application, etc.) can attach its own logging
   context attributes to a ledger `Handle`:
   - `CreateBuilder withLoggerContext(Map<String, Object> attrs)`
   - `OpenBuilder withLoggerContext(Map<String, Object> attrs)`
   - `DeleteBuilder withLoggerContext(Map<String, Object> attrs)`

   When set, the resulting `WriteHandle`/`ReadHandle` binds those attributes
   onto its internal logger. All BookKeeper client log lines emitted on behalf
   of that handle automatically carry the caller's attributes — e.g. Pulsar's
   `managedLedger=<topic>` and `cursor=<subscription>` — without BookKeeper
   knowing anything about Pulsar concepts. Note that the public API does
   **not** expose slog types: callers pass a plain `Map`, so they are not
   required to take a slog dependency to use the feature.

2. A new compile-scope dependency: `io.github.merlimat.slog:slog` (currently
   `0.9.7`).

**Changed public interfaces**: none. SLF4J remains on the classpath during and
after the migration as a transitive dependency, because slog ships an SLF4J
handler (`Slf4jHandler`) that sends structured records to any existing SLF4J
backend (Logback, Log4j2). Applications that currently configure BookKeeper
logging via `logback.xml` / `log4j2.xml` continue to work unchanged.

**Removed public interfaces**:

- The `bookkeeper-slogger` module (`org.apache.bookkeeper.slogger.Slogger`,
  `AbstractSlogger`, `Sloggable`, `ConsoleSlogger`, `NullSlogger`, and the
  `bookkeeper-slogger-slf4j` bridge). It is `@Unstable` and has no adoption
  outside the 5 internal files listed above.

**Not changed**:

- The BookKeeper wire protocol, binary formats (journal, entry log, ledger
  index), metadata formats, metrics, and CLI are untouched.
- MDC behavior (`MdcUtils`, MDC propagation in `OrderedExecutor`) is preserved;
  slog's SLF4J handler forwards structured attributes as MDC to the underlying
  SLF4J backend so that existing log appenders keep rendering them.

### Proposed Changes

#### 1. Add `slog` as a core dependency

- Add `io.github.merlimat.slog:slog:0.9.7` to root `pom.xml`
  `<dependencyManagement>` and to the global compile classpath, alongside the
  existing SLF4J API.
- Add `lombok.config` at the repo root so `@CustomLog` generates a slog
  `Logger`:
  ```
  lombok.log.custom.declaration = io.github.merlimat.slog.Logger \
      io.github.merlimat.slog.Logger.get(TYPE)
  ```
- Continue to ship the SLF4J handler at runtime so existing log configs (Logback
  / Log4j2 XML) keep working.

#### 2. Migrate SLF4J call sites to slog, one module at a time

Phased conversion. Each phase is a single commit; the codebase must compile at
the end of each phase.

| Phase | Scope                                                       |
|-------|-------------------------------------------------------------|
| 1     | `bookkeeper-common`                                         |
| 2     | `stats`, `bookkeeper-common-allocator`                      |
| 3     | `bookkeeper-server` (core, largest phase)                   |
| 4     | `bookkeeper-http`, `tools`                                  |
| 5     | `stream/distributedlog`                                     |
| 6     | `metadata-drivers`, `benchmark`, peripheral tests           |
| 7     | Cleanup: remove `bookkeeper-slogger`, remove `@Slf4j` usage |

Conversion rules:

- `@Slf4j` → `@CustomLog`; `LoggerFactory.getLogger(X.class)` →
  `Logger.get(X.class)`; naming: static = `LOG`, instance-bound = `log`.
- `log.info("text {} {}", a, b)` is **not** mechanically rewritten to `logf(...)`
  — instead convert parameters to structured attributes:
  `log.info().attr("a", a).attr("b", b).log("text")`.
- Classes with identity (ledger id, bookie id, stream name, ...) build an
  instance-bound logger in the constructor and drop those attributes from every
  call site:
  ```java
  this.log = Logger.get(PerChannelBookieClient.class)
          .with().attr("bookieId", bookieId).build();
  ```
- `if (log.isDebugEnabled())` guards are removed; slog returns a no-op `Event`
  for disabled levels so the `.attr()` chain is zero-overhead.
- When a `.attr()` value is itself expensive to compute (e.g.
  `Iterables.size(iter)`, `Joiner.on(...).join(...)`,
  `Arrays.toString(...)`), use slog's lambda form so the lambda is only invoked
  when the level is enabled:
  ```java
  log.debug(e -> e.attr("count", Iterables.size(newLocations))
                  .log("Update locations"));
  ```

#### 3. Cross-boundary logger context through the client API

This is the main API change. Today a caller (Pulsar, a user app, a CLI tool)
that wants to correlate its own log lines with BookKeeper's log lines has no
way to do so — BookKeeper's client code logs with its own loggers, and the
caller's context (topic name, cursor name, request id, trace id) never makes
it into those lines.

We add an optional `withLoggerContext(Map<String, Object>)` method on the
ledger builder API. The caller provides a plain map of attribute names to
values; slog does not appear in the public API surface:

```java
// Pulsar ManagedLedgerImpl
Map<String, Object> logCtx = Map.of(
        "managedLedger", name,
        "namespace", namespace);

CompletableFuture<WriteHandle> f = bookKeeper.newCreateLedgerOp()
        .withEnsembleSize(3)
        .withWriteQuorumSize(2)
        .withAckQuorumSize(2)
        .withLoggerContext(logCtx)    // <-- new
        .execute();
```

Inside BookKeeper, the `LedgerHandle` constructor binds those attributes onto
its internal logger:

```java
var builder = Logger.get(LedgerHandle.class).with();
if (callerAttrs != null) {
    callerAttrs.forEach(builder::attr);
}
this.log = builder.attr("ledgerId", ledgerId).build();
```

The net effect: a failed write on a bookie emits a log line like

```
level=ERROR ledgerId=12345 entryId=67 bookieId=bk-3:3181 \
    managedLedger=persistent://public/default/my-topic \
    "Write to bookie failed"
```

without any BookKeeper code referencing the `managedLedger` concept. The
propagation is one-way (caller context flows in; BookKeeper-only attributes do
not leak out), and `withLoggerContext` is optional — if unset, BookKeeper uses
its own `Logger.get(...)` as before and behavior is unchanged.

Scope for this BP: `CreateBuilder`, `OpenBuilder`, `DeleteBuilder`. The same
mechanism can be extended to `newListLedgersOp` / admin APIs in follow-up
changes.

#### 4. Retire `bookkeeper-slogger`

In the cleanup phase:

- Convert the 5 remaining `Slogger` call sites in
  `bookkeeper-server` to the new slog API (they are already structured-event
  loggers; the translation is mechanical).
- Delete the `bookkeeper-slogger/` module (API + SLF4J bridge + tests).
- Remove it from the root `pom.xml` reactor.

### Compatibility, Deprecation, and Migration Plan

- **End-user impact for the log output format**: with the default SLF4J
  handler, the rendered log lines continue to come from the user's existing
  Logback/Log4j2 pattern. Structured attributes are forwarded as MDC, so a
  layout like `%X{ledgerId} %msg` picks them up automatically. No log config
  change is required to upgrade.
- **End-user impact for the client API**: additive only. `withLoggerContext(...)`
  is a new method on `CreateBuilder` / `OpenBuilder` / `DeleteBuilder`; existing
  callers that don't use it see no behavioral change. Its parameter is a
  standard JDK `Map<String, Object>`, so callers are not required to take a
  slog dependency to use the feature.
- **Dependency impact**: applications get `io.github.merlimat.slog:slog` as a
  new transitive dependency. slog has no transitive dependencies of its own
  beyond an optional SLF4J handler (runtime scope).
- **`bookkeeper-slogger` removal**: `@Unstable`, 5 internal call sites, zero
  external adoption observed. We will deprecate in the release that begins the
  migration and remove after Phase 7 lands. Users who happen to depend on it
  can migrate to the published slog API, which is a superset.
- **SLF4J deprecation**: **not** part of this BP. SLF4J stays for the
  foreseeable future as the rendering backend behind slog.
- **Rollout**: each phase is independently mergeable and independently
  revertable. There is no flag day.

### Test Plan

- Each phase preserves the existing unit/integration test suite unchanged;
  "the tests still pass" is the baseline success criterion per phase.
- For the new `withLoggerContext(Map<String, Object>)` API, add unit tests
  that:
  1. Verify the log events emitted by a `LedgerHandle` carry the attributes
     supplied via the map (use a capturing slog handler in tests).
  2. Verify the default (no `withLoggerContext` call) behavior is identical
     to pre-BP, including logger name.
- For log-format compatibility, a small integration check in
  `bookkeeper-server` that wires the slog SLF4J handler to Logback and asserts
  that MDC-styled patterns render structured attributes.
- No wire/format/metric tests change — none of those are touched.

### Rejected Alternatives

1. **Stay on SLF4J, adopt a convention.** Rejected: SLF4J's `{}` format has no
   attribute names, no composable context (beyond MDC, which is thread-local
   and brittle across async/executor boundaries), and no zero-overhead disabled
   levels. Cross-project correlation with Pulsar would not be possible in a
   structured way.
2. **Expand `bookkeeper-slogger` to cover the whole codebase.** Rejected: the
   API surface is small and missing critical features (`debug`/`trace`,
   lambda-deferred attribute evaluation, per-instance context via
   `with().build()`). It has no external users. Hardening it would mean
   reinventing slog while duplicating work with Pulsar's choice. Cheaper to
   retire it.
3. **Use Log4j2's `ThreadContext` / structured API directly.** Rejected: binds
   BookKeeper to a specific backend (we currently support Logback, Log4j2, and
   others via SLF4J). slog keeps the rendering backend pluggable.
4. **OpenTelemetry Logs Bridge API.** Rejected for this BP: OTel logs are still
   stabilizing, and binding the in-process logging API to an OTel SDK is a much
   larger commitment (and dependency footprint) than what is needed to solve
   the stated problem. Nothing in this BP precludes later adding an OTel
   handler to slog.
5. **Expose a slog `Logger` parameter on the builder (e.g.
   `withLogger(io.github.merlimat.slog.Logger)`) instead of a
   `Map<String, Object>`.** Rejected: it would force every BookKeeper client
   that wants to propagate logging context to take a direct compile dependency
   on slog and to construct a slog `Logger` before calling the client API. The
   map-of-attributes form keeps slog out of the public API surface while giving
   the caller everything they actually need (a bag of key/value pairs to bind
   to the handle's logger). Nothing prevents a caller that already uses slog
   from deriving the map from an existing logger's attributes.

[pip-467]: https://github.com/apache/pulsar/blob/master/pip/pip-467.md
