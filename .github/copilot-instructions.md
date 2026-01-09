# Apache BookKeeper - AI Coding Agent Instructions

**Last Updated:** January 9, 2026

## Project Overview

Apache BookKeeper is a **distributed, fault-tolerant, append-only storage service** optimized for high-throughput, low-latency workloads. It serves as WAL, message store, and offset store for systems like Apache Pulsar and HDFS NameNode.

**Version:** 4.18.0-SNAPSHOT | **Build:** Maven 3.8+ | **Java:** 11+ LTS

---

## Architecture & Components

### Core Module Structure

```
bookkeeper-server/     → Core storage engine, ledger management, bookie operations
bookkeeper-client/     → Client libraries for BookKeeper API
stream/                → Newer stream storage API (gRPC-based table service)
bookkeeper-proto/      → Protocol Buffer definitions for RPC
bookkeeper-http/       → HTTP server implementations (Servlet, Vert.x)
metadata-drivers/      → Backend implementations (ZooKeeper, Etcd)
bookkeeper-dist/       → Distribution packaging
tests/                 → Integration and cross-component tests
```

### Data Flow Pattern

1. **Clients → BookKeeper Protocol** (Netty-based RPC)
2. **Ledger Manager** (orchestrates writes across bookies)
3. **Individual Bookies** (store entries in ledgers)
4. **Metadata Store** (ZooKeeper/Etcd tracks ledger state)
5. **Auto-Recovery** (Auditor/ReplicationWorker monitors failures)

### Critical Design Decisions

- **Ledger-based model**: Data organized in immutable ledgers with entries
- **Bookie replication**: Write-ahead-logging across multiple bookies for durability
- **Eventual consistency metadata**: ZooKeeper/Etcd used for coordination, not critical path
- **Pluggable backends**: Multiple metadata stores and journal implementations supported

---

## Development Workflows

### Build Commands

```bash
# Full build with tests
mvn clean install

# Build without tests (faster iteration)
mvn clean install -DskipTests

# Build specific module
mvn clean install -pl bookkeeper-server -DskipTests

# Run tests only
mvn clean test

# Run specific test class
mvn test -Dtest=ExponentialBackoffRetryPolicyTest
```

### Testing Patterns (Hybrid Approach)

**Three test styles are used in this project:**

1. **Mock/Stub Tests** - Isolation via `@Mock`, stubbing with `when().thenReturn()`
   - Use for: Dependency isolation, verifying interactions
   - Example: [ExponentialBackoffRetryPolicyMockStubTest.java](bookkeeper-server/src/test/java/org/apache/bookkeeper/zookeeper/)

2. **Parameterized Tests** - `@ParameterizedTest` with `@CsvSource` for multi-scenario coverage
   - Use for: Boundary conditions, multiple input combinations
   - Reduces code duplication significantly

3. **Nested Test Classes** - `@Nested` groups for organized, readable test structures
   - Use for: Complex scenarios organized by context
   - Improves maintainability at scale

**Key Testing Frameworks:**
- **JUnit 5** (Jupiter API) - `@Test`, `@ParameterizedTest`, `@Nested`, `@DisplayName`
- **Mockito 5.2** - Mocking and verification
- **Hamcrest 2.2** - Fluent assertions with `assertThat()`
- **JaCoCo** - Code coverage analysis
- **PITest** - Mutation testing

### Coverage & Analysis

```bash
# Generate JaCoCo coverage report
mvn jacoco:report

# View report at: target/site/jacoco/index.html

# Run PITest mutation testing
mvn org.pitest:pitest-maven:mutationCoverage
```

---

## Project-Specific Conventions

### Testing Conventions

**Control-Flow Labeling** - Use [CF-N] comments to document coverage:
```java
void testRetryScenario() {
    // [CF-1] Test when allowRetry returns true
    when(policy.allowRetry()).thenReturn(true);
    assertThat(backoffTime).isBetween(100, 200);
}
```

**Assertion Style** - Prefer Hamcrest `assertThat()` over JUnit assertions:
```java
// ✓ Good: Fluent, readable
assertThat(result).isEqualTo(expected);
assertThat(values).isNotEmpty().hasSizeGreaterThan(0);

// ✗ Avoid: Less fluent
assertEquals(expected, result);
assertTrue(values.size() > 0);
```

### Code Patterns

**Ledger API Pattern** - Common in `bookkeeper-server`:
```java
long ledgerId = client.createLedger();
LedgerHandle handle = client.openLedger(ledgerId);
handle.addEntry(data);
handle.close();
```

**Metadata Store Pattern** - Abstract interface with multiple implementations:
```java
MetadataDriver driver = MetadataDriverFactory.newMetadataDriver(...)
LedgerMetadata metadata = driver.getLedgerMetadata(ledgerId);
```

**Bookie Storage Pattern** - Interface-based with pluggable journal/ledger stores:
```java
Journal journal = journalManager.newJournal();
LedgerStorage ledgerStorage = ledgerStorageFactory.newLedgerStorage();
```

---

## Critical Developer Tasks

### Adding a New Test Class

1. **Placement**: `bookkeeper-server/src/test/java/org/apache/bookkeeper/{module}/`
2. **Dependencies**: Add `@Mock` fields for external dependencies
3. **Structure**: Use `@Nested` classes for test organization
4. **Naming**: `{ClassName}Test`, `{ClassName}ManagedTest`, or `{ClassName}ControlFlowTest`

### Debugging Tests

```bash
# Run test with verbose output
mvn test -Dtest=ExponentialBackoffRetryPolicyTest -X

# Debug mode (attach debugger to port 5005)
mvn -Dmaven.surefire.debug test

# Filter by test method pattern
mvn test -Dtest=ExponentialBackoffRetryPolicyTest#testAllowRetry*
```

### Integration Points

- **ZooKeeper interaction**: Tests use `ServerConfiguration` mock or embedded ZooKeeper
- **Netty networking**: Mock channel interactions; avoid real sockets in unit tests
- **Ledger operations**: Use `LedgerHandle` abstractions; mock for unit tests
- **Metrics**: Use `StatsLogger` injections; can be mocked or no-op

---

## Cross-Component Patterns

### Server-Client Communication

- **Protocol**: Bookkeeper Wire Protocol (Netty)
- **Async pattern**: CompletableFuture-based callbacks
- **Timeout handling**: Exponential backoff retry policy (see [ExponentialBackoffRetryPolicySuite])

### Auto-Recovery Workflow

1. **Auditor** (singleton): Monitors ZooKeeper for failed bookies
2. **Detects failures**: Watches ledger replication info
3. **Creates rereplication tasks**: Pushes to queue for workers
4. **ReplicationWorker** (per bookie): Processes rereplication entries

### Stream Storage (Newer API)

- **gRPC-based**: Modern alternative to legacy Bookkeeper Protocol
- **Table abstraction**: Materialized views over streams
- **RocksDB backend**: In-memory index with persistence to BookKeeper ledgers

---

## Common Pitfalls & Solutions

| Issue | Solution |
|-------|----------|
| Tests fail with ZooKeeper issues | Mock ZooKeeper with `ServerConfiguration` mock or use embedded ZooKeeper in `@BeforeEach` |
| Flaky timeout tests | Use `@Timeout` annotation; increase for slow CI; avoid real delays in unit tests |
| Mock verification fails | Ensure `when()` stubs are set BEFORE method calls; use `ArgumentCaptor` for complex args |
| Coverage gaps on edge cases | Use `@ParameterizedTest` to test boundary values: `0, 1, MAX, -1` |
| Ledger handle not closed | Use try-with-resources or `@AfterEach` cleanup |

---

## Integration Points & Dependencies

### External Systems

- **ZooKeeper/Etcd**: Metadata store (pluggable via `MetadataDriver`)
- **RocksDB**: Persistent key-value index for stream storage
- **Netty**: Network I/O and protocol handling
- **Protobuf**: RPC message serialization

### Maven Modules to Reference

- `bookkeeper-proto`: Protobuf message definitions
- `bookkeeper-common`: Shared utilities (pooling, backpressure)
- `stats`: Metrics collection framework
- `metadata-drivers`: ZooKeeper/Etcd abstractions

---

## File Navigation Quick Ref

- **Core ledger logic**: `bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/`
- **Client API**: `bookkeeper-server/src/main/java/org/apache/bookkeeper/client/`
- **Test utilities**: `testtools/`, `tests-common/`
- **Configuration**: `conf/` (bk_server.conf, zookeeper.conf)
- **Build tools**: `buildtools/` (Checksum validation, Protocol utilities)

---

## Quick Wins for Productivity

✅ **Read main README.md** - Understand "append-only workloads" thesis  
✅ **Start with a test** - Use hybrid Mock/Stub + Parameterized approach  
✅ **Use the demo module** - `bookkeeper-tests-demo/` has working examples  
✅ **Check CONTRIBUTING.md** - Review-then-commit workflow, coding style guides  
✅ **Explore existing tests** - Browse `*Test.java` files in `bookkeeper-server/src/test/`  

---

**Note:** This codebase prioritizes **reliability and fault tolerance** over simplicity. Expect complexity in threading, timeout handling, and failure scenarios. Test thoroughly; use mocks to isolate concerns.
