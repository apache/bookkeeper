# BookKeeper Table Service

BookKeeper Table Service is a contrib module added to BookKeeper, providing a table (aka key/value) service as part of the stream storage for bookkeeper.

## Detail Design

[BP-30](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f)

## Build

```
$ mvn clean install -DskipTests
```

## Start Standalone

```
$ bin/streamstorage standalone
```

## CLI Usage

CLI is available at `bin/streamstorage-cli`

## Examples

### Distributed Counters

#### Create Table

Stream is the management unit. A `Table` is a materialized view of a `Stream`.

```
bin/streamstorage-cli -s 127.0.0.1:4181 stream create --stream test_dist_counter

```

#### Listen on a key

```
bin/streamstorage-cli -s 127.0.0.1:4181 table get -t test_dist_counter -k "counter-1" --watch
```

#### Increment a counter

```
bin/streamstorage-cli -s 127.0.0.1:4181 table incr -t test_dist_counter -k "counter-1" -a 100
```

### K/V Store

Use the table service as the normal k/v store for storing metadata

#### Create Table

```
bin/streamstorage-cli -s 127.0.0.1:4181 stream create --stream test_kv_store
```

#### Listen on a key

```
bin/streamstorage-cli -s 127.0.0.1:4181 table get -t test_kv_store -k "test-key" --watch
```

#### Put Key/Value

```
bin/streamstorage-cli -s 127.0.0.1:4181 table put -t test_kv_store -k "test-key" -v "test-value-`date`"
```

#### Increment

```
bin/streamstorage-cli -s 127.0.0.1:4181 table get -t test_kv_store -k "test-counter-key" --watch
```

```
bin/streamstorage-cli -s 127.0.0.1:4181 table incr -t test_kv_store -k "test-counter-key" -a 200
```

## Features

- [x] API Model: support PTable & Table
    - [x] PTable: short for `partitioned table`. the table is modeled as `<pKey, lKey> -> value`, kv pairs are partitioned based on `pKey`. range operations over a single `pKey` is supported.
        - [x] put/get/delete on single `<pKey, lKey>`
        - [x] range/deleteRange on a single `<pKey>`
        - [x] txn on a single `<pKey>`
        - [x] increment on single `<pKey, lKey>`
    - [x] Table: the table is modeled as `<key> -> value`. kv pairs are also partitioned, based on `key`. range operations are not supported. single key txn (aka cas operation) is supported.
- [x] Persistence
    - [x] The source-of-truth of a table is its journals. The journal is a stream comprised of multiple log segments (aka ledgers).
    - [x] Rocksdb as its materialized index for each range partition. Rocksdb can be ephemeral and it can be restored from checkpoints that are also persisted as log streams.
    - [x] Rocksdb can spill in-memory data to disk. Additionally, the rocksdb files are periodically incrementally checkpointed to bookkeeper for fast recovery.
- [ ] Clients
    - [x] gRPC & protobuf based
    - [x] Thick client with client-side request routing
    - [x] Java implementation
    - [ ] Thin client with server-side request routing
    - [ ] Multiple language clients: C++, Python, Go
- [ ] Deployment
    - [ ] Run table service as a bookkeeper lifecycle component in bookie server

## Later Improvements

- [ ] Stream <-> Table exchangeable: (for metadata store)
    - [ ] Can retrieve a Stream of updates from a key, a range of keys or a Table.
    - [ ] A Stream can be materialized and viewed as a Table.
- [ ] TTL or Lease: for supporting membership (for metadata store)
- [ ] Auto Scale: split and merge ranges. should apply for both Stream and Table.
