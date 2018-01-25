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

## Tutorials

### Distributed Counters

#### Create Stream

Stream is the management unit. A `Table` is a materialized view of a `Stream`.

```
bin/streamstorage-cli -s 127.0.0.1:4181 stream create --stream test_dist_counter

```

#### Listen on a key

```
bin/streamstorage-cli -s 127.0.0.1:4181 table get -t test_dist_counter -k "counter-1" --watch
```

#### Mutable the table using a table writer

A `TableWriter` is mutating a table by appending updates to its stream. For example, if you
keep appending `incr(100)` updates, you will see the counter value keep incrementing.

```
bin/streamstorage-cli -s 127.0.0.1:4181 table append -t test_dist_counter -k "counter-1" -a 100 --times 1000 -s 1000
```

### K/V Store

Use the stream store as the normal k/v store.

#### Create Stream

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

- [ ] Support PTable & Table
    - PTable: short for `partitioned table`. the table is modeled as `<pKey, lKey> -> value`, kv pairs are partitioned based on `pKey`. range operations over a single `pKey` is supported.
        - put/get/delete on single `<pKey, lKey>`
        - range/deleteRange on a single `<pKey>`
        - txn on a single `<pKey>`
        - increment on single `<pKey, lKey>`
    - Table: the table is modeled as `<key> -> value`. kv pairs are also partitioned, based on `key`. range operations are not supported. single key txn (aka cas operation) is supported.
- [ ] Support PTableWriter & TableWriter
    - `Writer` is a table mutator that mutate a table by appending updates to the journal streams of the tables. `Writer` is useful for aggregation (e.g. distributed counters).
- [ ] Persistence
    - The source-of-truth of a table is its journal stream.
    - Rocksdb as its materialized index for each range partition.
    - Incrementally checkpointing the materialized rocksdb to bookkeeper for faster recovery.
- [ ] gRPC based
