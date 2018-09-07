---
title: "BP-34: 128 bits support"
issue: https://github.com/apache/bookkeeper/603
state: "Under Discussion"
design doc: https://docs.google.com/document/d/1cu54dNSV2ZrdWCi40LcyX8NxXGRCW0609T_ewmK9BWM
release: "4.9.0"
---

### Motivation

BookKeeper coordinates with a metadata store to generate a cluster wide `ledgerId`.
Currently this is a signed `64 bit` number (effectively 63 bits). This method works
great because we have a centralized metadata store for coordinating the id generation.
However this method may not scale as the cluster size and number of ledgers grow.

[Universally unique identifier - Wikipedia](https://en.wikipedia.org/wiki/Globally_unique_identifier)
is a preferred way to generate decentralized globally unique IDs and it takes `128 bits`.
This method can scale well as it doesn't need a centralized coordination. 

This BP proposes the changes for increasing ledger id from `63 bits` to `128 bits`.

### 128 bits

Since there is no native support for `128 bits` in both Java and
[Protobuf](https://github.com/google/protobuf/issues/2180), we have to break `128 bits`
into 2 `64 bits` numbers for representing the `128 bits` id:

- ledger-id-msb: the most significant 64 bits, bit 64 - 127
- ledger-id-lsb: the least significant 64 bits, bit 0 - 63

For backward compatibility, the `ledger-id-lsb` is the current `64 bits` ledger-id.
The `ledger-id-msb` will be added as a new field in both API and protocol. 

I am proposing calling `ledger-id-msb` as `ledger-scope-id`. So the current 64bits `ledgerId` and
the newly introduced 64bits `ledgerScopeId` together will be forming the new `128 bits` ledger id.

The default `ledgerScopeId` is `0`. That means any ledgers created prior to this change are allocated
under scope `0`. Hence it maintains backward compatibility during upgrade. 

The combination of `ledgerScopeId` and `ledgerId` forms the `128 bits` ledger id. We can introduce a
hex representation of this `128 bits` ledger id - `ledgerQualifiedName` . This `ledgerQualifiedName` can
be useful for CLI tooling, REST api and troubleshooting purpose. The API internally can convert
`ledgerQualifiedName` to `ledgerScopeId` and `ledgerId`.

### Public Interfaces

#### API Change

The API will be introducing `ledgerScopeId` across the interfaces. This field will be optional and default to `0`. 

##### Handle

Introduce a new method `getScopeId` for representing the scope id (the most significant  `128 bits` ledger id).

```java
public interface Handle extends AutoCloseable {

  ...

  /**
   * Return the ledger scope id. The most significant 64 bits of 128 bits.
   */
  long getScopeId();

  /**
   * Return the ledger id. The least significant 64 bits of 128 bits.
   */ 
  long getId();

  ...

}
```

##### Create LedgerAdv

Introduce a new method `withLedgerScopeId` in `CreateAdvBuilder` for providing `scopeId`
(the most significant 64 bits for 128 bits ledger id) on creating a ledger.

```java
public interface CreateAdvBuilder extends OpBuilder<WriteHandle> {
  ...

  /**
   * Set the scope id for the newly created ledger.
   * If no explicit scopeId is passed, the new ledger
   * will be created under scope `0`.
   */
  CreateAdvBuilder withLedgerScopeId(long scopeId);	

  ...
}
```

##### Open Ledger

Introduce a new method `withLedgerScopeId` in `OpenBuilder` for providing `scopeId`
(the most significant 64 bits for 128 bits ledger id) on opening a ledger.

```java
public interface OpenBuilder extends OpBuilder<ReadHandle> {
  ...
  /**
   * Set the scope id of the ledger to open.
   */
  OpenBuilder withLedgerScopeId(long scopeId);
  ...
}
```

##### Delete Ledger

Introduce a new method `withLedgerScopeId` in `DeleteBuilder` for providing `scopeId`
(the most significant 64 bits for 128 bits ledger id) on deleting a ledger.

```java
public interface DeleteBuilder extends OpBuilder<Void> {
  ...
  /**
   * Set the scope id of the ledger to delete.
   */
  DeleteBuilder withLedgerScopeId(long scopeId);
  ...
}
```

#### CLI

All BookKeeper CLI tools will be updated with additional option `—ledger-scope-id`.
Optionally we can add option `—ledger-qualified-name` (the hex representation of 128 bits).
Internally all the CLI tools will convert ledger qualified name to `ledgerId` and `ledgerScopeId`.

#### REST

1. All ledger related endpoints will be adding a new parameter `ledger_scope_id`. 
2. `ListLedgerService`  only supports listing ledgers under a given ledger scope id.
   If `ledger_scope_id` is missing, it will be listing ledgers under scope `0`.

#### Wire Protocol

> There will be no plan for supporting 128 bits in v2 protocol, due to the limitation in v2 protocol.
> So any operations in v2 protocol with scope id not equal to 0  will be failed immediately with
> `ILLEGAL_OP` exceptions.

All the request and response messages will be adding an optional field `optional int64 ledgerScopeId`.

#### Entry Format

Currently all the entries written to bookies are encoded in a certain format, including `metadata`,
`digest code` and `payload`. The entry format is not *versioned*.

In order to support adding another field `ledgerScopeId` in the `metadata` section, we are introducing
`version` in the entry format.

##### Entry Format V1

```json
Entry Format V1
===============
--- header ---
Bytes (0 - 7)                   : Ledger ID
Bytes (8 - 15)                  : Entry ID
Bytes (16 - 23)                 : LastAddConfirmed
Bytes (24 - 31)                 : Length
--- digest ---
Bytes (32 - (32 + x - 1))       : Digest Code (e.g. CRC32)
--- payload ---
Bytes ((32 + x) - )             : Payload
```

> `x` is the length of digest code.

>  Prior to introducing `ledgerScopeId`, ledgerId is assumed to be a positive value.

##### Entry Format V2

```json
Entry Format V2
===============
--- header ---
Bytes (0 - 7)                   : Metadata Flags
Bytes (8 - 15)                  : Ledger Scope ID
Bytes (16 - 23)                 : Ledger ID
Bytes (24 - 31)                 : Entry ID
Bytes (32 - 39)                 : LastAddConfirmed
Bytes (40 - 47)                 : Length
--- digest ---
Bytes (37 - (37 + x - 1))       : Digest Code (e.g. CRC32)
--- payload ---
Bytes ((37 + x) - )             : Payload
``` 

> `x` is the length of digest code.

###### Metadata Flags

```json
Metadata: 1 Bytes (Long)
------------------------
0x 0 0
   |__| 
     |
 version

----
Bit 0 - 3: digest type (e.g. CRC32, CRC32C and such)
Bit 4 - 7: version, the most significant bit of this byte will be always set to 1.
it will be used for differentiating entry format v1 and v2.

```

We are setting the most significant bit to be `1`. So the first byte in entry v2 will
be a negative value, which can be used for differentiating entry format v1 and v2.
The version will be encoded into the first byte. The version will be used for describing
the entry format.

##### Decoding Entry

The pseudo code for decoding an entry will be described as followings:

```java

ByteBuf entry = ...;

int metadataFlags = entry.getByte();

if (metadataFlags <= 128) { // the entry is encoded in v1 format
	// decoding the entry in v1 format
	...
} else {
	// decoding the entry in v2 format
}

```

#### Bookie Storage

##### Journal

A new method should be added in journal `WriteCallback` to handle `ledgerScopeId`.

```java
public interface WriteCallback {

    void writeComplete(int rc,
                       long ledgerScopeId,
                       long ledgerId,
                       long entryId,
                       BookieSocketAddress addr,
                       Object ctx);

    default void writeComplete(int rc,
                               long ledgerId,
                               long entryId,
                               BookieSocketAddress addr,
                               Object ctx) {
        writeComplete(rc, 0L, ledgerId, entryId, addr, ctx);
    }

}
```

The journal should be changed to be able to retrieve `ledgerScopeId` from the entry
payload based on [Entry Format](#entry-format).

##### Ledger Storage

###### EntryLogger

1. Methods in `EntryLogger` should be able to accept `ledgerScopeId` as a parameter.
2. EntryLogger should be updated to retrieve `ledgerScopeId` from the entry payload
   based on [Entry Format](#entry-format).

###### EntryMemTable

`ledgerScopeId` should be added as part of `EntryKey`.

###### IndexPersistenceMgr

Currently the ledger index files (64 bits) are stored into 2-level-hirechicy
directories - `<msb-32bits-hex>/<lsb-32bits-hex>/<ledger-id-hex>.idx`.

If `ledgerScopeId` is 0, it will be using existing scheme for storing and retrieving
ledger index files.

If `ledgerScopeId` is not 0, that means the ledgers are produced by new clients that
support 128-bits, those ledgers will be stored in a 4-level-hirechicy
directories -
`<msb-32bits-hex-ledger-scope-id>/<lsb-32bits-hex-ledger-scope-id>/<msb-32bits-hex-ledger-id>/<lsb-32bits-hex-ledger-id>`.

All the file info caches should be updated to use `<ledgerScopeId, ledgerId>`
as index keys.

###### IndexInMemPageMgr

The LRU pages map will be updated to use `<ledgerScopeId, ledgerId>` as index
keys.

###### DBLedgerStorage

Currently DBLedgerStorage use `<ledgerId, entryId>` as the index key for indexing entry
locations for each entry.

Similar as `SortedLedgerStorage` and `InterleavedLedgerStorage`, for ledgers whose
`ledgerScopeId` is 0, they will be using existing scheme for storing their entry locations.

For ledgers whose `ledgerScopeId` is not 0, they will be stored in a new rocksdb,
whose index key will be `<ledgerScopeId, ledgerId, entryId>`.

#### Metadata Store

##### LedgerManager

All the interfaces should be updated with accepting `ledgerScopeId`.

The actual implementation should decide how to store metadata
for `<ledgerScopeId, ledgerId>`. 

###### ZooKeeper Ledger Manager

We need to introduce a LongLongHierchicalLedgerManager for storing metadata
indexing by `<ledgerScopeId, ledgerId>`.

If `ledgerScopeId` is 0, then it will be falling back to `LongHierachicalLedgerManager`.
So no behavior is changed.

If `ledgerScopeId` is not 0, those ledgers will be indexed in new hierarchy
(possible under a different znode).

###### Ledger ID generation

When upgrading from 64bit to 128bits, we probably don't need any centralized mechanism
for generating ledger id. It can be implemented using UUID generation.

Especially since we are supporting 128bits by introducing `ledgerScopeId`. That means
application of bookkeeper can decide its own way for generating their `scopeId`.
An application or even bookkeeper client can generate its ledgerId using UUID generation,
then breaks the 128 bits UUID into two parts, one serves as `ledgerScopeId` and the other
one serves as `ledgerId`.

###### Etcd

Since Etcd has a better key/value presentation, we can basically just combine
`<ledgerScopeId, ledgerId>` as the index key for storing ledger metadata in Etcd.
Nothing is needed for special consideration.

### Performance Concerns

There shouldn't be any performance difference when not using 128 bit ledger id
(`ledgerScopeId` is omitted).

Performance concerns can be arised in following areas:

- **Wire Protocol**: additional 9 bytes will be added per entry, one byte for version
  and 8 bytes for the msb of 128 bit ledger id
- **Journal**: additional 9 bytes will be added per entry (same as wire protocol). 
- **EntryLogger**: additional 9 bytes will be added per entry (same as wire protocol)
- **Memtable**: additional 8 bytes will be added per indexed entry.
- **FileInfo**: there is no change to the index file format itself.
- **IndexPersistenceManager**: Files will be organized in more directory hierarchy.
  It shouldn't be a big deal. 
- **IndexInMemoryManager (LedgerCache)**: additional 8 bytes per index page.
- **DbLedgerStorage**: additional 8 bytes per entry for entry location.
- **Metadata**: on zookeeper, we need a 128 bit ledger manager, that means more znode
  hierarchy than 64 bit ledger manager. Etcd like key/value metadata store is probably
  more preferrable for 128 bit ledger manager.

However increasing ledger id from 64 bits to 128 bits can get rid of the only remaining
central point, since we don't need to use zookeeper for ledger id generation. The id
generation can become decentralized. 

### Proposed Changes

All the required changes are described above. In summary, the changes can
happen in following 2 phases:

1. Ensure all components have `ledgerScopeId` added (both wire protocol, storage and such).
   Assuming `ledgerScopeId` will be 0. The changes can happen independently and ensure
   they are backward compatible with old clients.
2. Add `ledgerScopeId` into public API, so application can start using `ledgerScopeId`.
   After that, applications can use UUID to generate ledger id and break UUID into two parts,
   one is `ledgerScopeId`, while the other one is `ledgerId`.

### Compatibility, Deprecation, and Migration Plan

All the changes are backward compatible, since we are doing the changes by adding an optional
field `ledgerScopeId`. Old clients can still operating in the mode of `ledgerScopeId == 0`.
The new application can activate the feature by starting using `ledgerScopeId` in the new API.

### Test Plan

1. Add unit tests for individual components on introducing `ledgerScopeId`.
2. Add backward compatibility tests for individual components.
3. Add end-to-end integration tests for introducing `ledgerScopeId`.
4. Add end-to-end backward compatibility tests.

### Rejected Alternatives

N/A
