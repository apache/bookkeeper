---
title: "BP-45: Add interceptor interface allowing modifications of the payload"
issue: https://github.com/apache/bookkeeper/issues/2731
state: Under Discussion
release: "4.15.0"
---

### Motivation

The proposed change targets addition of a pluggable way to modify payload sent to the ledger. 
Specific use cases include GDPR compliance, encryption, and compression. 
While these can be implemented at the application level, having this as a BK client level 
extension unlocks such functionality to any application built on top of Bookkeeper.

Let’s dig deeper into specific use cases: 

#### GDPR

Ledger's data on the bookie servers is stored in the immutable EntryLog files. The files, 
in the most common case, mix the data from multiple ledgers. There is no guarantee of immediate 
erasure from the disk upon the ledger deletion, the entry logs are "compacted" with some delay. 
The amount of the delay is not guaranteed and, in an extreme case, can be infinite if the data 
from a deleted small short-lived ledger gets mixed in the entry log with data from the long-lived 
ledgers. 

Such behavior of compaction is a trade-off between performance and disk space utilization. 

The data is also stored in the journal file and, in an extreme case 
(low TTL for deletion and low traffic volume), can be recoverable from the journal after the 
ledger deletion.

Modern global businesses are affected by privacy laws [1] and obligations, the most notable 
of which is "Art. 17 GDPR: Right to be forgotten" [2]. Privacy policies require setting deadlines 
after which the data cannot be used.

"Forgotten" encryption keys are an acceptable alternative to the actual deletion of the data. 
Encrypting the data with a ledger-specific key that is stored with ledger metadata and 
automatically deleted with the ledger is a sufficient option.

In proposed implementation, this will work as outlined below:


* On ledger creation, the interceptor generates a random key of configurable length for use 
with a configurable encryption method. The key and encryption method added into the ledger’s 
custom metadata
* On entry add, interceptor gets the key and encryption method, encrypts the data and replaces 
the original payload; Mac Digest and the rest of the processing happens as usual, without any 
changes on the bookie side/in the protocol
* Bookies receive the encrypted data
* On read, interceptor decrypts the data (using the key etc from the ledger’s custom metadata) 
and replaces the payload.
* On ledger delete, the metadata is deleted and the key is lost.


#### Encryption

Security-conscious organizations (financial institutions etc.) may have requirements to encrypt the data with the securely stored key.
This can be implemented similarly to the GDPR case but the key requested from the third party key management system.

#### Compression


Similar to GDPR/encryption minus the need to deal with the keys.


### Public Interfaces


```
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

/**
 * Interface for the interceptors that may need to modify
 * data written to the ledger.
 */
public interface LedgerPayloadInterceptor {

    /**
     * Called after interceptor creation (once)
     * @param ctx - ClientContext
     * @param interceptorsCfg - configuration prefixed by "interceptor.lpi"
     *                        (without the prefix for keys)
     */
    void init(final ClientContext ctx, final Configuration interceptorsCfg);

    /**
     * Executed before creating the ledger.
     * Gives opportunity to modify ledger's custom metadata.
     * Modified metadata will be persisted.
     * @param customMetadata - ledger's custom metadata (can be null or ImmutableMap)
     * @return modified metadata or the original metadata
     */
    Map<String, byte[]> beforeCreate(final Map<String, byte[]> customMetadata);

    /**
     * Executed before adding entry to the ledger.
     * Gives opportunity to modify the payload.
     * @param customMetadata - ledger's custom metadata
     * @param payload - data being added to the ledger
     * @return modified payload or the original payload
     */
    ByteBuf beforeAdd(final Map<String, byte[]> customMetadata, final ByteBuf payload);

    /**
     * Executed after receiving the payload from the bookie.
     * Gives opportunity to modify the payload.
     * @param customMetadata - ledger's custom metadata
     * @param payload - data read from the bookie
     * @return modified payload or the original payload
     */
    ByteBuf afterRead(final Map<String, byte[]> customMetadata, final ByteBuf payload);

    /**
     * called after the client is closed / interceptor is no longer usable.
     */
    void close();
}

```

### Proposed Changes

* Add an interface that allows modification of the payload: LedgerPayloadInterceptor is outlined above
* Configuration option to enable the interceptors, like
payloadInterceptor=org.apache.bookkeeper.CompressingPayloadInterceptor,org.apache.bookkeeper.EncryptingPayloadInterceptor
* configuration options to pass to interceptor (payloadInterceptor.<option name>=<value>)
* Client config to return the instances of configured interceptors
* Change LedgerCreateOp/PendingAddOp/PendingReadOp to call the interceptor (it is easier to avoid extra data copy there AFAICT)
* Change ClientContext to pass the interceptor to the <LC/PA/PR>Ops.
* Implement simple EncryptingLedgerPayloadInterceptor

All the changes affect the client only. 

### Compatibility, Deprecation, and Migration Plan

- What impact (if any) will there be on existing users? 

Disabled by default

- If we are changing behavior how will we phase out the older behavior? 

No change to the existing behavior.

### Test Plan

* existing unit tests
* additional unit tests for configuration/use of noop implementations of LedgerPayloadInterceptor
* Implementation of data encrypting LedgerPayloadInterceptor, unit tests, perf tests. 
* existing unit tests with data encrypting LedgerPayloadInterceptor on/off

### Rejected Alternatives

#### Tune/modify major compaction to compact everything (potentially track time-since-deletion 
   or something along these lines) to satisfy GDPR requirements. 
   
Rejected due to performance impact.

#### Encrypt the disks. 
   
Does not cover the GDPR requirements as anyone with permission to access the disks can 
potentially read the data/use tools to reconstruct the entries.

#### Encrypt data before writing it to the bookie. 
   
This might work in some cases when the bookkeeper is used directly. 

The specific problem for the GDPR case is the security key deletion.
It is not easy if it is used via i.e. Apache Pulsar, where end user does not have control over the ledger deletion 
and need to either find a way to track which keys can be deleted (and coordinate this process). 
It is also tricky if the user relies on the State Storage [3] to store state.

### References

[1] https://en.wikipedia.org/wiki/General_Data_Protection_Regulation

[2] https://gdpr-info.eu/art-17-gdpr/

[3] https://pulsar.apache.org/docs/en/functions-develop/#state-storage
