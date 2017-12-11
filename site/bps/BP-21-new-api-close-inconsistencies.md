---
title: "BP-21: New API close inconsistencies"
issue: https://github.com/apache/bookkeeper/issues/789
state: "Rejected"
release: "4.6.0"
---

Rejected due to lack of agreement that the issues raised in the motivation are valid.

### Motivation

The [Handle](http://bookkeeper.apache.org/docs/latest/api/javadoc/org/apache/bookkeeper/client/api/Handle.html) interface provides two methods, #asyncClose and #close (overriding AutoCloseable). 

#close is implemented in both [ReadHandle](http://bookkeeper.apache.org/docs/latest/api/javadoc/org/apache/bookkeeper/client/api/ReadHandle.html) and [WriteHandle](http://bookkeeper.apache.org/docs/latest/api/javadoc/org/apache/bookkeeper/client/api/WriteHandle.html). 

1. The implementations in ReadHandle and WriteHandle do vastly different things. In ReadHandle, #close unregisters listeners from the ledger manager. This is local resource cleanup, which is in line with what AutoCloseable is designed for. In WriteHandle, #close calls #asyncClose which writes the lastAddConfirmed to the LedgerMetadata. This violates the principle of separation of concerns, and overloads the meaning of the term "close".

2. #asyncClose is defined in Handle, but it only has any meaning in the WriteHandle. In ReadHandle, closing only cleans up local resources, there's no network nor disk I/O involved. The implementation directly calls the callback. It's only in WriteHandle that asyncClose has any meaning, and here it is completely different to in ReadHandle.

3. The name #asyncClose is inconsistent with every other method on the new api Handles (append, read, readLastAddConfirmed, etc).

4. #close is part of AutoClosable, so its not unreasonable for it to be used in a try-with-resource block. This means that a ledger closure (i.e. distributed state mutation) could be triggered by an exception within the block. This is nasty.

Overloading the meaning of the term "close" is very problematic on its own. Closing a WriteHandle is a very important part of the BookKeeper protocol, so it should at least have it's own _verb_. I propose that we stop using "closing a ledger" to describe setting the last entry of a ledger, and instead call it "sealing a ledger".

### Public Interfaces

1. Remove Handle#asyncClose
2. Add new method WriteHandle#seal.
```
class WriteHandle {
    CompletableFuture<Void> seal();
}
```

### Proposed Changes

The proposed change remove asyncClose from all handles and replaces it with a async #seal method on WriteHandle. WriteHandle will still have a #close method for cleaning up local resources. 

The proposed usage would look like:

```
try (WriteHandle writer = bk.newCreateLedgerOp().withPassword("bleh".getBytes()).execute().get()) {
    for (int i = 0; i < 100; i++) {
        writer.append("foobar".getBytes());
    }
    writer.seal().get(); // no more entries can be added
}
```

_What if the user forgets to call #seal before closing the ledger?_

The ledger is left unsealed. Readers will not read past the end of the unsealed ledger, and they will either try to recover the ledger or wait forever. In both cases, the consistency of the data is guaranteed as the writer would only acknowledge writes which have hit the full ack quorum, which will always be picked up by recovery. If the writer had made any writes that were not acknowledged, it would have halted and not moved onto writing a new ledger.

### Compatibility, Deprecation, and Migration Plan

None, this only affects the new api.

### Test Plan

The current tests for #asyncClose will be migrated to use #seal().

### Rejected Alternatives

The alternative is how it is now. The movitation section describes the problem with this.
