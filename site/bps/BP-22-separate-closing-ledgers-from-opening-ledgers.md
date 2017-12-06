---
title: "BP-22: Separate closing ledgers from opening ledgers"
issue: https://github.com/apache/bookkeeper/794
state: "Rejected"
release: "4.7.0"
---

Rejected due to lack of agreement that the issues raised in the motivation are valid.

### Motivation

In the beginning there was openLedger. Opening a ledger meant also figuring out what the last entry of a ledger should be, and writing it to ZooKeeper. For a long time this was the only way to read a ledger. If a writer was writing to the ledger, then anything it wrote after this point would be lost (fencing was added later). The open operation was the natural place to put recovery, as at this point it was only possible to read a recovered ledger.

openLedgerNoRecovery was added in 2011. This allowed users to read from a ledger as it was being written to, which opened up a bunch of tailing uses cases. Recovery was still the default, because that was what it had always been, and tailing was still considered a secondary usecase. If the user wanted to skip recovery, they'd have to explicitly call the no recovery method.

Then the new API arrived. In the new API, tailing is the primary read use case, and recovery has been demoted to a boolean flag on a builder for the open operation. The user is for the most part unaware of recovery.

However, recovery is still one of the most important aspects of BookKeeper. It is the mechanism on which our Total Order Atomic Broadcast guarantees are built. It deserves to be a bit more prominent than a boolean flag in a builder. It also doesn't help that the terminology is inconsistent. The flag is called withRecovery, while the to check if recovery is needed, we call isClosed. Closed itself is ambiguous because it may refer to the local handle, or it may refer to the state of the ledger.

As tailing is now the primary usecase, we expect that if a writer fails, then whichever node takes over as writer already has a non-recovered ReadHandle open. It would be nice to be able to continue using this Handle to read to the end of the ledger.

### Public Interfaces

I proposes the removal of OpenOpBuilder#withRecovery()

A new method on BookKeeper:
```
interface BookKeeper {

    ...

    CompletableFuture<Void> seal(ReadHandle handle);
}
```

### Proposed Changes

With the proposed interface changes, every ReadHandle will be opened without recovery. For the tailing usecase, usage will look like.

```java
ReadHandle reader = bk.newOpenLedgerOp().withLedgerId(X).execute().get();
long lastReadEntry = -1;
while (!leader) {
    long lac = reader.getLastAddConfirmed();
    if (lac > lastReadEntry) {
        LedgerEntries entries = reader.read(lastReadEntry+1, lac).get();
        doSomethingWithEntries(entries);
        lastReadEntry = lac;
    }
}
assert (leader);
bk.seal(reader).get();
long lac = reader.readLastAddConfirmed().get();
if (lac > lastReadEntry) {
    LedgerEntries entries = reader.read(lastReadEntry+1, lac).get();
    doSomethingWithEntries(entries);
}
WriteHandle writer = bk.newCreateLedgerOp().execute().get();
```

Constrast this with how it is with the current recovery on open mechanism.

```
ReadHandle reader = bk.newOpenLedgerOp().withLedgerId(X).execute().get();
long lastReadEntry = -1;
while (!leader) {
    long lac = reader.getLastAddConfirmed();
    if (lac > lastReadEntry) {
        LedgerEntries entries = reader.read(lastReadEntry+1, lac).get();
        doSomethingWithEntries(entries);
        lastReadEntry = lac;
    }
}
assert (leader);
reader.close();
reader = bk.newOpenLedgerOp().withLedgerId(reader.getId()).withRecovery(true).execute.get();
long lac = reader.readLastAddConfirmed().get();
if (lac > lastReadEntry) {
    LedgerEntries entries = reader.read(lastReadEntry+1, lac).get();
    doSomethingWithEntries(entries);
}
WriteHandle writer = bk.newCreateLedgerOp().execute().get();
```

The second one is more code, you need to remember to close the previous handle, and the intent of the operation is less clear.

### Compatibility, Deprecation, and Migration Plan

This change is only on the new API, so there's no promise of compatibility.

### Test Plan

This change replaces #withRecovery() with #seal(), so anyplace withRecovery was tested, should be replaced with #seal().

### Rejected Alternatives

- ReadHandle#seal: Rejected as ReadHandle should be side effect free
- OpenOpBuilder#withRecovery(true): Rejected as we want tailing to be default usecase.
- ReadHandle#forceClosed() or BookKeeper#forceClosed(ReadHandle): Rejected as unclear what the state of handle would be after (has ReadHandle#close been called).
