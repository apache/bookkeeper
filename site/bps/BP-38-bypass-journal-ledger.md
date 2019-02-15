---
title: "BP-38: bypass journal ledger"
issue: https://github.com/apache/bookkeeper/issues/1918
state: "Under Discussion"
release: "N/A"
---

### Motivation

To guarantee high durability, BK write journal before flush data to persistent device which will cause two write of data.
But we may not need this level of persistence under [scenarios](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-14+Relax+durability) which prefer weak durability guarantee.
This proposal is aimed at providing bypass journal ledger, this feature includes these parts work:
 - add new write flag `BYPASS_JOURNAL` to existing protocol
 - impl the newly write flag at the client side and server side

The usage will looks like this:
```java
WriteHandle writeHandle = newCreateLedgerOp()
                        .withEnsembleSize(3)
                        .withWriteQuorumSize(3)
                        .withAckQuorumSize(3)
                        .withPassword(PASSWORD)
                        .withWriteFlags(WriteFlag.BYPASS_JOURNAL)
                        .execute().get();
// append data as usual, but the lac won't advance
writeHandle.appendAsync(...);
// force to advance lac
writeHandle.force();
writeHandle.closeAsync();
``` 
The details of changes are listed in Section “[Proposed Changes](#proposed-changes)”.
 
### Public Interfaces

This feature will introduce new 'WRITE_FLAG' to enable single write storage impl.
Like `DEFERRED_SYNC`, when we construct `WriteHandle`, we can pass `BYPASS_JOURNAL`, then every `AddRequest` will carry this flag.
As for monitoring, we can add metrics to record the main time interval during the io path.
The detailed compatibility related stuff is in Section “[CDMP](#compatibility-deprecation-and-migration-plan)”.

### Proposed Changes

While the main approach is based on [`WRITE_FLAG` impl](https://github.com/apache/bookkeeper/pull/742),
the actual implementation is more tricky. In my opinion, there are three different solution:

1. Relax LAC protocol(rejected due to violate LAC protocol)

    Modify server side code mostly and don't change legerHandle's LAC advance logic, if the write flag is `BYPASS_JOURNAL`, after write to `LegerStorage`(the data maybe in the memTable, or the buffer of File, or the os cache),
bookie return result to the client directly.
This impl is like [disable syncData](https://github.com/apache/bookkeeper/issues/753), once all the replica fails, the BK cluster can't recovery from it.
Note, the user shall know the weak durability for his use case when using this impl.

2. Extend `Force` API

    Like `DEFERRED_SYNC`, the normal 'bypass-journal' operation don't advance LAC on the client.
Only advance LAC using `force` api. To support this new 'bypass-journal' option, we need theses changes:

    - Add force interface to `LedgerDescriptor` and LedgerStorage
    
        Used to flush the data on memTable to persistent device for specific ledger, and this make senses with "one entry logger per ledger" feature.
        For general SortedLedgerStorage, this "force" operation may has some overhead due to flush data too early.
    
    - Extend the force request
        
        If the force request contains bypass journal option, execute force method of `LedgerDescriptor`, which will call LedgerStorage's force interface to flush received entries to persistent device.

    In addition to these changes, we should consider who is responsible for the `force` execution? How often to schedule 'force'?
    To simply the design and give more choices to apps, these choices are up to user.

3. Add NonPersistentLAC semantic to existing LAC protocol(rejected due to infeasible and complex)

    To not violate LAC semantics like method 1, we can add `NonPersistentLAC` semantic, this includes these changes:

    - Add persistent callback to LedgerStorage

        Maintain non-persistent entry list and `maxPersistentEntryId` on `LedgerDescriptor`.
        
    - Wire protocol changes
    
        Add optional `maxPersistentEntryId` field to `AddResponse`. When the `AddRequest` has 'bypass-journal' option, the server get `maxPersistentEntryId` from `LedgerDescriptor` to construct response.

    - Client side changes
    
        Add `nonPersistentLAC` to WriteHandle. WriteHandle with 'bypass-journal' option updates the LAC using `maxPersistentEntryId`, and update `nonPersistentLAC` if receives enough ack.
        For normal WriteHandle, the `nonPersistentLAC` is set to null to indicate to keep original LAC advance logic. 

As we only introduce `BYPASS_JOURNAL` write flag to add request, it's better not change existing fencing logic. So fence operation shall falls into journal io path.

### Compatibility, Deprecation, and Migration Plan

As this new feature only add one ledger creating option and corresponding ledger implementation. It has no effect
on existing users. After this feature is implemented, user can use this function directly on new released version which includes this implementation.
When user use the latest client which has the `BYPASS_JOURNAL` write flag, but the bookie is old versions which can't do bypass-journal process,
 the bookie will reject this type write. Old client has no chance to use this new write flag, and the new bookie can handle as usual.

### Test Plan

- unit tests for newly introduced write_flag and extended force at client side and server side
- end-to-end tests for the new Protocol request/response
- compat tests (client with this new writeflags will not be able to write on old bookies)

### Rejected Alternatives

To avoid two write of data, we can write the data to one log abstraction in the bookie, either by bypassing journal or 
 bypassing entryLog. If we choose bypassing entryLog, then we will do lots of work to enable rich read on journal,
 which is more huge work compared with bypassing journal.


