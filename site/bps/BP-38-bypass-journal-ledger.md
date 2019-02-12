---
title: "BP-38: bypass journal ledger"
issue: https://github.com/apache/bookkeeper/issues/1918
state: "Under Discussion"
release: "N/A"
---

### Motivation

To guarantee high durability, BK write journal before flush data to persistent device which will cause two write of data.
At the presence of replicating and auto-recovery mechanism, the two-write is a bit waste of the persistent device bandwidth,
especially on the [scenarios](https://cwiki.apache.org/confluence/display/BOOKKEEPER/BP-14+Relax+durability) which prefer weak durability guarantee.
This proposal is aimed at providing bypass journal ledger, this feature includes these parts work:
 - add new write flag `BYPASS_JOURNAL` to existing protocol
 - impl the newly write flag at the client side and server side
 
The details of changes are listed in Section “[Proposed Changes](#proposed-changes)”.
 
### Public Interfaces

This feature will introduce new 'WRITE_FLAG' to enable single write storage impl.
Like `DEFERRED_SYNC`, when we construct `WriteHandle`, we can pass `BYPASS_JOURNAL`, then every `AddRequest` will carry this flag.
As for monitoring, we can add metrics to record the main time interval during the io path.
The detailed compatibility related stuff is in Section “[CDMP](#compatibility-deprecation-and-migration-plan)”.

### Proposed Changes

While the main approach is based on [`WRITE_FLAG` impl](https://github.com/apache/bookkeeper/pull/742),
the actual implementation is more tricky. In my opinion, there are three different solution:

1. Relax LAC protocol

    Modify server side code mostly and don't change legerHandle's LAC advance logic, if the write flag is `BYPASS_JOURNAL`, after write to `LegerStorage`(the data maybe in the memTable, or the buffer of File, or the os cache),
bookie return result to the client directly.
This impl is like [disable syncData](https://github.com/apache/bookkeeper/issues/753), once all the replica fails, the BK cluster can't recovery from it.
Note, the user shall know the weak durability for his use case when using this impl.

2. Extend `Force` API

    Like `DEFERRED_SYNC`, the normal 'bypass-journal' operation don't advance LAC on the client.
Only advance LAC using `force` api. To support this new 'bypass-journal' option, we need theses changes:

    - Add persistent callback to LedgerStorage
    
        Maintain non-persistent entry list(necessary for out of order entry arriving) and `maxPersistentEntryId` on `LedgerDescriptor`
If the entry is 'bypass-journal', the callback for this entry is not null, and it will receives notification after persistent through legerStorage.
    
    - Extend the force request
        
        If the force request contains bypass journal option, get `maxPersistentEntryId` from `LedgerDescriptor`
 or impl force method on `LedgerDescriptor`, which force the entry to persistent device through `LedgerStorage`.

    In addition to these changes, we should consider who is responsible for the `force` execution? How often to schedule 'force'?

3. Add NonPersistentLAC semantic to existing LAC protocol

    To not violate LAC semantics like method 1, we can add `NonPersistentLAC` semantic, this includes these changes:

    - Add persistent callback to LedgerStorage

        Maintain non-persistent entry list and `maxPersistentEntryId` on `LedgerDescriptor`.
        
    - Wire protocol changes
    
        Add optional `maxPersistentEntryId` field to `AddResponse`. When the `AddRequest` has 'bypass-journal' option, the server get `maxPersistentEntryId` from `LedgerDescriptor` to construct response.

    - Client side changes
    
        Add `nonPersistentLAC` to WriteHandle. WriteHandle with 'bypass-journal' option updates the LAC using `maxPersistentEntryId`, and update `nonPersistentLAC` if receives enough ack.
        For normal WriteHandle, the `nonPersistentLAC` is set to null to indicate to keep original LAC advance logic. 

### Compatibility, Deprecation, and Migration Plan

As this new feature only add one ledger creating option and corresponding ledger implementation. It has no effect
on existing users. After this feature is implemented, user can use this function directly on new released version which includes this implementation.
When user use the latest client which has the `BYPASS_JOURNAL` write flag, but the bookie is old versions which can't do bypass-journal process,
 the bookie will reject this type write. Old client has no chance to use this new write flag, and the new bookie can handle as usual.

### Test Plan

- unit tests for newly introduced write_flag at client side and server side
- end-to-end tests for the new Protocol request/response
- compat tests (client with this new writeflags will not be able to write on old bookies)

### Rejected Alternatives

To avoid two write of data, we can write the data to one log abstraction in the bookie, either by bypassing journal or 
 bypassing entryLog. If we choose bypassing entryLog, then we will do lots of work to enable rich read on journal,
 which is more huge work compared with bypassing journal.


