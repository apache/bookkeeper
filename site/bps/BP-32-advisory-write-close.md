---
title: "BP-32: Advisory (optimistic) write close"
issue: https://github.com/apache/bookkeeper/issues/1343
state: Under Discussion
release: N/A
---

### Motivation

With [entrylog per ledger feature](https://github.com/apache/bookkeeper/issues/570) there will be dedicated entrylog for each ledger and it provides EntryLogManagerForEntryLogPerLedger (EntryLogManager). Since there is going to be entrylog per ledger, with the current Bookie implementation there is no way for EntryLogManagerForEntryLogPerLedger (EntryLogManager) to know when the entrylog is writeclosed, so that entrylog for the active ledger can be rotated. So it would be ideal to have explicit call to EntryLogger when the write for this ledger is done and it is writeclosed, so that it can rotate the entrylog as soon as it is write closed. This will minimize the number of entrylogs/file descriptors that are open/active and also it will make progress in leastUnflushedLogId, so that GarbageCollectorThread can consider these entrylogs for garbage collection.

### Proposed Changes

So in entrylog per ledger feature implementation, as a first step of handling this, expireAfterAccess (each entrylog should be automatically rotated from the current active entrylog list once a fixed duration has elapsed after the last access of entrylog for addEntry) will be implemented and also the maximum number of entrylogs that can be active at a given time (this is for limiting number of entrylogs/filedescriptors open at a given time).

The above mentioned approaches are preliminary ways of handling but it would be ideal to have explicit call to EntryLogger when the write for this ledger is done so that it can rotate the entrylog as soon as it is write closed. So to implement this following can be done

- have explicit write close request. This write close request should be sent to the current ensemble when the write handle is closed. This should be just optimistic write close operation and callback of this operation should be just logger, saying if it is succeeded / partially succeeded / failed. This should be done asynchronously and the write handle close operation should not be blocked for this response.

- In the case of ledger recover open, readrequest (ReadEntryProcessorV3) with fence flag, can take care of calling appropriate methods in ledgerdescriptor / entry logger to rotate the entrylog for the ledger

- in the case of auto-replication case, LedgerFragmentReplicator already uses bookieclient for addEntry (bkc.getBookieClient().addEntry), the same bookieClient instance can be used to call explicit writeClose method in bookieclient.

- in the case of any write failure in bookie for an entry, then before sending error response to the client do entrylog rotation

EntryLogManagerForEntryLogPerLedger is the class which is last layer in the stack to receive this writeClose call for a ledger, it would store this info in in-memory data structure and when next next checkpoint is called it would rotate entrylogs of these writeclosed ledgers.

### Public Interfaces

As explained above, this advisory write close should be transparent to Bookkeeper API user, this should be piggybagged in writehandle close call and other internal Bookie / BookieClient internal class methods (ReadEntryProcessorV3.readrequest and LedgerFragmentReplicator). But this feature introduces new protobuf message between Client and Bookie.

```
message WriteCloseRequest {
    required int64 ledgerId = 1;
    required bytes masterKey = 2;    
}

message WriteCloseResponse {
    required StatusCode status = 1;
    required int64 ledgerId = 2;  
}
```

### Compatibility, Deprecation, and Migration Plan

- With this feature we are introducing new protocol message. Will do the required Compatibility testing. But since it is going to be advisory (optimistic) in nature, failure in this request in anyway should not affect functioning in any way.
- Also, possibly this explicit write close can be used in future for other purposes.

### Test Plan

- unit tests for all the write/read handle close scenarios, recoveropen and replicator scenarios
- end-to-end integrations tests.

### Rejected Alternatives

N/A
