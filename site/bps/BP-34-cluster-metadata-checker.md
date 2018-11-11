---
title: "BP-34: Cluster Metadata Checker“
issue: https://github.com/apache/bookkeeper/issues/1602
state: Accepted
release: N/A
---

### Motivation

Currently in the Auditor we have two checkers - Periodic Bookie Check and Periodic Ledger check. Bookie Check validates the availability of bookies and if it finds any lost bookies it will mark ledgers residing in the lost bookies to be under replicated. Ledger Check reads the first entry and last entry of the segment from all the corresponding bookies of the ensemble and if it fails to read any entry then it will mark that ledger under replicated.  By setting appropriate value to the conf - ‘auditorLedgerVerificationPercentage’ we can read upto 100% of entries of the ledger from all the corresponding bookies of the ensemble and any failure in reading will lead to mark ledger under replicated.

Ideally for having complete confidence on the date in the cluster, it is needed to have a new checker - validating ledger placement policy, durability contract, progress in handling under replication and availability of bookies of the ensemble of ledgers. Though by configuring 'auditorLedgerVerificationPercentage' to 100% in periodic ledger check, we would get most of what we are intending to achieve. But this comes at heavy price since it involves reading all the entries from all the corresponding bookies in the ensemble, so it is not a performant solution.

### Proposed Changes

Intention of this new checker is to validate following things
  - ledger placement policy : Ensemble of each segment in Ledger should adhere to LedgerPlacementPolicy
  - durability contract : Every entry has WQ number of replicas and entries are replicated according to RoundRobinDistributionSchedule
  - progress in handling under replication : No ledger is marked underreplicated for more than acceptable time
  - availability of bookies of the ensemble of ledgers : If Auditor fails to get response from a Bookie, then that Bookie shouldn’t be registered to metadata server and Auditor should be aware of it unavailability or if it is a transient error in getting response from Bookie then subsequent calls to that Bookie should succeed.

Roles and Responsibilities of the cluster metadata checker
  - Police the durability contract and report violations. Its job is to make sure that the metadata server(zk) and the storage servers (bookies) are in sync. Simply put, check if bookies agree with the metadata server metadata and if not, raise an alert.
  - Scrutiny’s job is not to fix if it finds any inconsistency. Instead make a noise about it. If the scrutiny fails, it means that we have a potential hole(bug) in our service to meet the durability contract. Scrutiny exposes that hole with enough information the help identify the issue and fix it.
  - The Metadata Scrutiny needs to be light weighted esp., on Bookie and must run regularly giving the confidence that the cluster is in good state.

High Level Logic
  - Things would get complicated analyzing ledgers which are not closed because of several reasons, viz., unable to know lastEntryId by reading ZK metadata, possibility of change in ensemble because of write failure to a bookie, and other subtleties in dealing with last unclosed segment of the ledger. So for the sake of simplicity this checker should be limited to ledgers which are write closed/fenced.
  - This durability check for each ledger will be run as a processor in ledgerManager.asyncProcessLedgers and it would ignore ledgers which are still open for write.
  - first step is to check if this ledger is marked underreplicated already. If it is marked underreplicated for more than acceptable time then report it as violation otherwise skip this underreplicated ledger for this iteration of durability check. Since there is no point in further analyzing this ledger if it is already marked under replicated.
  - get the ledger metadata of the ledger from the metadata server
  - make sure that the ensemble of the ledger segments is in agreement with ledgerplacement policy. Any violation should be reported.
  - get the info about available entries of the ledger from the bookies of the ensemble. Bookie is expected to return list of entries it contains for a given ledger
  - Have to make sure that Bookies contain all the entries it is supposed to contain according to the RoundRobinDistributionSchedule and each entry has writequorum number of copies. Any violation should be reported.
  - If there is any failure in trying to get info. from Bookie of the ensembles of the ledger, then add this ledger to potentially faulty ledgers list (but don't report it yet.)
  - (in previous steps, in case of any violation or bookie read error, before reporting violation, check if the ledger is marked underreplicated. If it is marked underreplicated then ignore this ledger for this iteration. If it is not marked underreplicated, then get the ledgermetadata of this ledger onemore time. Check if it is any different from the ledgermetadata we got initially then instead of reporting the violation, redo the analysis for this ledger because apparently something had changed in the metadata (esp. with ensemble) and hence it is better to reevaluate instead of false alarm.)
  - if there are potentially faulty ledgers because of unavailable/unreachable bookies, then schedule a new durability check task with time delay just for the potentially faulty ledgers. Even after subsequent delayed checks, if Auditor failed to get response from bookies then make sure that Bookie isn’t registered to metadata server and Auditor is aware of it unavailability, if not then report the violation.
  - Auditor is going to use existing mechanisms/frameworks to report the violations - bookkeeper-stats statslogger/counters and complementing information in logs.
  - It makes sense to group all the durability violations found in a scrutiny run according to the categories and report the aggregated count for each category after the end of the scrutiny run.
  - before reporting these violations, each violation should be logged with complete information, so that it can be used to understand what went wrong.

### Public Interfaces

To know the entries of a ledger data persisted in a Bookie, currently there is no other way than reading the entry from bookie using BookieClient instance. So for auditor to know what entries of a ledger, Bookie contains we need to have a new on wire Bookkeeper protocol API as mentioned below.

```
message GetListOfEntriesOfALedgerRequest {
	required int64 ledgerId = 1;
}

message GetListOfEntriesOfALedgerResponse {
	required StatusCode status = 1;
	required int64 ledgerId = 2;
	optional bytes availabilityOfEntriesOfLedger = 3; // treated as array of bits indicating availability of entry at that particular index location		
}
```

For the sake of future extensibility it would be helpful to add version info (and possibly some metadata in the future) at the beginning of the 'availabilityOfEntriesOfLedger'. So the first 64 bytes will be considered reserved space, with the first byte specifying the version for now and the rest of the bytes in the reserved space will be 0's.

Here Bookie is expected to attain this information just from just LedgerCache (Index Files) - IndexPersistenceMgr and IndexInMemPageMgr but it doesn’t actually check the availability of this entry in Entrylogger. Since the intention of this checker is limited to do just metadata validation at cluster level.

### Compatibility, Deprecation, and Migration Plan

- With this feature we are introducing new protocol message. Will do the required Compatibility testing.

### Test Plan

- unit tests for newly introduced API/code at LedgerCache Level
- end-to-end tests for the new Protocol request/response
- validating the checker in all cases of violations

### Rejected Alternatives

N/A
