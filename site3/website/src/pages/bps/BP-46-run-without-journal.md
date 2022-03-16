# BP-46: Running without the journal

### Motivation

The journal allows for fast add operations that provide strong data safety guarantees. An add operation is only acked to a client once written to the journal and an fsync performed. This however means that every entry must be written twice: once to the journal and once to an entry log file.

This double write increases the cost of ownership as more disks must be provisioned to service requests and makes disk provisioning more complex (separating journal from entry log writes onto separate disks). Running without the journal would halve the disk IO required (ignoring indexes) thereby reducing costs and simplifying provisioning.

However, running without the journal would introduce data consistency problems as the BookKeeper Replication Protocol requires that all writes are persistent for correctness. Running without the journal introduces the possibility of lost writes. In order to continue to offer strong data safety and support running without the journal, changes to the protocol are required.

### A note on Response Codes

The following categories are relevant:

- Positive: OK
- Explicit Negative: NoSuchEntry/NoSuchLedger
- Unknown: Any other non-success response that is not an explicit negative.

For correctness explicit negatives must be treated differently than other errors.

### A note on Quorums

In order to explain the protocol changes, it is useful to first consider how quorums are used for safety. We have the following relevant quorums:

- Single bookie (S)
- Ack quorum (AQ)
- Write quorum (WQ)
- Quorum Coverage (QC) where QC = (WQ - AQ) + 1
- Ensemble Coverage (EC) where EC = (E - AQ) + 1
- Whole Ensemble

Quorum Coverage (QC) and Ensemble Coverage (EC) are both defined by the following, only the cohorts differ: 

- A given property is satisfied by at least one bookie from every possible ack quorum within the cohort.
- There exists no ack quorum of bookies that do not satisfy the property within the cohort.

For QC, the cohort is the writeset of a given entry, and therefore QC is only used when we need guarantees regarding a single entry. For EC, the cohort is the ensemble of bookies of the current fragment. EC is required when we need a guarantee across an entire fragment.

For example:

- For fencing, we need to ensure that no AQ of bookies is unfenced before starting the read/write phase of recovery. This is true once EC successful fencing responses have been received.
- For a recovery read, a read is only negative once we know that no AQ of bookies could exist that might have the entry. Doing otherwise could truncate committed entries from a ledger. A read is negative once NoSuchEntry responses reach QC.

Different protocol actions require different quorums:

- Add entry: AQ success responses
- Read entry:
  - Positive when positive response from a single bookie
  - Negative when explicit negative from all bookies
  - Unknown: when at least one unknown and no positive from all bookies
- Fencing phase, LAC read (sent to ensemble of current fragment):
  - Complete when EC positive responses
  - Unknown (cannot make progress) when AQ unknown responses (fencing LAC reads cannot cause an explicit negative as fencing creates the ledger on the bookie if it doesn’t exist)
- Recovery read (sent to writeset of entry):
  - Entry recoverable: AQ positive read responses
  - Entry Unrecoverable: QC negative read responses
  - Unknown (cannot make progress):
    - QC unknown responses or
    - All responses received, but not enough for either a positive or negative


### Impact of Undetected Data Loss on Consistency

The ledger recovery process assumes that ledger entries are never arbitrarily lost. In the event of the loss of an entry, the recovery process can:
- allow the original client to keep writing entries to a ledger that has just been fenced and closed, thus losing those entries 
- allow the recovery client to truncate the ledger too soon, closing it with a last entry id lower than that of previously acknowledged entries - thus losing data.

The following scenarios assume existing behaviour but simply skipping the writing of entries and fencing ops to the journal.

### Scenario 1 - Lost Fenced Status Allows Writes After Ledger Close

1. 3 bookies, B1, B2 & B3
2. 2 clients, C1 & C2
3. 1 ledger, L1, with e3:w3:a2 configuration.
4. C1 writes entry E1 to L1. The write hits all three bookies.
5. C1 hangs for an indeterminate length of time. 
6. C2 sees that C1 is unresponsive, and assumes it has failed. C2 tries to recover the ledger L1.
7. L1 sends a fencing message to all bookies in the ensemble.
8. The fencing message succeeds in arriving at B1 & B2 and is acknowledged by both. The message to B3 is lost. 
9. C2 sees that at least one bookie in each possible ack quorum has acknowledged the fencing message (EC threshold reached), so continues with the read/write phase of recovery, finding that E1 is the last entry of the ledger, and committing the endpoint of the ledger in the ZK.
10. B2 crashes and boots again with all unflushed operations lost. 
11. C1 wakes up and writes entry E2 to all bookies. B2 & B3 acknowledge positively, so C1 considers E2 as persisted. B1 rejects the message as the ledger is fenced, but since ack quorum is 2, B2 & B3 are enough to consider the entry written.

### Scenario 2 - Recovery Truncates Previously Acknowledged Entries

1. C1 adds E0 to B1, B2, B3
2. B1 and B3 confirms. C1 confirms the write to its client.
3. C2 starts recovery
4. B2 fails to respond. C1 tries to change ensemble but gets a metadata version conflict.
5. B1 crashes and restarts, has lost E0 (undetected)
6. C2 fences the ledger on B1, B2, B3
7. C2 sends Read E0 to B1, B2, B3
8. B1 responds with NoSuchEntry
9. B2 responds with NoSuchEntry
10. QC negative response threshold reached. C2 closes the ledger as empty. Losing E0.

The problem is that without the journal (and syncing to entry log files before acknowledgement) a bookie can:
- lose the fenced status of a previously existing ledger
- respond with an explicit negative even though it had previously seen an entry. 

Undetected data loss could occur when running without the journal. Bookie crashes and loses most recent entries and fence statuses that had not yet been written and synced to disk.

### A note on cookies

Cookies play an essential part in the bookkeeper replication protocol, but their purpose is often unclear. 

When a bookie boots for the first time, it generates a cookie. The cookie encapsulates the identity of the bookie and should be considered immutable. This identity contains the advertised address of the bookie, the disks used for the journal, index, and ledger storage, and a unique ID. The bookie writes the cookie to ZK and each of the disks in use. On all subsequent boots, if the cookie is missing from any of these places, the bookie fails to boot.

The absence of a disk's cookie implies that the rest of the disk's data is also missing. Cookie validation is performed on boot-up and prevents the boot from succeeding if the validation fails, thus preventing the bookie starting with undetected data loss. 

This proposal improves the cookie mechanism by automating the resolution of a cookie validation error which currently requires human intervention to resolve. This automated feature will be configurable (enabled or disabled) and additionally a CLI command will be made available so an admin can manually run the operation (for when this feature is disabled - likely to be the default). 

### Proposed Changes

The proposed changes involve:
- A new config that controls whether add operations go into the journal
- Detecting possible data loss on boot
- Prevent explicit negative responses when data loss may have occurred, instead reply with unknown code, until data is repaired.
- Repair data loss
- Auto fix cookies (with new config to enable or disable the feature)
- CLI command for admin to run fix cookie logic in the case that auto fix is disabled

In these proposed changes, when running "without" the journal, the journal still exists, but add entry operations skip the addition to the journal. The boot-up sequence still replays the journal.

Add operations can be configured to be written to the journal or not based on the config `journalWriteData`. When set to `false`, add operations are not added to the journal.

### Detecting Data Loss On Boot

The new mechanism for data loss detection is checking for an unclean shutdown (aka a crash or abrupt termination of the bookie). When an unclean shutdown is detected further measures are taken to prevent data inconsistency.

The unclean shutdown detection will consist of setting a bit in the index on start-up and clearing it on shutdown. On subsequent start-up, the value will be checked and if it remains set, it knows that the prior shutdown was not clean.

Cookie validation will continue to be used to detect booting with one or more missing or empty disks (that once existed and contained a cookie).

### Protection Mechanism

Once possible data loss has been detected the following protection mechanism is carried out during the boot:

- Fencing: Ledger metadata for all ledgers of the cluster are obtained and all those ledgers are fenced on this bookie. This prevents data loss scenario 1.
- Limbo: All open ledgers are placed in the limbo status. Limbo ledgers can serve read requests, but never respond with an explicit negative, all explicit negatives are converted to unknowns (with the use of a new code EUNKNOWN).
- Recovery: All open ledgers are opened and recovered.
- Repair: Each ledger is scanned and any missing entries are sourced from peers.
- Limbo ledgers that have been repaired have their limbo status cleared.

### The Full Boot-Up Sequence

This mechanism of limbo ledgers and self-repair needs to work hand-in hand with the cookie validation check. Combining everything together:

On boot:
1. Check for unclean shutdown and validate cookies
2. Fetch the metadata for all ledgers in the cluster from ZK where the bookie is a member of its ensemble.
3. Phase one:
   - If the cookie check fails or unclean shutdown is detected:
     - For each non-closed ledger, mark the ledger as fenced and in-limbo in the index.
     - Update the cookie if it was a cookie failure
4. Phase two
   - For each ledger
     1. If the ledger is in-limbo, open and recover the ledger.
     2. Check that all entries assigned to this bookie exist in the index.
     3. For any entries that are missing, copy from another bookie.
     4. Clear limbo status if set

When booting a bookie with empty disks, only phase one needs to be complete before the bookie makes itself available for client requests. 

In phase one, if the cookie check fails, we mark all non-closed ledgers as “fenced”. This prevents any future writes to these ledgers on this bookie. This solves the problem of an empty bookie disk allowing writes to closed ledgers (Scenario 1).

Given that the algorithm solves both the issues that cookies are designed to solve, we can now allow the bookie to update its cookie without operator intervention. 

### Formal Verification of Proposed Changes

The use of the limbo status and fencing of all ledgers on boot-up when detecting an unclean shutdown has been modelled in TLA+. It does not model the whole boot-up sequence but a simplified version with only fencing and limbo status. 

The specification models the lifetime of a single ledger and includes a single bookie crashing, losing all data. The specification allows the testing of:

- enabling/disabling the fencing
- enabling/disabling the limbo status.

When running without limbo status, the model checker finds the counterexample of scenario 2. When running without fencing of all ledgers, the model checker finds the counterexample of scenario 1. When running with both enabled, the model checker finds no invariant violation.

The specification can be found here: https://github.com/Vanlightly/bookkeeper-tlaplus

### Public Interfaces

- Return codes. Addition of a new return code: `EUNKNOWN` which is returned when a read hits an in-limbo ledger and that ledger not contain the requested entry id.
- Bookie ledger metadata format (LedgerData). Addition of the limbo status.

### Compatibility, Deprecation, and Migration Plan

- Because we only skip the journal for add operations, there is no impact on existing deployments. When a bookie is booted with the new version, and `journalWriteData` is set to false, the journal is still replayed on boot-up causing no risk of data loss in the transition.

### Test Plan

- There is confidence in the design due to the modelling in TLA+ but this model does not include the full boot sequence.
- The implementation will require aggressive chaos testing to ensure correctness.

### Rejected Alternatives

Entry Log Per Ledger (ELPL) without the journal. From our performance testing of ELPL, performance degrades significantly with a large number of active ledgers and syncing to disk multiple times a second (which is required to offer low latency writes).

In the future this design could be extended to offer ledger level configuration of journal use. The scope of this BP is limited to cluster level.