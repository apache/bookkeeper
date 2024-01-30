---
id: protocol
title: The BookKeeper protocol
---

BookKeeper uses a special replication protocol for guaranteeing persistent storage of entries in an ensemble of bookies.

> This document assumes that you have some knowledge of leader election and log replication and how these can be used in a distributed system. If not, we recommend reading the [example application](../api/ledger-api#example-application) documentation first.

## Ledgers

Ledgers are the basic building block of BookKeeper and the level at which BookKeeper makes its persistent storage guarantees. A replicated log consists of an ordered list of ledgers. See [Ledgers to logs](#ledgers-to-logs) for info on building a replicated log from ledgers.

Ledgers are composed of metadata and entries. The metadata is stored in ZooKeeper, which provides a *compare-and-swap* (CAS) operation. Entries are stored on storage nodes known as bookies.

A ledger has a single writer and multiple readers (SWMR).

### Ledger metadata

A ledger's metadata contains the following:

Parameter | Name | Meaning
:---------|:-----|:-------
Identifer | | A 64-bit integer, unique within the system
Ensemble size | **E** | The number of nodes the ledger is stored on
Write quorum size | **Q<sub>w</sub>** | The number of nodes each entry is written to. In effect, the max replication for the entry.
Ack quorum size | **Q<sub>a</sub>** | The number of nodes an entry must be acknowledged on. In effect, the minimum replication for the entry.
Current state | | The current status of the ledger. One of `OPEN`, `CLOSED`, or `IN_RECOVERY`.
Last entry | | The last entry in the ledger or `NULL` is the current state is not `CLOSED`.

In addition, each ledger's metadata consists of one or more *fragments*. Each fragment is either

* the first entry of a fragment or
* a list of bookies for the fragment.

When creating a ledger, the following invariant must hold:

**E >= Q<sub>w</sub> >= Qa**

Thus, the ensemble size (**E**) must be larger than the write quorum size (**Q<sub>w</sub>**), which must in turn be larger than the ack quorum size (**Q<sub>a</sub>**). If that condition does not hold, then the ledger creation operation will fail.

### Ensembles

When a ledger is created, **E** bookies are chosen for the entries of that ledger. The bookies are the initial ensemble of the ledger. A ledger can have multiple ensembles, but an entry has only one ensemble. Changes in the ensemble involve a new fragment being added to the ledger.

Take the following example. In this ledger, with ensemble size of 3, there are two fragments and thus two ensembles, one starting at entry 0, the second at entry 12. The second ensemble differs from the first only by its first element. This could be because bookie1 has failed and therefore had to be replaced.

First entry | Bookies
:-----------|:-------
0 | B1, B2, B3
12 | B4, B2, B3

### Write quorums

Each entry in the log is written to **Q<sub>w</sub>** nodes. This is considered the write quorum for that entry. The write quorum is the subsequence of the ensemble, **Q<sub>w</sub>** in length, and starting at the bookie at index (entryid % **E**).

For example, in a ledger of **E** = 4, **Q<sub>w</sub>** = 3, and **Q<sub>a</sub>** = 2, with an ensemble consisting of B1, B2, B3, and B4, the write quorums for the first 6 entries will be:

Entry | Write quorum
:-----|:------------
0 | B1, B2, B3
1 | B2, B3, B4
2 | B3, B4, B1
3 | B4, B1, B2
4 | B1, B2, B3
5 | B2, B3, B4

There are only **E** distinct write quorums in any ensemble. If **Q<sub>w</sub>** = **E**, then there is only one, as no striping occurs.

### Ack quorums

The ack quorum for an entry is any subset of the write quorum of size **Q<sub>a</sub>**. If **Q<sub>a</sub>** bookies acknowledge an entry, it means it has been fully replicated.

### Guarantees

The system can tolerate **Q<sub>a</sub>** – 1 failures without data loss.

Bookkeeper guarantees that:

1. All updates to a ledger will be read in the same order as they were written.
2. All clients will read the same sequence of updates from the ledger.

## Writing to ledgers

writer, ensuring that entry ids are sequential is trivial. A bookie acknowledges a write once it has been persisted to disk and is therefore durable. Once **Q<sub>a</sub>** bookies from the write quorum acknowledge the write, the write is acknowledged to the client, but only if all entries with lower entry ids in the ledger have already been acknowledged to the client.

The entry written contains the ledger id, the entry id, the last add confirmed and the payload. The last add confirmed is the last entry which had been acknowledged to the client when this entry was written. Sending this with the entry speeds up recovery of the ledger in the case that the writer crashes.

Another client can also read entries in the ledger up as far as the last add confirmed, as we guarantee that all entries thus far have been replicated on Qa nodes, and therefore all future readers will be able to also read it. However, to read like this, the ledger should be opened with a non-fencing open. Otherwise, it would kill the writer.

If a node fails to acknowledge a write, the writer will create a new ensemble by replacing the failed node in the current ensemble. It creates a new fragment with this ensemble, starting from the first message that has not been acknowledged to the client. Creating the new fragment involves making a CAS write to the metadata. If the CAS write fails, someone else has modified something in the ledger metadata. This concurrent modification could have been caused by recovery or rereplication. We reread the metadata. If the state of the ledger is no longer `OPEN`, we send an error to the client for any outstanding writes. Otherwise, we try to replace the failed node again.

### Closing a ledger as a writer

Closing a ledger is straightforward for a writer. The writer makes a CAS write to the metadata, changing the state to `CLOSED` and setting the last entry of the ledger to the last entry which we have acknowledged to the client.

If the CAS write fails, it means someone else has modified the metadata. We reread the metadata, and retry closing as long as the state of the ledger is still `OPEN`. If the state is `IN_RECOVERY` we send an error to the client. If the state is `CLOSED` and the last entry is the same as the last entry we have acknowledged to the client, we complete the close operation successfully. If the last entry is different from what we have acknowledged to the client, we send an error to the client.

### Closing a ledger as a reader

A reader can also force a ledger to close. Forcing the ledger to close will prevent any writer from adding new entries to the ledger. This is called fencing. This can occur when a writer has crashed or become unavailable, and a new writer wants to take over writing to the log. The new writer must ensure that it has seen all updates from the previous writer, and prevent the previous writer from making any new updates before making any updates of its own.

To recover a ledger, we first update the state in the metadata to IN_RECOVERY. We then send a fence message to all the bookies in the last fragment of the ledger. When a bookie receives a fence message for a ledger, the fenced state of the ledger is persisted to disk. Once we receive a response from at least (**Q<sub>w</sub>** - **Q<sub>a</sub>**)+1 bookies from each write quorum in the ensemble, the ledger is fenced.

By ensuring we have received a response from at last (**Q<sub>w</sub>** - **Q<sub>a</sub>**) + 1 bookies in each write quorum, we ensure that, if the old writer is alive and tries to add a new entry there will be no write quorum in which Qa bookies will accept the write. If the old writer tries to update the ensemble, it will fail on the CAS metadata write, and then see that the ledger is in IN_RECOVERY state, and that it therefore shouldn’t try to write to it.

The old writer will be able to write entries to individual bookies (we can’t guarantee that the fence message reaches all bookies), but as it will not be able reach ack quorum, it will not be able to send a success response to its client. The client will get a LedgerFenced error instead.

It is important to note that when you get a ledger fenced message for an entry, it doesn’t mean that the entry has not been written. It means that the entry may or may not have been written, and this can only be determined after the ledger is recovered. In effect, LedgerFenced should be treated like a timeout.

Once the ledger is fenced, recovery can begin. Recovery means finding the last entry of the ledger and closing the ledger. To find the last entry of the ledger, the client asks all bookies for the highest last add confirmed value they have seen. It waits until it has received a response at least (**Q<sub>w</sub>** - **Q<sub>a</sub>**) + 1 bookies from each write quorum, and takes the highest response as the entry id to start reading forward from. It then starts reading forward in the ledger, one entry at a time, replicating all entries it sees to the entire write quorum for that entry. Once it can no longer read any more entries, it updates the state in the metadata to `CLOSED`, and sets the last entry of the ledger to the last entry it wrote. Multiple readers can try to recovery a ledger at the same time, but as the metadata write is CAS they will all converge on the same last entry of the ledger.

## Ledgers to logs

In BookKeeper, ledgers can be used to build a replicated log for your system. All guarantees provided by BookKeeper are at the ledger level. Guarantees on the whole log can be built using the ledger guarantees and any consistent datastore with a compare-and-swap (CAS) primitive. BookKeeper uses ZooKeeper as the datastore but others could theoretically be used.

A log in BookKeeper is built from some number of ledgers, with a fixed order. A ledger represents a single segment of the log. A ledger could be the whole period that one node was the leader, or there could be multiple ledgers for a single period of leadership. However, there can only ever be one leader that adds entries to a single ledger. Ledgers cannot be reopened for writing once they have been closed/recovered.

> BookKeeper does *not* provide leader election. You must use a system like ZooKeeper for this.

In many cases, leader election is really leader suggestion. Multiple nodes could think that they are leader at any one time. It is the job of the log to guarantee that only one can write changes to the system.

### Opening a log

Once a node thinks it is leader for a particular log, it must take the following steps:

1. Read the list of ledgers for the log
1. Fence the last two ledgers in the list. Two ledgers are fenced because the writer may be writing to the second-to-last ledger while adding the last ledger to the list.
1. Create a new ledger
1. Add the new ledger to the ledger list
1. Write the new ledger back to the datastore using a CAS operation

The fencing in step 2 and the CAS operation in step 5 prevent two nodes from thinking that they have leadership at any one time.

The CAS operation will fail if the list of ledgers has changed between reading it and writing back the new list. When the CAS operation fails, the leader must start at step 1 again. Even better, they should check that they are in fact still the leader with the system that is providing leader election. The protocol will work correctly without this step, though it will be able to make very little progress if two nodes think they are leader and are duelling for the log.

The node must not serve any writes until step 5 completes successfully.

### Rolling ledgers

The leader may wish to close the current ledger and open a new one every so often. Ledgers can only be deleted as a whole. If you don't roll the log, you won't be able to clean up old entries in the log without a leader change. By closing the current ledger and adding a new one, the leader allows the log to be truncated whenever that data is no longer needed. The steps for rolling the log is similar to those for creating a new ledger.

1. Create a new ledger
1. Add the new ledger to the ledger list
1. Write the new ledger list to the datastore using CAS
1. Close the previous ledger

By deferring the closing of the previous ledger until step 4, we can continue writing to the log while we perform metadata update operations to add the new ledger. This is safe as long as you fence the last 2 ledgers when acquiring leadership.

