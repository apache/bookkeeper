---
title: The Advanced Ledger API
---

In release `4.5.0`, Apache BookKeeper introduces a few advanced API for advanced usage.
This sections covers these advanced APIs.

> Before learn the advanced API, please read [Ledger API](../ledger-api) first.

## LedgerHandleAdv

[`LedgerHandleAdv`](../javadoc/org/apache/bookkeeper/client/LedgerHandleAdv) is an advanced extension of [`LedgerHandle`](../javadoc/org/apache/bookkeeper/client/LedgerHandle).
It allows user passing in an `entryId` when adding an entry.

### Creating advanced ledgers

Here's an exmaple:

```java
byte[] passwd = "some-passwd".getBytes();
LedgerHandleAdv handle = bkClient.createLedgerAdv(
    3, 3, 2, // replica settings
    DigestType.CRC32,
    passwd);
```

You can also create advanced ledgers asynchronously.

```java
class LedgerCreationCallback implements AsyncCallback.CreateCallback {
    public void createComplete(int returnCode, LedgerHandle handle, Object ctx) {
        System.out.println("Ledger successfully created");
    }
}
client.asyncCreateLedgerAdv(
        3, // ensemble size
        3, // write quorum size
        2, // ack quorum size
        BookKeeper.DigestType.CRC32,
        password,
        new LedgerCreationCallback(),
        "some context"
);
```

Besides the APIs above, BookKeeper allows users providing `ledger-id` when creating advanced ledgers.

```java
long ledgerId = ...; // the ledger id is generated externally.

byte[] passwd = "some-passwd".getBytes();
LedgerHandleAdv handle = bkClient.createLedgerAdv(
    ledgerId, // ledger id generated externally
    3, 3, 2, // replica settings
    DigestType.CRC32,
    passwd);
```

> Please note, it is users' responsibility to provide a unique ledger id when using the API above.
> If a ledger already exists when users try to create an advanced ledger with same ledger id,
> a [LedgerExistsException](../javadoc/org/apache/bookkeeper/client/BKException.BKLedgerExistException.html) is thrown by the bookkeeper client.

Creating advanced ledgers can be done throught a fluent API since 4.6.

```java
BookKeeper bk = ...;

byte[] passwd = "some-passwd".getBytes();

WriteHandle wh = bk.newCreateLedgerOp()
    .withDigestType(DigestType.CRC32)
    .withPassword(passwd)
    .withEnsembleSize(3)
    .withWriteQuorumSize(3)
    .withAckQuorumSize(2)
    .makeAdv()                  // convert the create ledger builder to create ledger adv builder
    .withLedgerId(1234L)
    .execute()                  // execute the creation op
    .get();                     // wait for the execution to complete

```

### Add Entries

The normal [add entries api](ledger-api/#adding-entries-to-ledgers) in advanced ledgers are disabled. Instead, when users want to add entries
to advanced ledgers, an entry id is required to pass in along with the entry data when adding an entry.

```java
long entryId = ...; // entry id generated externally

ledger.addEntry(entryId, "Some entry data".getBytes());
```

If you are using the new API, you can do as following:

```java
WriteHandle wh = ...;
long entryId = ...; // entry id generated externally

wh.write(entryId, "Some entry data".getBytes()).get();
```

A few notes when using this API:

- The entry id has to be non-negative.
- Clients are okay to add entries out of order.
- However, the entries are only acknowledged in a monotonic order starting from 0.

### Read Entries

The read entries api in advanced ledgers remain same as [normal ledgers](../ledger-api/#reading-entries-from-ledgers).
