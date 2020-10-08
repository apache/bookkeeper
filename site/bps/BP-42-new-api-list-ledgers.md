---
title: "BP-42: New Client API - list ledgers"
issue: https://github.com/apache/bookkeeper/issues/2422
state: "Under Discussion"
release: "N/A"
---

### Motivation

The new Client API (`org.apache.bookkeeper.client.api.BookKeeper`) aims to replace obsolete BookKeeper API but some features are not implemented yet, like the functionalities provided by `BookKeeperAdmin`. 
For example, it does not expose a method to list available ledgers, comparable to `BookKeeperAdmin#listLedgers()`.

#### Ledgers listing 
The goal here is to extend the Client API for listing ledgers. Moreover current method  `BookKeeperAdmin#listLedgers()` does not report errors from the metadata driver; for instance, if an IOException occurs during iterator flow, the error is not visible to the caller and the iteration is stopped (e.g. hasNext will return false). However there is no intention to change this behaviour in this proposal.

#### Simpler access to LedgerMetadata
The goal here is to streamline the access to `LedgerMetadata`, directly from BookKeeper interface.

#### Ledger id inside LedgerMetadata
Currently there is no `ledgerId` property inside `LedgerMetadata` interface, this can be helpful in some contexts.


### Public Interfaces

This proposal adds new interfaces to `org.apache.bookkeeper.client.api` package, similar to `org.apache.bookkeeper.client.api.BookKeeper` methods. 

    // new interface
    interface LedgersIterator {

        boolean hasNext() throws IOException;

        long next() throws IOException;
    }

    // new interface
    interface ListLedgersResult extends AutoCloseable {

        LedgersIterator iterator();

        Iterable<Long> toIterable();
    }

    // new interface
    interface ListLedgersResultBuilder extends OpBuilder<Void>{

        // empty now, maybe some filters in future
    }

    interface BookKeeper {

        ....

        ListLedgersResultBuilder newListLedgersOp();

        CompleatableFuture<LedgerMetadata> getLedgerMetadata(long ledgerId);

    }

    interface LedgerMetadata {
        
        ....

        long getLedgerId();

    }

### Proposed Changes

#### Ledgers listing

The implementation is pretty similar to `BookKeeperAdmin#listLedgers()` but there are few enhancements:
- Handle metadata driver errors, since the IOException is directly thrown up to caller, allowing user to handle network errors in a more suitable way.
- Leave the possibility to restrict/filter returned ledgers in future, without API breaking changes   
- Dispose some resources needed to retrieve ledgers (`ListLedgersResult` extends `AutoCloseable`)) (will be empty for current implementations)

The implementation will be the same used in BookKeeperAdmin, iterating over `LedgerRangeIterator`, which already handles ledgers search properly.

#### Simpler access to LedgerMetadata
The implementation will use LedgerManager to retrieve metadata for a specified ledgerId.  

#### Ledger id inside LedgerMetadata
Each time a LedgerMetadata instance is created, the ledgerId is known, so it is trivial to set it in the instance.

### Compatibility, Deprecation, and Migration Plan

No impact for current API, the proposal does not aim to explicit deprecate `BookKeeperAdmin#listLedgers()` method.

### Test Plan

This proposal needs only new unit tests, other tests must continue pass without any changes.

### Rejected Alternatives

An alternative could be creates a new API similar to BookKeeperAdmin but it is better to invest enhancing `org.apache.bookkeeper.client.api.BookKeeper` API.
