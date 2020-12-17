/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.apache.bookkeeper.proto.MockBookies;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger recovery tests using mocks rather than a real cluster.
 */
public class LedgerRecovery2Test {
    private static final Logger log = LoggerFactory.getLogger(LedgerRecovery2Test.class);

    private static final byte[] PASSWD = "foobar".getBytes();
    private static final BookieId b1 = new BookieSocketAddress("b1", 3181).toBookieId();
    private static final BookieId b2 = new BookieSocketAddress("b2", 3181).toBookieId();
    private static final BookieId b3 = new BookieSocketAddress("b3", 3181).toBookieId();
    private static final BookieId b4 = new BookieSocketAddress("b4", 3181).toBookieId();
    private static final BookieId b5 = new BookieSocketAddress("b5", 3181).toBookieId();

    private static Versioned<LedgerMetadata> setupLedger(ClientContext clientCtx, long ledgerId,
                                              List<BookieId> bookies) throws Exception {
        LedgerMetadata md = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(PASSWD).withDigestType(DigestType.CRC32C)
            .newEnsembleEntry(0, bookies).build();
        return clientCtx.getLedgerManager().createLedgerMetadata(1L, md).get();
    }

    @Test
    public void testCantRecoverAllDown() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1L, Lists.newArrayList(b1, b2, b3));

        clientCtx.getMockBookieClient().errorBookies(b1, b2, b3);

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);
        try {
            GenericCallbackFuture<Void> promise = new GenericCallbackFuture<>();
            lh.recover(promise, null, false);
            promise.get();
            Assert.fail("Recovery shouldn't have been able to complete");
        } catch (ExecutionException ee) {
            Assert.assertEquals(BKException.BKReadException.class, ee.getCause().getClass());
        }
    }

    @Test
    public void testCanReadLacButCantWrite() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1, Lists.newArrayList(b1, b2, b3));

        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b1, 1L, 0L, -1L);
        clientCtx.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> FutureUtils.exception(new BKException.BKWriteException()));

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);
        try {
            GenericCallbackFuture<Void> promise = new GenericCallbackFuture<>();
            lh.recover(promise, null, false);
            promise.get();
            Assert.fail("Recovery shouldn't have been able to complete");
        } catch (ExecutionException ee) {
            Assert.assertEquals(BKException.BKNotEnoughBookiesException.class, ee.getCause().getClass());
        }
    }

    @Test
    public void testMetadataClosedDuringRecovery() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1, Lists.newArrayList(b1, b2, b3));

        CompletableFuture<Void> writingBack = new CompletableFuture<>();
        CompletableFuture<Void> blocker = new CompletableFuture<>();
        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b1, 1L, 0L, -1L);
        // will block recovery at the writeback phase
        clientCtx.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    writingBack.complete(null);
                    return blocker;
                });

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);

        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        lh.recover(recoveryPromise, null, false);

        writingBack.get(10, TimeUnit.SECONDS);

        ClientUtil.transformMetadata(clientCtx, 1L,
                (metadata) -> LedgerMetadataBuilder.from(metadata)
                                     .withClosedState().withLastEntryId(-1).withLength(0).build());

        // allow recovery to continue
        blocker.complete(null);

        recoveryPromise.get();

        Assert.assertEquals(lh.getLastAddConfirmed(), -1);
        Assert.assertEquals(lh.getLength(), 0);
    }

    @Test
    public void testNewEnsembleAddedDuringRecovery() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        clientCtx.getMockRegistrationClient().addBookies(b4).get();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1, Lists.newArrayList(b1, b2, b3));

        CompletableFuture<Void> writingBack = new CompletableFuture<>();
        CompletableFuture<Void> blocker = new CompletableFuture<>();
        CompletableFuture<Void> failing = new CompletableFuture<>();
        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b1, 1L, 0L, -1L);
        // will block recovery at the writeback phase
        clientCtx.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    writingBack.complete(null);
                    if (bookie.equals(b3)) {
                        return failing;
                    } else {
                        return blocker;
                    }
                });

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);

        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        lh.recover(recoveryPromise, null, false);

        writingBack.get(10, TimeUnit.SECONDS);

        ClientUtil.transformMetadata(clientCtx, 1L,
                (metadata) -> LedgerMetadataBuilder.from(metadata).newEnsembleEntry(1L, Lists.newArrayList(b1, b2, b4))
                                     .build());

        // allow recovery to continue
        failing.completeExceptionally(new BKException.BKWriteException());
        blocker.complete(null);

        try {
            recoveryPromise.get();
            Assert.fail("Should fail on the update");
        } catch (ExecutionException ee) {
            Assert.assertEquals(BKException.BKUnexpectedConditionException.class, ee.getCause().getClass());
        }
    }

    @Test
    public void testRecoveryBookieFailedAtStart() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        clientCtx.getMockRegistrationClient().addBookies(b4).get();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1, Lists.newArrayList(b1, b2, b3));

        CompletableFuture<Void> writingBack = new CompletableFuture<>();
        CompletableFuture<Void> blocker = new CompletableFuture<>();
        CompletableFuture<Void> failing = new CompletableFuture<>();
        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b1, 1L, 0L, -1L);
        clientCtx.getMockBookieClient().errorBookies(b2);

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);

        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        lh.recover(recoveryPromise, null, false);
        recoveryPromise.get();

        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L),
                            Lists.newArrayList(b1, b4, b3));
    }

    @Test
    public void testRecoveryOneBookieFailsDuring() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        clientCtx.getMockRegistrationClient().addBookies(b4).get();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1, Lists.newArrayList(b1, b2, b3));
        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b1, 1L, 0L, -1L);
        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b3, 1L, 1L, -1L);
        clientCtx.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    if (bookie.equals(b2) && entryId == 1L) {
                        return FutureUtils.exception(new BKException.BKWriteException());
                    } else {
                        return FutureUtils.value(null);
                    }
                });

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);

        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        lh.recover(recoveryPromise, null, false);
        recoveryPromise.get();

        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 2);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L),
                            Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L),
                            Lists.newArrayList(b1, b4, b3));
        Assert.assertEquals(lh.getLastAddConfirmed(), 1L);
    }

    @Test
    public void testRecoveryTwoBookiesFailOnSameEntry() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        clientCtx.getMockRegistrationClient().addBookies(b4, b5).get();

        Versioned<LedgerMetadata> md = setupLedger(clientCtx, 1, Lists.newArrayList(b1, b2, b3));
        clientCtx.getMockBookieClient().getMockBookies().seedEntries(b1, 1L, 0L, -1L);
        clientCtx.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    if (bookie.equals(b1) || bookie.equals(b2)) {
                        return FutureUtils.exception(new BKException.BKWriteException());
                    } else {
                        return FutureUtils.value(null);
                    }
                });

        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(
                clientCtx, 1L, md, BookKeeper.DigestType.CRC32C, PASSWD, false);

        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        lh.recover(recoveryPromise, null, false);
        recoveryPromise.get();

        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).contains(b3));
        Assert.assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).contains(b4));
        Assert.assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).contains(b5));
        Assert.assertEquals(lh.getLastAddConfirmed(), 0L);
    }

    /**
     * This test verifies the fix for the data loss scenario found by the TLA+ specfication, specifically
     * the invariant violation that metadata and writer can diverge. The scenario is that the original writer
     * can commit an entry e that will later be lost because a second writer can close the ledger at e-1.
     * The cause is that fencing was originally only performed on LAC reads which is not enough to prevent
     * the 1st writer from reaching Ack Quorum after the 2nd writer has closed the ledger. The fix has
     * been to fence on recovery reads also.
     */
    @Test
    public void testFirstWriterCannotCommitWriteAfter2ndWriterCloses() throws Exception {
        //  Setup w1
        MockBookies mockBookies = new MockBookies();
        mockBookies.seedEntries(b1, 1, 0, -1);
        mockBookies.seedEntries(b2, 1, 0, -1);
        mockBookies.seedEntries(b3, 1, 0, -1);

        MockClientContext clientCtx1 = MockClientContext.create(mockBookies);
        Versioned<LedgerMetadata> md1 = setupLedger(clientCtx1, 1, Lists.newArrayList(b1, b2, b3));

        CompletableFuture<Void> blockB1Write = new CompletableFuture<>();
        CompletableFuture<Void> blockB2Write = new CompletableFuture<>();
        CompletableFuture<Void> blockB3Write = new CompletableFuture<>();
        clientCtx1.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    if (bookie.equals(b1)) {
                        return blockB1Write;
                    } else if (bookie.equals(b2)) {
                        return blockB2Write;
                    } else if (bookie.equals(b3)) {
                        return blockB3Write;
                    }  else {
                        return FutureUtils.value(null);
                    }
                });

        LedgerHandle w1 = new LedgerHandle(clientCtx1, 1, md1,
                BookKeeper.DigestType.CRC32C,
                ClientUtil.PASSWD, WriteFlag.NONE);
        w1.setLastAddConfirmed(0);

        //  Setup w2
        MockClientContext clientCtx2 = MockClientContext.create(mockBookies);
        Versioned<LedgerMetadata> md2 = setupLedger(clientCtx2, 1, Lists.newArrayList(b1, b2, b3));

        CompletableFuture<Void> blockB1ReadLac = new CompletableFuture<>();
        CompletableFuture<Void> blockB2ReadLac = new CompletableFuture<>();
        CompletableFuture<Void> blockB3ReadLac = new CompletableFuture<>();

        CompletableFuture<Void> blockB1ReadEntry0 = new CompletableFuture<>();
        CompletableFuture<Void> blockB2ReadEntry0 = new CompletableFuture<>();
        CompletableFuture<Void> blockB3ReadEntry0 = new CompletableFuture<>();

        AtomicBoolean isB1LacRead = new AtomicBoolean(true);
        AtomicBoolean isB2LacRead = new AtomicBoolean(true);
        AtomicBoolean isB3LacRead = new AtomicBoolean(true);
        clientCtx2.getMockBookieClient().setPreReadHook(
                (bookie, ledgerId, entryId) -> {
                    if (bookie.equals(b1)) {
                        if (isB1LacRead.get()) {
                            isB1LacRead.set(false);
                            return blockB1ReadLac;
                        } else {
                            return blockB1ReadEntry0;
                        }
                    } else if (bookie.equals(b2)) {
                        if (isB2LacRead.get()) {
                            try {
                                isB2LacRead.set(false);
                                blockB2ReadLac.get();
                            } catch (Throwable t){}
                            return FutureUtils.exception(new BKException.BKWriteException());
                        } else {
                            return blockB2ReadEntry0;
                        }
                    } else if (bookie.equals(b3)) {
                        if (isB3LacRead.get()) {
                            isB3LacRead.set(false);
                            return blockB2ReadLac;
                        } else {
                            return blockB3ReadEntry0;
                        }
                    }  else {
                        return FutureUtils.value(null);
                    }
                });

        AtomicInteger w2MetaUpdates = new AtomicInteger(0);
        CompletableFuture<Void> blockW2StartingRecovery = new CompletableFuture<>();
        CompletableFuture<Void> blockW2ClosingLedger = new CompletableFuture<>();
        clientCtx2.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
            if (w2MetaUpdates.get() == 0) {
                return blockW2StartingRecovery;
            } else {
                return blockW2ClosingLedger;
            }
        });

        ReadOnlyLedgerHandle w2 = new ReadOnlyLedgerHandle(
                clientCtx2, 1L, md2, BookKeeper.DigestType.CRC32C, PASSWD, false);

        // Start an async add entry, blocked for now.
        CompletableFuture<Object> w1WriteFuture = new CompletableFuture<>();
        AtomicInteger writeResult = new AtomicInteger(0);
        w1.asyncAddEntry("e1".getBytes(), (int rc, LedgerHandle lh1, long entryId, Object ctx) -> {
            if (rc == BKException.Code.OK) {
                writeResult.set(1);
            } else {
                writeResult.set(2);
            }
            SyncCallbackUtils.finish(rc, null, w1WriteFuture);
        }, null);
        Thread.sleep(50);

        // Step 1. w2 starts recovery
        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        w2.recover(recoveryPromise, null, false);
        blockW2StartingRecovery.complete(null);
        Thread.sleep(50);

        // Step 2. w2 fencing read LAC reaches B1
        blockB1ReadLac.complete(null);
        Thread.sleep(50);

        // Step 3. w1 add e0 reaches B3
        blockB3Write.complete(null);
        Thread.sleep(50);

        // Step 4. w2 fencing LAC read does not reach B2 or it fails
        blockB2ReadLac.complete(null);
        Thread.sleep(50);

        // Step 5. w2 fencing LAC read reaches B3
        blockB3ReadLac.complete(null);
        Thread.sleep(50);

        // Step 6. w2 sends read e0 to b1, gets NoSuchLedger
        blockB1ReadEntry0.complete(null);
        Thread.sleep(50);

        // Step 7. w2 send read e0 to b2, gets NoSuchLedger
        blockB2ReadEntry0.complete(null);
        Thread.sleep(50);

        // Step 8. w2 closes ledger because (Qw-Qa)+1 bookies confirmed they do not have it
        // last entry id set to 0
        blockW2ClosingLedger.complete(null);
        Thread.sleep(50);

        // Step 9. w1 add e0 reaches b2 (which was fenced by a recovery read)
        blockB2Write.complete(null);
        Thread.sleep(50);

        // Step 10. w1 write fails to reach AckQuorum
        try {
            w1WriteFuture.get(200, TimeUnit.MILLISECONDS);
            Assert.fail("The write to b2 should have failed as it was fenced by the recovery read of step 7");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof BKException.BKLedgerFencedException);
        }

        // w1 received negative acknowledgement of e0 being written
        Assert.assertEquals(1, w1.getLedgerMetadata().getAllEnsembles().size());
        Assert.assertEquals(2, writeResult.get());
        Assert.assertEquals(0L, w1.getLastAddConfirmed());
        // w2 closed the ledger with only the original entry, not the second one
        Assert.assertEquals(1, w2.getLedgerMetadata().getAllEnsembles().size());
        Assert.assertEquals(0L, w2.getLastAddConfirmed());
    }
}
