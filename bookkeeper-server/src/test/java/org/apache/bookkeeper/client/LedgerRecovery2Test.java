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

import java.nio.charset.StandardCharsets;
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
        /*
            This test uses CompletableFutures to control the sequence of actions performed by
            two writers. There are different sets of futures:
             - block*: These futures block the various reads, writes and metadata updates until the
                       test thread is ready for them to be executed. Thus ensuring the right sequence
                       of events occur.
             - reachedStepN: These futures block in the test thread to ensure that we only unblock
                             an action when the prior one has been executed and we are already blocked
                             on the next actionin the sequence.
         */

        //  Setup w1
        CompletableFuture<Void> reachedStep1 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep2 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep3 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep4 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep5 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep6 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep7 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep8 = new CompletableFuture<>();
        CompletableFuture<Void> reachedStep9 = new CompletableFuture<>();

        MockBookies mockBookies = new MockBookies();
        MockClientContext clientCtx1 = MockClientContext.create(mockBookies);
        Versioned<LedgerMetadata> md1 = setupLedger(clientCtx1, 1, Lists.newArrayList(b1, b2, b3));

        CompletableFuture<Void> blockB1Write = new CompletableFuture<>();
        CompletableFuture<Void> blockB2Write = new CompletableFuture<>();
        CompletableFuture<Void> blockB3Write = new CompletableFuture<>();
        clientCtx1.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    // ignore seed entries e0 and e1
                    if (entryId < 2) {
                        return FutureUtils.value(null);
                    }

                    if (!reachedStep1.isDone()) {
                        reachedStep1.complete(null);
                    }

                    if (bookie.equals(b1)) {
                        return blockB1Write;
                    } else if (bookie.equals(b2)) {
                        reachedStep9.complete(null);
                        return blockB2Write;
                    } else if (bookie.equals(b3)) {
                        reachedStep3.complete(null);
                        return blockB3Write;
                    }  else {
                        return FutureUtils.value(null);
                    }
                });

        LedgerHandle w1 = new LedgerHandle(clientCtx1, 1, md1,
                BookKeeper.DigestType.CRC32C,
                ClientUtil.PASSWD, WriteFlag.NONE);
        w1.addEntry("e0".getBytes(StandardCharsets.UTF_8));
        w1.addEntry("e1".getBytes(StandardCharsets.UTF_8));

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
                            reachedStep2.complete(null);
                            return blockB1ReadLac;
                        } else {
                            reachedStep6.complete(null);
                            return blockB1ReadEntry0;
                        }
                    } else if (bookie.equals(b2)) {
                        if (isB2LacRead.get()) {
                            try {
                                isB2LacRead.set(false);
                                reachedStep4.complete(null);
                                blockB2ReadLac.get(); // block this read - it does not succeed
                            } catch (Throwable t){}
                            return FutureUtils.exception(new BKException.BKWriteException());
                        } else {
                            reachedStep7.complete(null);
                            return blockB2ReadEntry0;
                        }
                    } else if (bookie.equals(b3)) {
                        if (isB3LacRead.get()) {
                            isB3LacRead.set(false);
                            reachedStep5.complete(null);
                            return blockB3ReadLac;
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
                w2MetaUpdates.incrementAndGet();
                return blockW2StartingRecovery;
            } else {
                reachedStep8.complete(null);
                return blockW2ClosingLedger;
            }
        });

        ReadOnlyLedgerHandle w2 = new ReadOnlyLedgerHandle(
                clientCtx2, 1L, md2, BookKeeper.DigestType.CRC32C, PASSWD, false);

        // Start an async add entry, blocked for now.
        CompletableFuture<Object> w1WriteFuture = new CompletableFuture<>();
        AtomicInteger writeResult = new AtomicInteger(0);
        w1.asyncAddEntry("e2".getBytes(), (int rc, LedgerHandle lh1, long entryId, Object ctx) -> {
            if (rc == BKException.Code.OK) {
                writeResult.set(1);
            } else {
                writeResult.set(2);
            }
            SyncCallbackUtils.finish(rc, null, w1WriteFuture);
        }, null);

        // Step 1. w2 starts recovery
        stepBlock(reachedStep1);
        GenericCallbackFuture<Void> recoveryPromise = new GenericCallbackFuture<>();
        w2.recover(recoveryPromise, null, false);
        blockW2StartingRecovery.complete(null);

        // Step 2. w2 fencing read LAC reaches B1
        stepBlock(reachedStep2);
        blockB1ReadLac.complete(null);

        // Step 3. w1 add e0 reaches B3
        stepBlock(reachedStep3);
        blockB3Write.complete(null);

        // Step 4. w2 fencing LAC read does not reach B2 or it fails
        stepBlock(reachedStep4);
        blockB2ReadLac.complete(null);

        // Step 5. w2 fencing LAC read reaches B3
        stepBlock(reachedStep5);
        blockB3ReadLac.complete(null);

        // Step 6. w2 sends read e0 to b1, gets NoSuchLedger
        stepBlock(reachedStep6);
        blockB1ReadEntry0.complete(null);

        // Step 7. w2 send read e0 to b2, gets NoSuchLedger
        stepBlock(reachedStep7);
        blockB2ReadEntry0.complete(null);

        // Step 8. w2 closes ledger because (Qw-Qa)+1 bookies confirmed they do not have it
        // last entry id set to 0
        stepBlock(reachedStep8);
        blockW2ClosingLedger.complete(null);

        // Step 9. w1 add e0 reaches b2 (which was fenced by a recovery read)
        stepBlock(reachedStep9);
        blockB2Write.complete(null);

        // Step 10. w1 write fails to reach AckQuorum
        try {
            w1WriteFuture.get(200, TimeUnit.MILLISECONDS);
            Assert.fail("The write to b2 should have failed as it was fenced by the recovery read of step 7");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof BKException.BKLedgerFencedException);
        }

        // w1 received negative acknowledgement of e2 being written
        Assert.assertEquals(1, w1.getLedgerMetadata().getAllEnsembles().size());
        Assert.assertEquals(2, writeResult.get());
        Assert.assertEquals(1L, w1.getLastAddConfirmed());

        // w2 closed the ledger with only the original entries, not the third one
        // i.e there is no divergence between w1m, w2 and metadata
        Assert.assertEquals(1, w2.getLedgerMetadata().getAllEnsembles().size());
        Assert.assertEquals(1L, w2.getLastAddConfirmed());
    }

    private void stepBlock(CompletableFuture<Void> reachedStepFuture) {
        try {
            reachedStepFuture.get();
        } catch (Exception e) {}
    }
}
