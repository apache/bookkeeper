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

import static org.apache.bookkeeper.util.TestUtils.assertEventuallyTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.MockBookieClient;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger recovery tests using mocks rather than a real cluster.
 */
public class HandleFailuresTest {
    private static final Logger log = LoggerFactory.getLogger(LedgerRecovery2Test.class);

    private static final BookieId b1 = new BookieSocketAddress("b1", 3181).toBookieId();
    private static final BookieId b2 = new BookieSocketAddress("b2", 3181).toBookieId();
    private static final BookieId b3 = new BookieSocketAddress("b3", 3181).toBookieId();
    private static final BookieId b4 = new BookieSocketAddress("b4", 3181).toBookieId();
    private static final BookieId b5 = new BookieSocketAddress("b5", 3181).toBookieId();

    @Test(timeout = 30000)
    public void testChangeTriggeredOneTimeForOneFailure() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create().newEnsembleEntry(
                                                           0L, Lists.newArrayList(b1, b2, b3)));

        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b1);

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.appendAsync("entry1".getBytes());
        lh.appendAsync("entry2".getBytes());
        lh.appendAsync("entry3".getBytes());
        lh.appendAsync("entry4".getBytes());
        lh.appendAsync("entry5".getBytes()).get();

        verify(clientCtx.getLedgerManager(), times(1)).writeLedgerMetadata(anyLong(), any(), any());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b4, b2, b3));
    }

    @Test(timeout = 30000)
    public void testSecondFailureOccursWhileFirstBeingHandled() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));

        clientCtx.getMockRegistrationClient().addBookies(b4, b5).get();
        CompletableFuture<Void> b2blocker = new CompletableFuture<>();
        clientCtx.getMockBookieClient().setPreWriteHook(
                (bookie, ledgerId, entryId) -> {
                    if (bookie.equals(b1)) {
                        return FutureUtils.exception(new BKException.BKWriteException());
                    } else if (bookie.equals(b2)) {
                        return b2blocker;
                    } else {
                        return FutureUtils.value(null);
                    }
                });
        CompletableFuture<Void> metadataNotifier = new CompletableFuture<>();
        CompletableFuture<Void> metadataBlocker = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook(
                (ledgerId, metadata) -> {
                    metadataNotifier.complete(null);
                    return metadataBlocker;
                });

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.appendAsync("entry1".getBytes());
        lh.appendAsync("entry2".getBytes());
        lh.appendAsync("entry3".getBytes());
        lh.appendAsync("entry4".getBytes());
        CompletableFuture<?> future = lh.appendAsync("entry5".getBytes());

        metadataNotifier.get(); // wait for first metadata write to occur
        b2blocker.completeExceptionally(new BKException.BKWriteException()); // make b2 requests fail
        metadataBlocker.complete(null);

        future.get();
        verify(clientCtx.getLedgerManager(), times(2)).writeLedgerMetadata(anyLong(), any(), any());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).contains(b3));
        Assert.assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).contains(b4));
        Assert.assertTrue(lh.getLedgerMetadata().getAllEnsembles().get(0L).contains(b5));
    }

    @Test(timeout = 30000)
    public void testHandlingFailuresOneBookieFailsImmediately() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b1);

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.append("entry1".getBytes());
        lh.close();

        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b4, b2, b3));
    }

    @Test(timeout = 30000)
    public void testHandlingFailuresOneBookieFailsAfterOneEntry() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockRegistrationClient().addBookies(b4).get();

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.append("entry1".getBytes());
        clientCtx.getMockBookieClient().errorBookies(b1);
        lh.append("entry2".getBytes());
        lh.close();

        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 2);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L), Lists.newArrayList(b4, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), 1L);
    }

    @Test(timeout = 30000)
    public void testHandlingFailuresMultipleBookieFailImmediatelyNotEnoughToReplace() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockBookieClient().errorBookies(b1, b2);

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        try {
            lh.append("entry1".getBytes());
            Assert.fail("Shouldn't have been able to add");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // correct behaviour
            assertEventuallyTrue("Failure to add should trigger ledger closure",
                                 () -> lh.getLedgerMetadata().isClosed());
            Assert.assertEquals("Ledger should be empty",
                                lh.getLedgerMetadata().getLastEntryId(), LedgerHandle.INVALID_ENTRY_ID);
            Assert.assertEquals("Should be only one ensemble", lh.getLedgerMetadata().getAllEnsembles().size(), 1);
            Assert.assertEquals("Ensemble shouldn't have changed", lh.getLedgerMetadata().getAllEnsembles().get(0L),
                                Lists.newArrayList(b1, b2, b3));
        }
    }

    @Test(timeout = 30000)
    public void testHandlingFailuresMultipleBookieFailAfterOneEntryNotEnoughToReplace() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.append("entry1".getBytes());

        clientCtx.getMockBookieClient().errorBookies(b1, b2);

        try {
            lh.append("entry2".getBytes());
            Assert.fail("Shouldn't have been able to add");
        } catch (BKException.BKNotEnoughBookiesException bke) {
            // correct behaviour
            assertEventuallyTrue("Failure to add should trigger ledger closure",
                                 () -> lh.getLedgerMetadata().isClosed());
            Assert.assertEquals("Ledger should be empty", lh.getLedgerMetadata().getLastEntryId(), 0L);
            Assert.assertEquals("Should be only one ensemble", lh.getLedgerMetadata().getAllEnsembles().size(), 1);
            Assert.assertEquals("Ensemble shouldn't have changed", lh.getLedgerMetadata().getAllEnsembles().get(0L),
                                Lists.newArrayList(b1, b2, b3));
        }
    }

    @Test(timeout = 30000)
    public void testClientClosesWhileFailureHandlerInProgress() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b2);

        CompletableFuture<Void> changeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockEnsembleChange = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                // block the write trying to replace b2 with b4
                if (metadata.getAllEnsembles().get(0L).get(1).equals(b4)) {
                    changeInProgress.complete(null);
                    return blockEnsembleChange;
                } else {
                    return FutureUtils.value(null);
                }
            });

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        CompletableFuture<?> future = lh.appendAsync("entry1".getBytes());
        changeInProgress.get();

        lh.close();

        blockEnsembleChange.complete(null); // allow ensemble change to continue
        try {
            future.get();
            Assert.fail("Add shouldn't have succeeded");
        } catch (ExecutionException ee) {
            Assert.assertEquals(ee.getCause().getClass(), BKException.BKLedgerClosedException.class);
        }
        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), LedgerHandle.INVALID_ENTRY_ID);
    }

    @Test(timeout = 30000)
    public void testMetadataSetToClosedDuringFailureHandler() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b2);

        CompletableFuture<Void> changeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockEnsembleChange = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                if (metadata.getAllEnsembles().get(0L).get(1).equals(b4)) {
                    // block the write trying to replace b2 with b4
                    changeInProgress.complete(null);
                    return blockEnsembleChange;
                } else {
                    return FutureUtils.value(null);
                }
            });

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        CompletableFuture<?> future = lh.appendAsync("entry1".getBytes());
        changeInProgress.get();

        ClientUtil.transformMetadata(clientCtx, 10L,
                (metadata) -> LedgerMetadataBuilder.from(metadata)
                                     .withClosedState().withLastEntryId(1234L).withLength(10L).build());

        blockEnsembleChange.complete(null); // allow ensemble change to continue
        try {
            future.get();
            Assert.fail("Add shouldn't have succeeded");
        } catch (ExecutionException ee) {
            Assert.assertEquals(ee.getCause().getClass(), BKException.BKLedgerClosedException.class);
        }
        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), 1234L);
    }

    @Test(timeout = 30000)
    public void testMetadataSetToInRecoveryDuringFailureHandler() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b2);

        CompletableFuture<Void> changeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockEnsembleChange = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                if (metadata.getAllEnsembles().get(0L).get(1).equals(b4)) {
                    // block the write trying to replace b2 with b4
                    changeInProgress.complete(null);
                    return blockEnsembleChange;
                } else {
                    return FutureUtils.value(null);
                }
            });

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        CompletableFuture<?> future = lh.appendAsync("entry1".getBytes());
        changeInProgress.get();

        ClientUtil.transformMetadata(clientCtx, 10L,
                                     (metadata) -> LedgerMetadataBuilder.from(metadata).withInRecoveryState().build());

        blockEnsembleChange.complete(null); // allow ensemble change to continue
        try {
            future.get();
            Assert.fail("Add shouldn't have succeeded");
        } catch (ExecutionException ee) {
            Assert.assertEquals(ee.getCause().getClass(), BKException.BKLedgerFencedException.class);
        }
        Assert.assertFalse(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
    }

    @Test(timeout = 30000)
    public void testOldEnsembleChangedDuringFailureHandler() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.append("entry1".getBytes());
        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b3);
        lh.append("entry2".getBytes());

        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 2);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L), Lists.newArrayList(b1, b2, b4));


        CompletableFuture<Void> changeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockEnsembleChange = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                 // block the write trying to replace b1 with b5
                if (metadata.getAllEnsembles().size() > 2
                    && metadata.getAllEnsembles().get(2L).get(0).equals(b5)) {
                    changeInProgress.complete(null);
                    return blockEnsembleChange;
                } else {
                    return FutureUtils.value(null);
                }
            });

        clientCtx.getMockRegistrationClient().addBookies(b5).get();
        clientCtx.getMockBookieClient().errorBookies(b1);

        CompletableFuture<?> future = lh.appendAsync("entry3".getBytes());
        changeInProgress.get();

        ClientUtil.transformMetadata(clientCtx, 10L,
                                     (metadata) -> LedgerMetadataBuilder.from(metadata).replaceEnsembleEntry(
                                             0L, Lists.newArrayList(b4, b2, b5)).build());

        blockEnsembleChange.complete(null); // allow ensemble change to continue
        future.get();

        Assert.assertFalse(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 3);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b4, b2, b5));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L), Lists.newArrayList(b1, b2, b4));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(2L), Lists.newArrayList(b5, b2, b4));
    }

    @Test(timeout = 30000)
    public void testNoAddsAreCompletedWhileFailureHandlingInProgress() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));

        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b3);

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.append("entry1".getBytes());

        CompletableFuture<Void> changeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockEnsembleChange = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                 // block the write trying to replace b3 with b4
                if (metadata.getAllEnsembles().get(1L).get(2).equals(b4)) {
                    changeInProgress.complete(null);
                    return blockEnsembleChange;
                } else {
                    return FutureUtils.value(null);
                }
            });

        CompletableFuture<?> future = lh.appendAsync("entry2".getBytes());
        changeInProgress.get();
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Shouldn't complete");
        } catch (TimeoutException te) {
        }
        blockEnsembleChange.complete(null);
        future.get();

        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 2);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L), Lists.newArrayList(b1, b2, b4));
    }

    @Test(timeout = 30000)
    public void testHandleFailureBookieNotInWriteSet() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                LedgerMetadataBuilder.create()
                .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(1)
                .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        clientCtx.getMockRegistrationClient().addBookies(b4).get();

        CompletableFuture<Void> b1Delay = new CompletableFuture<>();
        // Delay the first write to b1, then error it
        clientCtx.getMockBookieClient().setPreWriteHook((bookie, ledgerId, entryId) -> {
                if (bookie.equals(b1)) {
                    return b1Delay;
                } else {
                    return FutureUtils.value(null);
                }
            });

        CompletableFuture<Void> changeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockEnsembleChange = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                changeInProgress.complete(null);
                return blockEnsembleChange;
            });

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        log.info("b2 should be enough to complete first add");
        lh.append("entry1".getBytes());

        log.info("when b1 completes with failure, handleFailures should kick off");
        b1Delay.completeExceptionally(new BKException.BKWriteException());

        log.info("write second entry, should have enough bookies, but blocks completion on failure handling");
        AtomicReference<CompletableFuture<?>> e2 = new AtomicReference<>();

        // Execute appendAsync at the same thread of preWriteHook exception thread. So that the
        // `delayedWriteFailedBookies` could update before appendAsync invoke.
        ((MockBookieClient) clientCtx.getBookieClient()).getExecutor()
                .chooseThread(lh.ledgerId)
                .execute(() -> e2.set(lh.appendAsync("entry2".getBytes())));
        changeInProgress.get();
        assertEventuallyTrue("e2 should eventually complete", () -> lh.pendingAddOps.peek().completed);
        Assert.assertFalse("e2 shouldn't be completed to client", e2.get().isDone());
        blockEnsembleChange.complete(null); // allow ensemble change to continue

        log.info("e2 should complete");
        e2.get().get(10, TimeUnit.SECONDS);
    }

}
