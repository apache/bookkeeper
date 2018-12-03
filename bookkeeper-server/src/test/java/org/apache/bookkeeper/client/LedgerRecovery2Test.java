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
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
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
    private static final BookieSocketAddress b1 = new BookieSocketAddress("b1", 3181);
    private static final BookieSocketAddress b2 = new BookieSocketAddress("b2", 3181);
    private static final BookieSocketAddress b3 = new BookieSocketAddress("b3", 3181);
    private static final BookieSocketAddress b4 = new BookieSocketAddress("b4", 3181);
    private static final BookieSocketAddress b5 = new BookieSocketAddress("b5", 3181);

    private static Versioned<LedgerMetadata> setupLedger(ClientContext clientCtx, long ledgerId,
                                              List<BookieSocketAddress> bookies) throws Exception {
        LedgerMetadata md = LedgerMetadataBuilder.create()
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

        clientCtx.getMockBookieClient().seedEntries(b1, 1L, 0L, -1L);
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
        clientCtx.getMockBookieClient().seedEntries(b1, 1L, 0L, -1L);
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
        clientCtx.getMockBookieClient().seedEntries(b1, 1L, 0L, -1L);
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
        clientCtx.getMockBookieClient().seedEntries(b1, 1L, 0L, -1L);
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
        clientCtx.getMockBookieClient().seedEntries(b1, 1L, 0L, -1L);
        clientCtx.getMockBookieClient().seedEntries(b3, 1L, 1L, -1L);
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
        clientCtx.getMockBookieClient().seedEntries(b1, 1L, 0L, -1L);
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
}
