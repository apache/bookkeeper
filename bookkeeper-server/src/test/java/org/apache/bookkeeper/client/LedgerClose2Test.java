/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger recovery tests using mocks rather than a real cluster.
 */
public class LedgerClose2Test {
    private static final Logger log = LoggerFactory.getLogger(LedgerRecovery2Test.class);

    private static final BookieSocketAddress b1 = new BookieSocketAddress("b1", 3181);
    private static final BookieSocketAddress b2 = new BookieSocketAddress("b2", 3181);
    private static final BookieSocketAddress b3 = new BookieSocketAddress("b3", 3181);
    private static final BookieSocketAddress b4 = new BookieSocketAddress("b4", 3181);
    private static final BookieSocketAddress b5 = new BookieSocketAddress("b5", 3181);

    @Test
    public void testTryAddAfterCloseHasBeenCalled() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();

        for (int i = 0; i < 1000; i++) {
            Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, i,
                    LedgerMetadataBuilder.create().newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
            LedgerHandle lh = new LedgerHandle(clientCtx, i, md, BookKeeper.DigestType.CRC32C,
                                               ClientUtil.PASSWD, WriteFlag.NONE);
            CompletableFuture<?> closeFuture = lh.closeAsync();
            try {
                long eid = lh.append("entry".getBytes());

                // if it succeeds, it should be in final ledge
                closeFuture.get();
                Assert.assertTrue(lh.getLedgerMetadata().isClosed());
                Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), eid);
            } catch (BKException.BKLedgerClosedException bke) {
                closeFuture.get();
                Assert.assertTrue(lh.getLedgerMetadata().isClosed());
                Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), LedgerHandle.INVALID_ENTRY_ID);
            }
        }
    }

    @Test
    public void testMetadataChangedDuringClose() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));


        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        lh.append("entry1".getBytes());
        clientCtx.getMockRegistrationClient().addBookies(b4).get();
        clientCtx.getMockBookieClient().errorBookies(b3);
        lh.append("entry2".getBytes());

        CompletableFuture<Void> closeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockClose = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                 // block the write trying to replace b3 with b4
                if (metadata.isClosed()) {
                    closeInProgress.complete(null);
                    return blockClose;
                } else {
                    return FutureUtils.value(null);
                }
            });
        CompletableFuture<?> closeFuture = lh.closeAsync();
        closeInProgress.get();

        ClientUtil.transformMetadata(clientCtx, 10L,
                                     (metadata) -> LedgerMetadataBuilder.from(metadata).replaceEnsembleEntry(
                                             0L, Lists.newArrayList(b4, b2, b5)).build());

        blockClose.complete(null);
        closeFuture.get();

        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 2);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b4, b2, b5));
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L), Lists.newArrayList(b1, b2, b4));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), 1L);
    }

    @Test
    public void testMetadataCloseWithCorrectLengthDuringClose() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));


        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        long lac = lh.append("entry1".getBytes());
        long length = lh.getLength();

        CompletableFuture<Void> closeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockClose = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                // block the write trying to do the first close
                if (!closeInProgress.isDone() && metadata.isClosed()) {
                    closeInProgress.complete(null);
                    return blockClose;
                } else {
                    return FutureUtils.value(null);
                }
            });
        CompletableFuture<?> closeFuture = lh.closeAsync();
        closeInProgress.get();

        ClientUtil.transformMetadata(clientCtx, 10L,
                (metadata) -> LedgerMetadataBuilder.from(metadata)
                                     .withClosedState().withLastEntryId(lac).withLength(length).build());

        blockClose.complete(null);
        closeFuture.get();

        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), lac);
        Assert.assertEquals(lh.getLedgerMetadata().getLength(), length);
    }

    @Test
    public void testMetadataCloseWithDifferentLengthDuringClose() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));


        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        long lac = lh.append("entry1".getBytes());
        long length = lh.getLength();

        CompletableFuture<Void> closeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockClose = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                // block the write trying to do the first close
                if (!closeInProgress.isDone() && metadata.isClosed()) {
                    closeInProgress.complete(null);
                    return blockClose;
                } else {
                    return FutureUtils.value(null);
                }
            });
        CompletableFuture<?> closeFuture = lh.closeAsync();
        closeInProgress.get();

        /* close with different length. can happen in cases where there's a write outstanding */
        ClientUtil.transformMetadata(clientCtx, 10L,
                (metadata) -> LedgerMetadataBuilder.from(metadata)
                                     .withClosedState().withLastEntryId(lac + 1).withLength(length + 100).build());

        blockClose.complete(null);
        try {
            closeFuture.get();
            Assert.fail("Close should fail. Ledger has been closed in a state we don't know how to untangle");
        } catch (ExecutionException ee) {
            Assert.assertEquals(ee.getCause().getClass(), BKException.BKMetadataVersionException.class);
        }
    }

    @Test
    public void testMetadataCloseMarkedInRecoveryWhileClosing() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));

        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        long lac = lh.append("entry1".getBytes());
        long length = lh.getLength();

        CompletableFuture<Void> closeInProgress = new CompletableFuture<>();
        CompletableFuture<Void> blockClose = new CompletableFuture<>();
        clientCtx.getMockLedgerManager().setPreWriteHook((ledgerId, metadata) -> {
                // block the write trying to do the first close
                if (metadata.isClosed()) {
                    closeInProgress.complete(null);
                    return blockClose;
                } else {
                    return FutureUtils.value(null);
                }
            });
        CompletableFuture<?> closeFuture = lh.closeAsync();
        closeInProgress.get();

        ClientUtil.transformMetadata(clientCtx, 10L,
                (metadata) -> LedgerMetadataBuilder.from(metadata).withInRecoveryState().build());

        blockClose.complete(null);

        closeFuture.get(); // should override in recovery, since this handle knows what it has written
        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), lac);
        Assert.assertEquals(lh.getLedgerMetadata().getLength(), length);
    }

    @Test
    public void testCloseWhileAddInProgress() throws Exception {
        MockClientContext clientCtx = MockClientContext.create();
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                                                   LedgerMetadataBuilder.create()
                                                   .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                                                   .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        // block all entry writes from completing
        CompletableFuture<Void> writesHittingBookies = new CompletableFuture<>();
        clientCtx.getMockBookieClient().setPreWriteHook((bookie, ledgerId, entryId) -> {
                writesHittingBookies.complete(null);
                return new CompletableFuture<Void>();
            });
        LedgerHandle lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C,
                                           ClientUtil.PASSWD, WriteFlag.NONE);
        CompletableFuture<?> future = lh.appendAsync("entry1".getBytes());
        writesHittingBookies.get();

        lh.close();
        try {
            future.get();
            Assert.fail("That write shouldn't have succeeded");
        } catch (ExecutionException ee) {
            Assert.assertEquals(ee.getCause().getClass(), BKException.BKLedgerClosedException.class);
        }
        Assert.assertTrue(lh.getLedgerMetadata().isClosed());
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().size(), 1);
        Assert.assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        Assert.assertEquals(lh.getLedgerMetadata().getLastEntryId(), LedgerHandle.INVALID_ENTRY_ID);
        Assert.assertEquals(lh.getLedgerMetadata().getLength(), 0);
    }
}

