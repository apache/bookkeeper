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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger recovery tests using mocks rather than a real cluster.
 */
public class LedgerClose2Test {
    private static final Logger log = LoggerFactory.getLogger(LedgerRecovery2Test.class);

    private static final BookieId b1 = new BookieSocketAddress("b1", 3181).toBookieId();
    private static final BookieId b2 = new BookieSocketAddress("b2", 3181).toBookieId();
    private static final BookieId b3 = new BookieSocketAddress("b3", 3181).toBookieId();
    private static final BookieId b4 = new BookieSocketAddress("b4", 3181).toBookieId();
    private static final BookieId b5 = new BookieSocketAddress("b5", 3181).toBookieId();

    @Test
    void tryAddAfterCloseHasBeenCalled() throws Exception {
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
                assertTrue(lh.getLedgerMetadata().isClosed());
                assertEquals(lh.getLedgerMetadata().getLastEntryId(), eid);
            } catch (BKException.BKLedgerClosedException bke) {
                closeFuture.get();
                assertTrue(lh.getLedgerMetadata().isClosed());
                assertEquals(LedgerHandle.INVALID_ENTRY_ID, lh.getLedgerMetadata().getLastEntryId());
            }
        }
    }

    @Test
    void metadataChangedDuringClose() throws Exception {
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

        assertTrue(lh.getLedgerMetadata().isClosed());
        assertEquals(2, lh.getLedgerMetadata().getAllEnsembles().size());
        assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b4, b2, b5));
        assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(1L), Lists.newArrayList(b1, b2, b4));
        assertEquals(1L, lh.getLedgerMetadata().getLastEntryId());
    }

    @Test
    void metadataCloseWithCorrectLengthDuringClose() throws Exception {
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

        assertTrue(lh.getLedgerMetadata().isClosed());
        assertEquals(1, lh.getLedgerMetadata().getAllEnsembles().size());
        assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        assertEquals(lh.getLedgerMetadata().getLastEntryId(), lac);
        assertEquals(lh.getLedgerMetadata().getLength(), length);
    }

    @Test
    void metadataCloseWithDifferentLengthDuringClose() throws Exception {
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
            fail("Close should fail. Ledger has been closed in a state we don't know how to untangle");
        } catch (ExecutionException ee) {
            assertEquals(BKException.BKMetadataVersionException.class, ee.getCause().getClass());
        }
    }

    @Test
    void metadataCloseMarkedInRecoveryWhileClosing() throws Exception {
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
        assertTrue(lh.getLedgerMetadata().isClosed());
        assertEquals(1, lh.getLedgerMetadata().getAllEnsembles().size());
        assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        assertEquals(lh.getLedgerMetadata().getLastEntryId(), lac);
        assertEquals(lh.getLedgerMetadata().getLength(), length);
    }

    @Test
    void closeWhileAddInProgress() throws Exception {
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
            fail("That write shouldn't have succeeded");
        } catch (ExecutionException ee) {
            assertEquals(BKException.BKLedgerClosedException.class, ee.getCause().getClass());
        }
        assertTrue(lh.getLedgerMetadata().isClosed());
        assertEquals(1, lh.getLedgerMetadata().getAllEnsembles().size());
        assertEquals(lh.getLedgerMetadata().getAllEnsembles().get(0L), Lists.newArrayList(b1, b2, b3));
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, lh.getLedgerMetadata().getLastEntryId());
        assertEquals(0, lh.getLedgerMetadata().getLength());
    }

    @Test
    void doubleCloseOnHandle() throws Exception {
        long ledgerId = 123L;
        MockClientContext clientCtx = MockClientContext.create();

        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, ledgerId,
                LedgerMetadataBuilder.create()
                .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(3)
                .newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));

        CompletableFuture<Void> metadataPromise = new CompletableFuture<>();
        CompletableFuture<Void> clientPromise = new CompletableFuture<>();

        LedgerHandle writer = new LedgerHandle(clientCtx, ledgerId, md,
                                               BookKeeper.DigestType.CRC32C,
                                               ClientUtil.PASSWD, WriteFlag.NONE);
        long eid1 = writer.append("entry1".getBytes());

        log.info("block writes from completing on bookies and metadata");
        clientCtx.getMockBookieClient().setPostWriteHook((bookie, lid, eid) -> clientPromise);
        clientCtx.getMockLedgerManager().setPreWriteHook((lid, metadata) -> metadataPromise);

        log.info("try to add another entry, it will block");
        writer.appendAsync("entry2".getBytes());

        log.info("attempt one close, should block forever");
        CompletableFuture<Void> firstClose = writer.closeAsync();

        log.info("attempt second close, should not finish before first one");
        CompletableFuture<Void> secondClose = writer.closeAsync();

        Thread.sleep(500); // give it a chance to complete, the request jumps around threads
        assertFalse(firstClose.isDone());
        assertFalse(secondClose.isDone());
    }
}

