/*
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
 */

package org.apache.bookkeeper.bookie.datainteg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.MockLedgerStorage;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MockTicker;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.MockBookieClient;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.InOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for EntryCopierImpl.
 */
@SuppressWarnings("deprecation")
public class EntryCopierTest {
    private static final Logger log = LoggerFactory.getLogger(EntryCopierTest.class);
    private static final BookieId bookie1 = BookieId.parse("bookie1:3181");
    private static final BookieId bookie2 = BookieId.parse("bookie2:3181");
    private static final BookieId bookie3 = BookieId.parse("bookie3:3181");
    private static final BookieId bookie4 = BookieId.parse("bookie4:3181");
    private static final BookieId bookie5 = BookieId.parse("bookie5:3181");
    private static final BookieId bookie6 = BookieId.parse("bookie6:3181");

    private OrderedExecutor executor = null;

    @Before
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("test").build();
    }

    @After
    public void teardown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCopyFromAvailable() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(2)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        EntryCopier.Batch batch = copier.newBatch(
                ledgerId, metadata);

        CompletableFuture<Long> f1 = batch.copyFromAvailable(0);
        CompletableFuture<Long> f2 = batch.copyFromAvailable(2);
        CompletableFuture<Long> f3 = batch.copyFromAvailable(4);
        CompletableFuture<Long> f4 = batch.copyFromAvailable(10);
        try {
            batch.copyFromAvailable(100);
            Assert.fail("Should have given IllegalArgumentException");
        } catch (IllegalArgumentException ie) {
            // correct
        }

        try {
            batch.copyFromAvailable(-1);
            Assert.fail("Should have given IllegalArgumentException");
        } catch (IllegalArgumentException ie) {
            // correct
        }
        CompletableFuture.allOf(f1, f2, f3, f4).get();

        verify(bookieClient, times(1)).readEntry(eq(bookie2), eq(ledgerId), eq(0L),
                                                 any(), any(), anyInt(), any());
        verify(bookieClient, times(1)).readEntry(eq(bookie2), eq(ledgerId), eq(2L),
                                                 any(), any(), anyInt(), any());
        verify(bookieClient, times(1)).readEntry(eq(bookie2), eq(ledgerId), eq(4L),
                                                 any(), any(), anyInt(), any());
        verify(bookieClient, times(1)).readEntry(eq(bookie2), eq(ledgerId), eq(10L),
                                                 any(), any(), anyInt(), any());
        verify(bookieClient, times(4)).readEntry(eq(bookie2), eq(ledgerId), anyLong(),
                                                 any(), any(), anyInt(), any());

        verify(storage, times(4)).addEntry(any());
        assertThat(storage.entryExists(ledgerId, 0), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 2), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 4), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 10), equalTo(true));
    }

    @Test
    public void testNoCopiesAvailable() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(1)
            .withWriteQuorumSize(1)
            .withAckQuorumSize(1)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        EntryCopier.Batch batch = copier.newBatch(
                ledgerId, metadata);
        List<CompletableFuture<Long>> futures = Lists.newArrayList();
        for (long l = 0; l < 10; l++) {
            futures.add(batch.copyFromAvailable(l));
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
            Assert.fail("Should have failed");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(BKException.BKReadException.class));
        }
    }

    @Test
    public void testCopyOneEntryFails() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(2)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        bookieClient.setPreReadHook((bookie, ledger, entry) -> {
                if (entry == 2L) {
                    return FutureUtils.exception(new BKException.BKTimeoutException());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        EntryCopier.Batch batch = copier.newBatch(ledgerId, metadata);

        CompletableFuture<Long> f1 = batch.copyFromAvailable(0);
        CompletableFuture<Long> f2 = batch.copyFromAvailable(2);
        CompletableFuture<Long> f3 = batch.copyFromAvailable(4);
        CompletableFuture<Long> f4 = batch.copyFromAvailable(10);

        try {
            CompletableFuture.allOf(f1, f2, f3, f4).get();
            Assert.fail("Should have failed");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(BKException.BKTimeoutException.class));
        }

        // other entries should still have been added
        verify(storage, times(3)).addEntry(any());
        assertThat(storage.entryExists(ledgerId, 0), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 4), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 10), equalTo(true));
    }

    @Test
    public void testCopyAllEntriesFail() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(2)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        bookieClient.setPreReadHook((bookie, ledger, entry) ->
                                    FutureUtils.exception(new BKException.BKTimeoutException()));
        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        EntryCopier.Batch batch = copier.newBatch(ledgerId, metadata);

        CompletableFuture<Long> f1 = batch.copyFromAvailable(0);
        CompletableFuture<Long> f2 = batch.copyFromAvailable(2);
        CompletableFuture<Long> f3 = batch.copyFromAvailable(4);
        CompletableFuture<Long> f4 = batch.copyFromAvailable(10);

        try {
            CompletableFuture.allOf(f1, f2, f3, f4).get();
            Assert.fail("Should have failed");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(BKException.BKTimeoutException.class));
        }

        // Nothing should have been added
        verify(storage, times(0)).addEntry(any());
    }

    @Test
    public void testCopyOneEntryFailsOnStorage() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public long addEntry(ByteBuf buffer) throws IOException, BookieException {
                    long entryId = buffer.getLong(buffer.readerIndex() + 8);
                    if (entryId == 0L) {
                        throw new IOException("failing");
                    }
                    return super.addEntry(buffer);
                }
            });
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(2)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        EntryCopier.Batch batch = copier.newBatch(ledgerId, metadata);

        CompletableFuture<Long> f1 = batch.copyFromAvailable(0);
        CompletableFuture<Long> f2 = batch.copyFromAvailable(2);
        CompletableFuture<Long> f3 = batch.copyFromAvailable(4);
        CompletableFuture<Long> f4 = batch.copyFromAvailable(10);

        try {
            CompletableFuture.allOf(f1, f2, f3, f4).get();
            Assert.fail("Should have failed");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(IOException.class));
        }

        // other entries should still have been added
        verify(storage, times(4)).addEntry(any());
        assertThat(storage.entryExists(ledgerId, 0), equalTo(false));
        assertThat(storage.entryExists(ledgerId, 2), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 4), equalTo(true));
        assertThat(storage.entryExists(ledgerId, 10), equalTo(true));
    }

    @Test
    public void testCopyAllEntriesFailOnStorage() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public long addEntry(ByteBuf buffer) throws IOException, BookieException {
                    throw new IOException("failing");
                }
            });
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(2)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        EntryCopier.Batch batch = copier.newBatch(ledgerId, metadata);

        CompletableFuture<Long> f1 = batch.copyFromAvailable(0);
        CompletableFuture<Long> f2 = batch.copyFromAvailable(2);
        CompletableFuture<Long> f3 = batch.copyFromAvailable(4);
        CompletableFuture<Long> f4 = batch.copyFromAvailable(10);

        try {
            CompletableFuture.allOf(f1, f2, f3, f4).get();
            Assert.fail("Should have failed");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(IOException.class));
        }

        // other entries should still have been added
        verify(storage, times(4)).addEntry(any());
        assertThat(storage.entryExists(ledgerId, 0), equalTo(false));
        assertThat(storage.entryExists(ledgerId, 2), equalTo(false));
        assertThat(storage.entryExists(ledgerId, 4), equalTo(false));
        assertThat(storage.entryExists(ledgerId, 10), equalTo(false));
    }

    @Test
    public void testReadOneEntry() throws Exception {
        long ledgerId = 0xbeeb; // don't change, the shuffle for preferred bookies uses ledger id as seed
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(3)
            .withWriteQuorumSize(3)
            .withAckQuorumSize(3)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2, bookie3))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        EntryCopier copier = new EntryCopierImpl(bookie2, bookieClient,
                                                 new MockLedgerStorage(), new MockTicker());
        EntryCopierImpl.BatchImpl batch = (EntryCopierImpl.BatchImpl) copier.newBatch(ledgerId, metadata);
        for (int i = 0; i <= 10; i++) {
            batch.fetchEntry(i).get();
            verify(bookieClient, times(i + 1)).readEntry(any(), anyLong(), anyLong(),
                                                       any(), any(), anyInt());
            verify(bookieClient, times(i + 1)).readEntry(eq(bookie3), anyLong(), anyLong(),
                                                       any(), any(), anyInt());
        }
    }

    @Test
    public void testReadOneFirstReplicaFails() throws Exception {
        long ledgerId = 0xbeeb; // don't change, the shuffle for preferred bookies uses ledger id as seed
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(3)
            .withWriteQuorumSize(3)
            .withAckQuorumSize(3)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2, bookie3))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);
        bookieClient.errorBookies(bookie3);
        MockTicker ticker = new MockTicker();
        EntryCopierImpl copier = new EntryCopierImpl(bookie2, bookieClient,
                                                     new MockLedgerStorage(), ticker);
        CompletableFuture<Void> errorProcessedPromise = new CompletableFuture<>();
        EntryCopierImpl.BatchImpl batch = copier.new BatchImpl(bookie2, ledgerId,
                                                               metadata,
                                                               new EntryCopierImpl.SinBin(ticker)) {
                @Override
                void notifyBookieError(BookieId bookie) {
                    super.notifyBookieError(bookie);
                    errorProcessedPromise.complete(null);
                }
            };

        batch.fetchEntry(0).get();

        // will read twice, fail at bookie3, succeed at bookie1
        verify(bookieClient, times(2)).readEntry(any(), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        verify(bookieClient, times(1)).readEntry(eq(bookie3), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        verify(bookieClient, times(1)).readEntry(eq(bookie1), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        errorProcessedPromise.get(10, TimeUnit.SECONDS);
        batch.fetchEntry(1).get();

        // subsequent read should go straight for bookie1
        verify(bookieClient, times(3)).readEntry(any(), anyLong(), anyLong(),
                                                       any(), any(), anyInt());
        verify(bookieClient, times(2)).readEntry(eq(bookie1), anyLong(), anyLong(),
                                                   any(), any(), anyInt());
    }

    @Test
    public void testReadOneAllReplicasFail() throws Exception {
        long ledgerId = 0xbeeb; // don't change, the shuffle for preferred bookies uses ledger id as seed
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(3)
            .withWriteQuorumSize(3)
            .withAckQuorumSize(3)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2, bookie3))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);
        // we expect to try to read from bookie3 first
        bookieClient.setPreReadHook((bookie, ledgerId1, entryId) -> {
                if (bookie.equals(bookie1)) {
                    return FutureUtils.exception(new BKException.BKReadException());
                } else if (bookie.equals(bookie3)) {
                    return FutureUtils.exception(new BKException.BKBookieException());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
        EntryCopier copier = new EntryCopierImpl(bookie2, bookieClient,
                                                 new MockLedgerStorage(), new MockTicker());
        EntryCopierImpl.BatchImpl batch = (EntryCopierImpl.BatchImpl) copier.newBatch(ledgerId, metadata);

        try {
            batch.fetchEntry(0).get();
            Assert.fail("Shouldn't get this far");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(BKException.BKBookieException.class));
        }

        InOrder inOrder = inOrder(bookieClient);
        inOrder.verify(bookieClient, times(1)).readEntry(eq(bookie3), anyLong(), anyLong(),
                                                         any(), any(), anyInt());
        inOrder.verify(bookieClient, times(1)).readEntry(eq(bookie1), anyLong(), anyLong(),
                                                         any(), any(), anyInt());
    }

    @Test
    public void testReadOneWithErrorBookieReinstatedAfterSinBin() throws Exception {
        long ledgerId = 0xbeeb; // don't change, the shuffle for preferred bookies uses ledger id as seed
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(3)
            .withWriteQuorumSize(3)
            .withAckQuorumSize(3)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2, bookie3))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);
        bookieClient.errorBookies(bookie3);

        CompletableFuture<Void> errorProcessedPromise = new CompletableFuture<>();

        MockTicker ticker = new MockTicker();
        EntryCopierImpl copier = new EntryCopierImpl(bookie2, bookieClient,
                                                     new MockLedgerStorage(), ticker);
        EntryCopierImpl.SinBin sinBin = new EntryCopierImpl.SinBin(ticker);
        EntryCopierImpl.BatchImpl batch = copier.new BatchImpl(bookie2, ledgerId, metadata, sinBin) {
                @Override
                void notifyBookieError(BookieId bookie) {
                    super.notifyBookieError(bookie);
                    errorProcessedPromise.complete(null);
                }
            };
        batch.fetchEntry(0).get();
        verify(bookieClient, times(1)).readEntry(eq(bookie3), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        verify(bookieClient, times(1)).readEntry(eq(bookie1), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        errorProcessedPromise.get(10, TimeUnit.SECONDS);

        // bookie3 should be fine to use again, but we shouldn't use it until if come out
        // of the sinbin
        bookieClient.removeErrors(bookie3);

        // read batch again, error should carry over
        EntryCopierImpl.BatchImpl batch2 = copier.new BatchImpl(bookie2, ledgerId, metadata, sinBin);
        batch2.fetchEntry(0).get();
        verify(bookieClient, times(1)).readEntry(eq(bookie3), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        verify(bookieClient, times(2)).readEntry(eq(bookie1), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        // advance time
        ticker.advance(70, TimeUnit.SECONDS);

        // sinbinned bookie should be restored, read should come from bookie3 again
        EntryCopierImpl.BatchImpl batch3 = copier.new BatchImpl(bookie2, ledgerId, metadata, sinBin);
        batch3.fetchEntry(0).get();
        verify(bookieClient, times(2)).readEntry(eq(bookie3), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
        verify(bookieClient, times(2)).readEntry(eq(bookie1), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
    }

    @Test
    public void testReadEntryOnlyOnSelf() throws Exception {
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(1)
            .withWriteQuorumSize(1)
            .withAckQuorumSize(1)
            .newEnsembleEntry(0, Lists.newArrayList(bookie2))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        bookieClient.getMockBookies().seedLedger(ledgerId, metadata);

        CompletableFuture<Void> errorProcessedPromise = new CompletableFuture<>();

        MockTicker ticker = new MockTicker();
        EntryCopierImpl copier = new EntryCopierImpl(bookie2, bookieClient,
                                                     new MockLedgerStorage(), ticker);
        EntryCopierImpl.BatchImpl batch = (EntryCopierImpl.BatchImpl) copier.newBatch(ledgerId, metadata);
        try {
            batch.fetchEntry(0).get();
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(BKException.BKReadException.class));
        }
        verify(bookieClient, times(0)).readEntry(any(), anyLong(), anyLong(),
                                                 any(), any(), anyInt());
    }

    @Test
    public void testPreferredBookieIndices() throws Exception {
        long ledgerId = 0xbeeb;
        LedgerMetadata metadata1 = LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(5)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2, bookie3, bookie4, bookie5))
            .newEnsembleEntry(3, Lists.newArrayList(bookie1, bookie6, bookie3, bookie4, bookie5))
            .newEnsembleEntry(5, Lists.newArrayList(bookie1, bookie2, bookie3, bookie4, bookie5))
            .withLastEntryId(10)
            .withLength(1000)
            .withClosedState()
            .build();

        Map<Long, ? extends List<Integer>> order =
            EntryCopierImpl.preferredBookieIndices(bookie2, metadata1,
                                                     Collections.emptySet(),
                                                     ledgerId);
        assertThat(order.get(0L), contains(4, 0, 3, 2));
        assertThat(order.get(3L), contains(4, 1, 0, 3, 2));
        assertThat(order.get(5L), contains(4, 0, 3, 2));

        Map<Long, ? extends List<Integer>> orderWithErr =
            EntryCopierImpl.preferredBookieIndices(bookie2, metadata1,
                                                     Sets.newHashSet(bookie1, bookie3),
                                                     ledgerId);
        assertThat(orderWithErr.get(0L), contains(4, 3, 0, 2));
        assertThat(orderWithErr.get(3L), contains(4, 1, 3, 0, 2));
        assertThat(orderWithErr.get(5L), contains(4, 3, 0, 2));
    }
}
