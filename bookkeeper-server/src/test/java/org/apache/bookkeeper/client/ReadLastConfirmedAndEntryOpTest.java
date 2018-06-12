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

package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.impl.LastConfirmedAndEntryImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.ReadLastConfirmedAndEntryContext;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.DummyDigestManager;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ReadLastConfirmedAndEntryOp} with mocks.
 */
@Slf4j
public class ReadLastConfirmedAndEntryOpTest {

    private static final long LEDGERID = System.currentTimeMillis();

    private final TestStatsProvider testStatsProvider = new TestStatsProvider();
    private OpStatsLogger readLacAndEntryOpLogger;
    private BookieClient mockBookieClient;
    private BookKeeper mockBk;
    private LedgerHandle mockLh;
    private ScheduledExecutorService scheduler;
    private OrderedScheduler orderedScheduler;
    private SpeculativeRequestExecutionPolicy speculativePolicy;
    private LedgerMetadata ledgerMetadata;
    private DistributionSchedule distributionSchedule;
    private DigestManager digestManager;

    @Before
    public void setup() throws Exception {
        // stats
        this.readLacAndEntryOpLogger = testStatsProvider
            .getStatsLogger("").getOpStatsLogger("readLacAndEntry");
        // policy
        this.speculativePolicy = new DefaultSpeculativeRequestExecutionPolicy(
            100, 200, 2);
        // metadata
        this.ledgerMetadata =
            new LedgerMetadata(3, 3, 2, DigestType.CRC32, new byte[0]);
        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            ensemble.add(new BookieSocketAddress("127.0.0.1", 3181 + i));
        }
        this.ledgerMetadata.addEnsemble(0L, ensemble);
        this.distributionSchedule = new RoundRobinDistributionSchedule(3, 2, 3);
        // schedulers
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.orderedScheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-ordered-scheduler")
            .numThreads(1)
            .build();

        this.mockBookieClient = mock(BookieClient.class);

        this.mockBk = mock(BookKeeper.class);
        when(mockBk.getReadLACSpeculativeRequestPolicy()).thenReturn(Optional.of(speculativePolicy));
        when(mockBk.getBookieClient()).thenReturn(mockBookieClient);
        when(mockBk.getReadLacAndEntryOpLogger()).thenReturn(readLacAndEntryOpLogger);
        when(mockBk.getMainWorkerPool()).thenReturn(orderedScheduler);
        EnsemblePlacementPolicy mockPlacementPolicy = mock(EnsemblePlacementPolicy.class);
        when(mockBk.getPlacementPolicy()).thenReturn(mockPlacementPolicy);
        this.mockLh = mock(LedgerHandle.class);
        when(mockLh.getBk()).thenReturn(mockBk);
        when(mockLh.getId()).thenReturn(LEDGERID);
        when(mockLh.getLedgerMetadata()).thenReturn(ledgerMetadata);
        when(mockLh.getDistributionSchedule()).thenReturn(distributionSchedule);
        digestManager = new DummyDigestManager(LEDGERID, false);
        when(mockLh.getDigestManager()).thenReturn(digestManager);
    }

    @After
    public void teardown() {
        this.scheduler.shutdown();
        this.orderedScheduler.shutdown();
    }

    @Data
    static class ReadLastConfirmedAndEntryHolder {

        private final BookieSocketAddress address;
        private final ReadEntryCallback callback;
        private final ReadLastConfirmedAndEntryContext context;

    }

    /**
     * Test case: handling different speculative responses. one speculative response might return a valid response
     * with a read entry, while the other speculative response might return a valid response without an entry.
     * {@link ReadLastConfirmedAndEntryOp} should handle both responses well.
     *
     * <p>This test case covers {@link https://github.com/apache/bookkeeper/issues/1476}.
     */
    @Test
    public void testSpeculativeResponses() throws Exception {
        final long entryId = 2L;
        final long lac = 1L;

        ByteBuf data = Unpooled.copiedBuffer("test-speculative-responses", UTF_8);
        ByteBufList dataWithDigest = digestManager.computeDigestAndPackageForSending(
            entryId, lac, data.readableBytes(), data);
        byte[] bytesWithDigest = new byte[dataWithDigest.readableBytes()];
        assertEquals(bytesWithDigest.length, dataWithDigest.getBytes(bytesWithDigest));

        final Map<BookieSocketAddress, ReadLastConfirmedAndEntryHolder> callbacks =
            Collections.synchronizedMap(new HashMap<>());
        doAnswer(invocationOnMock -> {
            BookieSocketAddress address = invocationOnMock.getArgument(0);
            ReadEntryCallback callback = invocationOnMock.getArgument(6);
            ReadLastConfirmedAndEntryContext context = invocationOnMock.getArgument(7);

            ReadLastConfirmedAndEntryHolder holder = new ReadLastConfirmedAndEntryHolder(address, callback, context);

            log.info("Received read request to bookie {}", address);

            callbacks.put(address, holder);
            return null;
        }).when(mockBookieClient).readEntryWaitForLACUpdate(
            any(BookieSocketAddress.class),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyBoolean(),
            any(ReadEntryCallback.class),
            any()
        );

        CompletableFuture<LastConfirmedAndEntry> resultFuture = new CompletableFuture<>();
        LastConfirmedAndEntryCallback resultCallback = (rc, lastAddConfirmed, entry) -> {
            if (Code.OK != rc) {
                FutureUtils.completeExceptionally(resultFuture, BKException.create(rc));
            } else {
                FutureUtils.complete(resultFuture, LastConfirmedAndEntryImpl.create(lastAddConfirmed, entry));
            }
        };

        ReadLastConfirmedAndEntryOp op = new ReadLastConfirmedAndEntryOp(
            mockLh,
            resultCallback,
            1L,
            10000,
            scheduler
        );
        op.initiate();

        // wait until all speculative requests are sent
        while (callbacks.size() < 3) {
            log.info("Received {} read requests", callbacks.size());
            Thread.sleep(100);
        }

        log.info("All speculative reads are outstanding now.");

        // once all the speculative reads are outstanding. complete the requests in following sequence:

        // 1) complete one bookie with empty response (OK, entryId = INVALID_ENTRY_ID)
        // 2) complete second bookie with valid entry response. this will trigger double-release bug described in
        //    {@link https://github.com/apache/bookkeeper/issues/1476}

        Iterator<Entry<BookieSocketAddress, ReadLastConfirmedAndEntryHolder>> iter = callbacks.entrySet().iterator();
        assertTrue(iter.hasNext());
        Entry<BookieSocketAddress, ReadLastConfirmedAndEntryHolder> firstBookieEntry = iter.next();
        ReadLastConfirmedAndEntryHolder firstBookieHolder = firstBookieEntry.getValue();
        ReadLastConfirmedAndEntryContext firstContext = firstBookieHolder.context;
        firstContext.setLastAddConfirmed(entryId);
        firstBookieHolder.getCallback()
            .readEntryComplete(Code.OK, LEDGERID, BookieProtocol.INVALID_ENTRY_ID, null, firstContext);

        // readEntryComplete above will release the entry impl back to the object pools.
        // we want to make sure after the entry is recycled, it will not be mutated by any future callbacks.
        LedgerEntryImpl entry = LedgerEntryImpl.create(LEDGERID, Long.MAX_VALUE);

        assertTrue(iter.hasNext());
        Entry<BookieSocketAddress, ReadLastConfirmedAndEntryHolder> secondBookieEntry = iter.next();
        ReadLastConfirmedAndEntryHolder secondBookieHolder = secondBookieEntry.getValue();
        ReadLastConfirmedAndEntryContext secondContext = secondBookieHolder.context;
        secondContext.setLastAddConfirmed(entryId);
        secondBookieHolder.getCallback().readEntryComplete(
            Code.OK, LEDGERID, entryId, Unpooled.wrappedBuffer(bytesWithDigest), secondContext);

        // the recycled entry shouldn't be updated by any future callbacks.
        assertNull(entry.getEntryBuffer());
        entry.close();

        // wait for results
        try (LastConfirmedAndEntry lacAndEntry = FutureUtils.result(resultFuture)) {
            assertEquals(entryId, lacAndEntry.getLastAddConfirmed());
            assertNull(lacAndEntry.getEntry());
        }
    }

}
