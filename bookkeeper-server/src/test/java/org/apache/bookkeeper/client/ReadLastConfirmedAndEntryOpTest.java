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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.impl.LastConfirmedAndEntryImpl;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.DummyDigestManager;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
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
    private TestOpStatsLogger readLacAndEntryOpLogger;
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
        this.readLacAndEntryOpLogger = testStatsProvider.getOpStatsLogger("readLacAndEntry");
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
        this.distributionSchedule = new RoundRobinDistributionSchedule(3, 3, 2);
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

    /**
     * Test case: handling different speculative responses. one speculative response might return a valid response
     * with a read entry, while the other speculative response might return a valid response without an entry.
     * {@link ReadLastConfirmedAndEntryOp} should handle both responses well.
     *
     * <p>This test case covers {@link https://github.com/apache/bookkeeper/issues/1476}.
     */
    @Test
    public void testSpeculativeResponses() throws Exception {
        final long entryId = 3L;
        final long lac = 2L;

        ByteBuf data = Unpooled.copiedBuffer("test-speculative-responses", UTF_8);
        ByteBufList dataWithDigest = digestManager.computeDigestAndPackageForSending(
            entryId, lac, data.readableBytes(), data);
        byte[] bytesWithDigest = new byte[dataWithDigest.readableBytes()];
        assertEquals(bytesWithDigest.length, dataWithDigest.getBytes(bytesWithDigest));

        final Map<BookieSocketAddress, ReadEntryCallback> callbacks = Collections.synchronizedMap(new HashMap<>());
        doAnswer(invocationOnMock -> {
            BookieSocketAddress address = invocationOnMock.getArgument(0);
            ReadEntryCallback callback = invocationOnMock.getArgument(6);

            log.info("Received read request to bookie {}", address);

            callbacks.put(address, callback);
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
    }

}
