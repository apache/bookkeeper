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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link PendingAddOp}.
 */
public class PendingAddOpTest {

    private LedgerHandle lh;
    private ClientContext mockClientContext;

    private ByteBuf payload;

    @Before
    public void setup() {
        BookKeeperClientStats clientStats = BookKeeperClientStats.newInstance(NullStatsLogger.INSTANCE);
        BookieClient bookieClient = mock(BookieClient.class);
        OrderedExecutor mainWorkerPool = mock(OrderedExecutor.class);
        mockClientContext = mock(ClientContext.class);
        when(mockClientContext.getBookieClient()).thenReturn(bookieClient);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.defaultValues());
        when(mockClientContext.getMainWorkerPool()).thenReturn(mainWorkerPool);
        when(mockClientContext.getClientStats()).thenReturn(clientStats);

        lh = mock(LedgerHandle.class);
        when(lh.getDistributionSchedule())
            .thenReturn(new RoundRobinDistributionSchedule(3, 3, 2));
        byte[] data = "test-pending-add-op".getBytes(UTF_8);
        payload = Unpooled.wrappedBuffer(data);
        payload.writerIndex(data.length);
    }

    @Test(timeout = 30000)
    public void testReconcileStaleEnsembleOnNoOpChange() throws Exception {
        checkStaleEnsembleReconciled(Collections.emptySet());
    }

    @Test(timeout = 30000)
    public void testReconcileStaleEnsembleWhenOnlyOtherIndexChanges() throws Exception {
        checkStaleEnsembleReconciled(Collections.singleton(1));
    }

    private void checkStaleEnsembleReconciled(Set<Integer> changedIndices) throws Exception {
        BookieId removed = BookieId.parse("removed:3181");
        BookieId replacement = BookieId.parse("replacement:3181");
        BookieId other = BookieId.parse("other:3181");
        BookieId newOther = BookieId.parse("new-other:3181");
        List<BookieId> target = Arrays.asList(replacement,
                changedIndices.isEmpty() ? other : newOther);
        MockClientContext client = MockClientContext.create();
        CompletableFuture<Void> oldWrite = new CompletableFuture<>();
        try {
            Versioned<LedgerMetadata> metadata = ClientUtil.setupLedger(client, 10L,
                    LedgerMetadataBuilder.create().withEnsembleSize(2).withWriteQuorumSize(2).withAckQuorumSize(2)
                            .newEnsembleEntry(0L, target));
            LedgerHandle handle = new LedgerHandle(client, 10L, metadata, BookKeeper.DigestType.CRC32C,
                    ClientUtil.PASSWD, WriteFlag.NONE);
            client.getMockBookieClient().setPreWriteHook((bookie, ledgerId, entryId) ->
                    bookie.equals(removed) ? oldWrite : CompletableFuture.completedFuture(null));
            CompletableFuture<Integer> first = new CompletableFuture<>();
            // Reproduce the interleaving deterministically: metadata is already updated,
            // but the operation captured the old ensemble before it entered the queue.
            PendingAddOp stale = PendingAddOp.create(handle, client, Arrays.asList(removed, other),
                    Unpooled.wrappedBuffer(new byte[] {1}), WriteFlag.NONE,
                    (rc, lh, entryId, latency, ctx) -> first.complete(rc), null);
            handle.doAsyncAddEntry(stale);
            CompletableFuture<Integer> second = new CompletableFuture<>();
            handle.asyncAddEntry(new byte[] {2}, (rc, lh, entryId, ctx) -> second.complete(rc), null);
            handle.executor.submit(() -> { }).get();
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            handle.executor.submit(() -> handle.unsetSuccessAndSendWriteRequest(target, changedIndices)).get();
            assertEquals(Integer.valueOf(Code.OK), first.get(10, TimeUnit.SECONDS));
            assertEquals(Integer.valueOf(Code.OK), second.get(10, TimeUnit.SECONDS));
            assertEquals(1L, handle.getLastAddConfirmed());
        } finally {
            oldWrite.complete(null);
            client.getMainWorkerPool().shutdown();
            client.getScheduler().shutdown();
        }
    }

    @Test(timeout = 30000)
    public void testMultiIndexAckCleanupAndLateResponsesAcrossChanges() throws Exception {
        MockClientContext client = MockClientContext.create();
        // Control every response explicitly, including replies from older generations.
        client.setBookieClient(mock(BookieClient.class));
        List<BookieId> original = Arrays.asList(BookieId.parse("a:3181"),
                BookieId.parse("b:3181"), BookieId.parse("c:3181"));
        List<BookieId> next = Arrays.asList(BookieId.parse("d:3181"),
                BookieId.parse("e:3181"), BookieId.parse("f:3181"));
        List<BookieId> last = Arrays.asList(BookieId.parse("g:3181"),
                BookieId.parse("h:3181"), BookieId.parse("i:3181"));
        try {
            Versioned<LedgerMetadata> metadata = ClientUtil.setupLedger(client, 11L,
                    LedgerMetadataBuilder.create().withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(2)
                            .newEnsembleEntry(0L, original));
            LedgerHandle handle = new LedgerHandle(client, 11L, metadata, BookKeeper.DigestType.CRC32C,
                    ClientUtil.PASSWD, WriteFlag.NONE);
            AtomicInteger callbacks = new AtomicInteger();
            handle.executor.submit(() -> {
                PendingAddOp[] ops = new PendingAddOp[2];
                handle.changingEnsemble = true;
                handle.pendingAddsSequenceHead = 0;
                for (int entry = 0; entry < ops.length; entry++) {
                    PendingAddOp op = PendingAddOp.create(handle, client, original,
                            Unpooled.wrappedBuffer(new byte[] {1}), WriteFlag.NONE,
                            (rc, lh, eid, latency, ctx) -> {
                                assertEquals(Code.OK, rc);
                                callbacks.incrementAndGet();
                            }, null);
                    ops[entry] = op;
                    op.entryId = entry + 1;
                    op.toSend = op.payload;
                    handle.pendingAddOps.add(op);
                    for (int index = 0; index < 3; index++) {
                        if (handle.distributionSchedule.hasEntry(op.entryId, index)) {
                            op.pendingWriteRequests++;
                            op.writeComplete(Code.OK, 11L, op.entryId, original.get(index), index);
                        }
                    }
                    assertTrue(op.completed);
                }
                handle.changingEnsemble = false;
                Set<Integer> changed = new LinkedHashSet<>(Arrays.asList(0, 2, 1));
                handle.unsetSuccessAndSendWriteRequest(next, changed);
                assertEquals(0, callbacks.get());
                for (PendingAddOp op : ops) {
                    assertFalse(op.completed);
                    assertTrue(op.addEntrySuccessBookies.isEmpty());
                }
                // Some ACKs arrive for the intermediate ensemble before another change.
                ops[0].writeComplete(Code.OK, 11L, 1L, next.get(1), 1);
                handle.unsetSuccessAndSendWriteRequest(last, changed);
                for (PendingAddOp op : ops) {
                    for (int index = 0; index < 3; index++) {
                        if (handle.distributionSchedule.hasEntry(op.entryId, index)) {
                            op.writeComplete(Code.OK, 11L, op.entryId, original.get(index), index);
                            op.writeComplete(Code.WriteException, 11L, op.entryId, next.get(index), index);
                        }
                    }
                    assertFalse(op.completed);
                }
                assertEquals(0, callbacks.get());
                assertFalse(handle.changingEnsemble);
                assertEquals(-1L, handle.getLastAddConfirmed());
                for (PendingAddOp op : ops) {
                    for (int index = 0; index < 3; index++) {
                        if (handle.distributionSchedule.hasEntry(op.entryId, index)) {
                            op.writeComplete(Code.OK, 11L, op.entryId, last.get(index), index);
                        }
                    }
                }
                assertEquals(2, callbacks.get());
                assertEquals(2L, handle.getLastAddConfirmed());
                for (PendingAddOp op : ops) {
                    op.payload.release();
                }
            }).get(10, TimeUnit.SECONDS);
        } finally {
            client.getMainWorkerPool().shutdown();
            client.getScheduler().shutdown();
        }
    }

    @Test(timeout = 30000)
    public void testInitialSendStopsAfterInlineClientClose() throws Exception {
        MockClientContext client = MockClientContext.create();
        client.setBookieClient(mock(BookieClient.class));
        AtomicInteger sends = new AtomicInteger();
        doAnswer(call -> {
            sends.incrementAndGet();
            WriteCallback cb = call.getArgument(5);
            cb.writeComplete(Code.ClientClosedException, call.getArgument(1), call.getArgument(3),
                    call.getArgument(0), call.getArgument(6));
            return null;
        }).when(client.getBookieClient()).addEntry(any(), anyLong(), any(), anyLong(), any(), any(), any(),
                anyInt(), anyBoolean(), any(), any());

        try {
            Versioned<LedgerMetadata> metadata = ClientUtil.setupLedger(client, 14L,
                    LedgerMetadataBuilder.create().withEnsembleSize(2).withWriteQuorumSize(2).withAckQuorumSize(2)
                            .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("a:3181"), BookieId.parse("b:3181"))));
            LedgerHandle handle = new LedgerHandle(client, 14L, metadata, BookKeeper.DigestType.CRC32C,
                    ClientUtil.PASSWD, WriteFlag.NONE);
            AtomicInteger callbacks = new AtomicInteger();
            CompletableFuture<Integer> result = new CompletableFuture<>();
            handle.asyncAddEntry(new byte[] {1}, (rc, lh, entryId, ctx) -> {
                callbacks.incrementAndGet();
                result.complete(rc);
            }, null);

            assertEquals(Integer.valueOf(Code.ClientClosedException), result.get(10, TimeUnit.SECONDS));
            assertEquals(1, callbacks.get());
            assertEquals(1, sends.get());
        } finally {
            client.getMainWorkerPool().shutdown();
            client.getScheduler().shutdown();
        }
    }

    @Test(timeout = 30000)
    public void testAckRemovalRechecksFaultDomains() throws Exception {
        checkResendCompletion(true);
    }

    @Test(timeout = 30000)
    public void testInlineResendCompletionStopsFurtherRetries() throws Exception {
        checkResendCompletion(false);
    }

    private void checkResendCompletion(boolean enforceFaultDomains) throws Exception {
        MockClientContext client = MockClientContext.create();
        client.setBookieClient(mock(BookieClient.class));
        ClientConfiguration conf = new ClientConfiguration();
        conf.setEnforceMinNumFaultDomainsForWrite(enforceFaultDomains);
        client.setConf(ClientInternalConf.fromConfig(conf));
        List<BookieId> original = Arrays.asList(BookieId.parse("a:3181"), BookieId.parse("b:3181"),
                BookieId.parse("c:3181"), BookieId.parse("d:3181"));
        BookieId replacement = BookieId.parse("e:3181");
        List<BookieId> target = enforceFaultDomains
                ? Arrays.asList(original.get(0), original.get(1), replacement, original.get(3))
                : Arrays.asList(replacement, BookieId.parse("f:3181"), original.get(2), original.get(3));
        if (enforceFaultDomains) {
            client.setPlacementPolicy(mock(EnsemblePlacementPolicy.class));
            when(client.getPlacementPolicy().areAckedBookiesAdheringToPlacementPolicy(anySet(), anyInt(), anyInt()))
                    .thenAnswer(call -> {
                        Set<BookieId> acked = call.getArgument(0);
                        return acked.contains(original.get(2)) || acked.contains(replacement);
                    });
        }
        AtomicInteger sends = new AtomicInteger();
        doAnswer(call -> {
            sends.incrementAndGet();
            if (!enforceFaultDomains && (long) call.getArgument(3) == 0L) {
                WriteCallback cb = call.getArgument(5);
                cb.writeComplete(Code.BookieHandleNotAvailableException, 13L, 0L,
                        call.getArgument(0), call.getArgument(6));
            }
            return null;
        }).when(client.getBookieClient()).addEntry(any(), anyLong(), any(), anyLong(), any(), any(), any(),
                anyInt(), anyBoolean(), any(), any());

        try {
            Versioned<LedgerMetadata> metadata = ClientUtil.setupLedger(client, 13L,
                    LedgerMetadataBuilder.create().withEnsembleSize(4).withWriteQuorumSize(4).withAckQuorumSize(2)
                            .newEnsembleEntry(0L, original));
            LedgerHandle handle = new LedgerHandle(client, 13L, metadata, BookKeeper.DigestType.CRC32C,
                    ClientUtil.PASSWD, WriteFlag.NONE);
            handle.executor.submit(() -> {
                CompletableFuture<Integer> result = new CompletableFuture<>();
                PendingAddOp op = PendingAddOp.create(handle, client, original,
                        Unpooled.wrappedBuffer(new byte[] {1}), WriteFlag.NONE,
                        (rc, lh, eid, latency, ctx) -> result.complete(rc), null);
                op.entryId = 0;
                op.hasRun = true;
                op.toSend = op.payload.retain();
                handle.pendingAddOps.add(op);
                handle.changingEnsemble = true;
                List<Integer> acked = enforceFaultDomains ? Arrays.asList(0, 1, 2) : Arrays.asList(2, 3);
                for (int index : acked) {
                    op.pendingWriteRequests++;
                    op.writeComplete(Code.OK, 13L, 0L, original.get(index), index);
                }
                assertTrue(op.completed);
                CompletableFuture<Integer> laterResult = new CompletableFuture<>();
                PendingAddOp later = null;
                if (!enforceFaultDomains) {
                    later = PendingAddOp.create(handle, client, original, Unpooled.wrappedBuffer(new byte[] {2}),
                            WriteFlag.NONE, (rc, lh, eid, latency, ctx) -> laterResult.complete(rc), null);
                    later.entryId = 1;
                    later.hasRun = true;
                    later.toSend = later.payload.retain();
                    handle.pendingAddOps.add(later);
                    for (int index = 0; index < 2; index++) {
                        later.pendingWriteRequests++;
                        later.writeComplete(Code.OK, 13L, 1L, original.get(index), index);
                    }
                    assertTrue(later.completed);
                }
                handle.changingEnsemble = false;
                Set<Integer> changed = enforceFaultDomains ? Collections.singleton(2)
                        : new LinkedHashSet<>(Arrays.asList(0, 1));
                handle.unsetSuccessAndSendWriteRequest(target, changed);
                if (enforceFaultDomains) {
                    assertFalse(result.isDone());
                    assertEquals(-1L, handle.getLastAddConfirmed());
                    op.writeComplete(Code.OK, 13L, 0L, replacement, 2);
                }
                assertEquals(Integer.valueOf(Code.OK), result.getNow(null));
                assertEquals(0L, handle.getLastAddConfirmed());
                assertEquals(enforceFaultDomains ? 1 : 3, sends.get());
                assertNull(op.lh);
                if (later != null) {
                    assertFalse(laterResult.isDone());
                    for (int index = 0; index < 2; index++) {
                        later.writeComplete(Code.OK, 13L, 1L, target.get(index), index);
                    }
                    assertEquals(Integer.valueOf(Code.OK), laterResult.getNow(null));
                    assertEquals(1L, handle.getLastAddConfirmed());
                }
            }).get(10, TimeUnit.SECONDS);
        } finally {
            client.getMainWorkerPool().shutdown();
            client.getScheduler().shutdown();
        }
    }

    @Test(timeout = 30000)
    public void testFailuresQueuedDuringEnsembleChange() throws Exception {
        MockClientContext client = MockClientContext.create();
        List<BookieId> original = Arrays.asList(BookieId.parse("a:3181"), BookieId.parse("b:3181"));
        List<BookieId> target = Arrays.asList(BookieId.parse("c:3181"), BookieId.parse("d:3181"));
        client.setBookieWatcher(mock(BookieWatcher.class));
        BookieWatcher watcher = client.getBookieWatcher();
        when(watcher.replaceBookie(anyInt(),
                anyInt(), anyInt(),
                any(), anyList(),
                anyInt(), anySet()))
                .thenAnswer(call -> target.get((Integer) call.getArgument(5)));
        try {
            Versioned<LedgerMetadata> metadata = ClientUtil.setupLedger(client, 12L,
                    LedgerMetadataBuilder.create().withEnsembleSize(2).withWriteQuorumSize(2).withAckQuorumSize(2)
                            .newEnsembleEntry(0L, original));
            LedgerHandle handle = new LedgerHandle(client, 12L, metadata, BookKeeper.DigestType.CRC32C,
                    ClientUtil.PASSWD, WriteFlag.NONE);
            handle.executor.submit(() -> {
                handle.handleBookieFailure(Collections.singletonMap(0, original.get(0)));
                handle.handleBookieFailure(Collections.singletonMap(1, original.get(1)));
                assertTrue(handle.hasDelayedWriteFailedBookies());
            }).get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() ->
                    handle.executor.submit(() -> !handle.changingEnsemble
                            && handle.getCurrentEnsemble().equals(target)).get());
            assertEquals(target, client.getLedgerManager().readLedgerMetadata(12L).get()
                    .getValue().getAllEnsembles().lastEntry().getValue());
        } finally {
            client.getMainWorkerPool().shutdown();
            client.getScheduler().shutdown();
        }
    }

    @Test
    public void testExecuteAfterCancelled() {
        AtomicInteger rcHolder = new AtomicInteger(-0xdead);
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {
                    rcHolder.set(rc);
                }, null);
        assertSame(lh, op.lh);

        // cancel the op.
        op.submitCallback(Code.NotEnoughBookiesException);
        // if a op is cancelled, it is not recycled until it has been run.
        assertSame(lh, op.lh);
        assertEquals(Code.NotEnoughBookiesException, rcHolder.get());

        op.initiate();
        // after the op is run, the object is recycled.
        assertNull(op.lh);
    }

    /**
     * Verify that {@link PendingAddOp#maybeTimeout()} returns {@code false} without throwing
     * {@link NullPointerException} when {@code clientCtx} is {@code null}.
     *
     * <p>This is the exact scenario that caused the production NPE reported in
     * apache/bookkeeper#4759: {@code monitorPendingAddOps()} holds an iterator reference to
     * an op while {@code recyclePendAddOpObject()} runs concurrently on another thread and
     * clears {@code clientCtx} to {@code null}. After recycling the op has already completed,
     * so the correct behaviour is to skip the timeout check (return {@code false}).
     */
    @Test
    public void testMaybeTimeoutReturnsFalseWhenClientCtxIsNull() {
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);

        // Simulate the race: recyclePendAddOpObject() cleared clientCtx on the writer
        // thread while monitorPendingAddOps() is still iterating the pendingAddOps queue
        // on the scheduler thread.
        op.clientCtx = null;

        // Before the fix this threw NullPointerException; after the fix it must return false.
        assertFalse(op.maybeTimeout());
    }

    /**
     * Verify that {@link PendingAddOp#maybeTimeout()} returns {@code false} when
     * {@code clientCtx} is non-null and the quorum timeout has not yet elapsed.
     */
    @Test
    public void testMaybeTimeoutReturnsFalseWhenWithinQuorumTimeout() {
        // Configure a 1-hour quorum timeout so the op will not have expired.
        ClientConfiguration conf = new ClientConfiguration();
        conf.setAddEntryQuorumTimeout(3600);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.fromConfig(conf));

        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);
        // Stamp the request as starting right now so elapsed time is effectively 0.
        op.requestTimeNanos = MathUtils.nowInNano();

        assertFalse(op.maybeTimeout());
    }

    /**
     * Verify that {@link PendingAddOp#maybeTimeout()} returns {@code true} and triggers
     * {@link PendingAddOp#timeoutQuorumWait()} when the quorum timeout has already elapsed.
     */
    @Test
    public void testMaybeTimeoutReturnsTrueWhenQuorumTimeoutExpired() {
        // Configure a 1-second quorum timeout.
        ClientConfiguration conf = new ClientConfiguration();
        conf.setAddEntryQuorumTimeout(1);
        when(mockClientContext.getConf()).thenReturn(ClientInternalConf.fromConfig(conf));

        LedgerMetadata meta = mock(LedgerMetadata.class);
        when(lh.getLedgerMetadata()).thenReturn(meta);
        // addEntrySuccessBookies starts empty (size 0 < ackQuorumSize 3),
        // so the fault-domain stats branch is skipped and no extra mocking is needed.
        when(meta.getAckQuorumSize()).thenReturn(3);

        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);
        // Set the request start time to 0 so that elapsed nanos is enormous (>> 1 second).
        op.requestTimeNanos = 0L;

        assertTrue(op.maybeTimeout());
    }

    /**
     * Verify that {@link PendingAddOp#timeoutQuorumWait()} is a no-op when the op has already
     * been recycled (i.e. {@code lh} and {@code clientCtx} are {@code null}).
     *
     * <p>This covers the second concern raised in the review of #4760: even after
     * {@link PendingAddOp#maybeTimeout()} captures a non-null {@code clientCtx} snapshot,
     * {@code recyclePendAddOpObject()} may complete before {@code timeoutQuorumWait()}
     * acquires the monitor. The pre-existing {@code if (completed) return;} guard does not
     * cover this because recycling resets {@code completed} to {@code false}; without an
     * explicit null check the method NPEs on {@code lh.getLedgerMetadata()} (or similar)
     * and, worse, may invoke {@code handleUnrecoverableErrorDuringAdd} on a stale handle.
     */
    @Test
    public void testTimeoutQuorumWaitIsNoOpWhenAlreadyRecycled() {
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {}, null);

        // Simulate post-recycle state: recyclePendAddOpObject() has already nulled these fields.
        op.lh = null;
        op.clientCtx = null;

        // Must not throw NPE — and must not invoke the unrecoverable-error handler on the
        // (now-stale) ledger handle, which would cause spurious failures on a recycled op.
        op.timeoutQuorumWait();
        verify(lh, never()).handleUnrecoverableErrorDuringAdd(anyInt());
    }
}
