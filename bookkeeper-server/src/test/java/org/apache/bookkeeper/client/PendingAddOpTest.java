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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
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
}
