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
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
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

}
