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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.MockRegistrationClient;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.MockBookies;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
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
        when(lh.getWriteFlags())
                .thenReturn(EnumSet.of(WriteFlag.DEFERRED_SYNC));
        Map<Integer, BookieId> failedBookies = new HashMap<>();
        failedBookies.put(1, BookieId.parse("0.0.0.0:3181"));
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

    @Test
    public void testReadOnlyLedgerHandleWithNotEnoughBookiesExceptionDuringRecoveryAdd() throws Exception {
        final BookieId b1 = new BookieSocketAddress("b1", 3181).toBookieId();
        final BookieId b2 = new BookieSocketAddress("b2", 3181).toBookieId();
        final BookieId b3 = new BookieSocketAddress("b3", 3181).toBookieId();
        MockBookies mockBookies = new MockBookies();
        ClientConfiguration conf = new ClientConfiguration();
        MockRegistrationClient regClient = new MockRegistrationClient();
        EnsemblePlacementPolicy placementPolicy = new DefaultEnsemblePlacementPolicy();
        BookieWatcher bookieWatcher = new MockBookieWatcher(conf, placementPolicy,
                regClient,
                new DefaultBookieAddressResolver(regClient),
                NullStatsLogger.INSTANCE);
        ClientContext clientCtx = MockClientContext.create(mockBookies, conf, regClient, placementPolicy,
                bookieWatcher);
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 0,
                LedgerMetadataBuilder.create().withInRecoveryState().newEnsembleEntry(0L,
                        Lists.newArrayList(b1, b2, b3)));
        ReadOnlyLedgerHandle lh = new ReadOnlyLedgerHandle(clientCtx, 0, md, BookKeeper.DigestType.CRC32C,
                ClientUtil.PASSWD, true);
        lh.notifyWriteFailed(0, b1);
        AtomicInteger rcHolder = new AtomicInteger(-0xdead);
        PendingAddOp op = PendingAddOp.create(
                lh, mockClientContext, lh.getCurrentEnsemble(),
                payload, WriteFlag.NONE,
                (rc, handle, entryId, qwcLatency, ctx) -> {
                    rcHolder.set(rc);
                }, null).enableRecoveryAdd();
        assertSame(lh, op.lh);
        op.setEntryId(0);
        lh.pendingAddOps.add(op);
        lh.clientCtx.getMainWorkerPool().submitOrdered(lh.ledgerId, (Callable<Void>) () -> {
            op.initiate();
            return null;
        }).get();
    }

}
