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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.LedgerCreateOp.CreateBuilderImpl;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.NullStatsLogger;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Unit tests of builders in this package
 */
public class BookKeeperBuildersTest {

    @Test
    public void testCreateLedgerDefaults() throws Exception {

        int ensembleSize = 3;
        int writeQuorumSize = 3;
        int ackQuorumSize = 3;
        Map<String, byte[]> customMetadata = new HashMap<>();

        BookieWatcher mockWatcher = mock(BookieWatcher.class);
        when(mockWatcher.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata))
                .thenReturn(new ArrayList<>(Arrays.asList(new BookieSocketAddress("localhost", 1234))));

        NullStatsLogger nullStatsLogger = new NullStatsLogger();
        BookKeeper bk = mock(BookKeeper.class);
        when(bk.getCreateOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getCloseLock()).thenReturn(new ReentrantReadWriteLock());
        when(bk.isClosed()).thenReturn(false);
        when(bk.getBookieWatcher()).thenReturn(mockWatcher);
        when(bk.getExplicitLacInterval()).thenReturn(0);

        ClientConfiguration defaultConfig = new ClientConfiguration();

        when(bk.getConf()).thenReturn(defaultConfig);
        when(bk.getStatsLogger()).thenReturn(nullStatsLogger);

        LedgerIdGenerator ledgerIdGenerator = mock(LedgerIdGenerator.class);
        when(bk.getLedgerIdGenerator()).thenReturn(ledgerIdGenerator);

        long mockLedgerId = 12342L;

        Mockito.doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[0];
                cb.operationComplete(BKException.Code.OK, mockLedgerId);
                return null;
            }
        }).when(ledgerIdGenerator).generateLedgerId(any());

        LedgerManager mockLedgerManager = mock(LedgerManager.class);
        AtomicReference<LedgerMetadata> metadataHolder = new AtomicReference<>();

        Mockito.doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
                metadataHolder.set((LedgerMetadata) args[1]);
                cb.operationComplete(BKException.Code.OK, null);
                return null;
            }
        }).when(mockLedgerManager).createLedgerMetadata(eq(mockLedgerId), any(), any());

        when(bk.getLedgerManager()).thenReturn(mockLedgerManager);

        CreateBuilder builder = new CreateBuilderImpl(bk);
        WriteHandle writer = builder
                .withAckQuorumSize(ackQuorumSize)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withCustomMetadata(customMetadata)
                .execute()
                .get();
        assertEquals(mockLedgerId, writer.getId());
        LedgerMetadata metadata = metadataHolder.get();
        assertEquals(ensembleSize, metadata.getEnsembleSize());
        assertEquals(ackQuorumSize, metadata.getAckQuorumSize());
        assertEquals(writeQuorumSize, metadata.getWriteQuorumSize());

    }

}
