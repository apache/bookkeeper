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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadOptions;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PendingReadOpTest {

    @Test
    public void testReadOptionsDisableReadAheadSetsBookieReadFlag() throws Exception {
        BookieClient bookieClient = mock(BookieClient.class);

        newPendingReadOp(bookieClient, false, ReadOptions.builder().disableReadAhead(true).build()).initiate();

        ArgumentCaptor<Integer> flags = ArgumentCaptor.forClass(Integer.class);
        verify(bookieClient).readEntry(any(BookieId.class), anyLong(), eq(0L), any(), any(), flags.capture());
        assertEquals(BookieProtocol.FLAG_NO_READ_AHEAD, flags.getValue() & BookieProtocol.FLAG_NO_READ_AHEAD);
    }

    @Test
    public void testDefaultReadOptionsDoNotSetNoReadAheadFlag() throws Exception {
        BookieClient bookieClient = mock(BookieClient.class);

        newPendingReadOp(bookieClient, false, ReadOptions.DEFAULT).initiate();

        ArgumentCaptor<Integer> flags = ArgumentCaptor.forClass(Integer.class);
        verify(bookieClient).readEntry(any(BookieId.class), anyLong(), eq(0L), any(), any(), flags.capture());
        assertEquals(0, flags.getValue() & BookieProtocol.FLAG_NO_READ_AHEAD);
    }

    @Test
    public void testRecoveryReadPreservesExistingFlagsWithNoReadAhead() throws Exception {
        BookieClient bookieClient = mock(BookieClient.class);

        newPendingReadOp(bookieClient, true, ReadOptions.builder().disableReadAhead(true).build()).initiate();

        ArgumentCaptor<Integer> flags = ArgumentCaptor.forClass(Integer.class);
        verify(bookieClient).readEntry(any(BookieId.class), anyLong(), eq(0L), any(), any(),
                flags.capture(), isNull());
        assertEquals(BookieProtocol.FLAG_HIGH_PRIORITY, flags.getValue() & BookieProtocol.FLAG_HIGH_PRIORITY);
        assertEquals(BookieProtocol.FLAG_DO_FENCING, flags.getValue() & BookieProtocol.FLAG_DO_FENCING);
        assertEquals(BookieProtocol.FLAG_NO_READ_AHEAD, flags.getValue() & BookieProtocol.FLAG_NO_READ_AHEAD);
    }

    private static PendingReadOp newPendingReadOp(BookieClient bookieClient,
                                                 boolean recoveryRead,
                                                 ReadOptions readOptions) throws Exception {
        ClientContext clientContext = mock(ClientContext.class);
        ClientConfiguration conf = new ClientConfiguration().setReorderReadSequenceEnabled(false);
        doReturn(ClientInternalConf.fromConfig(conf)).when(clientContext).getConf();
        doReturn(bookieClient).when(clientContext).getBookieClient();

        LedgerHandle ledgerHandle = mock(LedgerHandle.class);
        LedgerMetadata metadata = mock(LedgerMetadata.class);
        BookieId bookie = BookieId.parse("127.0.0.1:3181");
        List<BookieId> ensemble = Collections.singletonList(bookie);
        TreeMap<Long, List<BookieId>> ensembles = new TreeMap<>();
        ensembles.put(0L, ensemble);
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(1, 1, 1);

        doReturn(metadata).when(ledgerHandle).getLedgerMetadata();
        doReturn(1).when(metadata).getWriteQuorumSize();
        doReturn(1).when(metadata).getAckQuorumSize();
        doReturn(1).when(metadata).getEnsembleSize();
        doReturn(ensemble).when(metadata).getEnsembleAt(0L);
        doReturn(ensembles).when(metadata).getAllEnsembles();
        doAnswer(invocation -> schedule.getWriteSet(invocation.getArgument(0))).when(ledgerHandle)
                .getWriteSetForReadOperation(anyLong());

        return new PendingReadOp(ledgerHandle, clientContext, 0, 0, recoveryRead, readOptions)
                .parallelRead(true);
    }
}
