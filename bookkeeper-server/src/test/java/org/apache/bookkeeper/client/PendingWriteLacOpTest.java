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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link PendingWriteLacOp}.
 */
public class PendingWriteLacOpTest implements AsyncCallback.AddLacCallback {

    private LedgerHandle lh;
    private ClientContext mockClientContext;
    private BookieClient mockBookieClient;
    private boolean callbackInvoked;

    @Before
    public void setup() {
        lh = mock(LedgerHandle.class);
        mockClientContext = mock(ClientContext.class);
        mockBookieClient = mock(BookieClient.class);
        doNothing().when(mockBookieClient).writeLac(any(BookieId.class), anyLong(), any(byte[].class), anyLong(),
                any(ByteBufList.class), any(BookkeeperInternalCallbacks.WriteLacCallback.class), any(Object.class));
        when(mockClientContext.getBookieClient()).thenReturn(mockBookieClient);
        callbackInvoked = false;
    }

    @Test
    public void testWriteLacOp332() {
        // 3-3-2: ack quorum=2, complete after 2 OK responses, release toSend after 3rd response
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(3, 2, 3));

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(3);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(2);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);

        PendingWriteLacOp writeLacOp = new PendingWriteLacOp(lh, mockClientContext,
                lh.getCurrentEnsemble(), this, null);
        writeLacOp.setLac(1000);

        assertEquals(1000, writeLacOp.lac);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 0);
        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 1);

        assertTrue(callbackInvoked);
        assertTrue(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 2);
        assertTrue(writeLacOp.receivedResponseSet.isEmpty());
    }

    @Test
    public void testWriteLacOp333() {
        // 3-3-3: ack quorum=3, complete only after all 3 responses
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(3, 3, 3));

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(3);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(3);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);

        PendingWriteLacOp writeLacOp = new PendingWriteLacOp(lh, mockClientContext,
                lh.getCurrentEnsemble(), this, null);
        writeLacOp.setLac(1000);

        assertEquals(1000, writeLacOp.lac);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 0);
        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 1);

        assertFalse(callbackInvoked);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 2);
        assertTrue(callbackInvoked);
        assertTrue(writeLacOp.completed);
        assertTrue(writeLacOp.receivedResponseSet.isEmpty());
    }

    @Test
    public void testWriteLacOp111() {
        // 1-1-1: single bookie, complete immediately on first response
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(1, 1, 1));

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(1);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(1);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);

        PendingWriteLacOp writeLacOp = new PendingWriteLacOp(lh, mockClientContext,
                lh.getCurrentEnsemble(), this, null);
        writeLacOp.setLac(1000);

        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 0);

        assertTrue(callbackInvoked);
        assertTrue(writeLacOp.completed);
        assertTrue(writeLacOp.receivedResponseSet.isEmpty());
    }

    @Test
    public void testInitiateReleasesBuffer() {
        // Verify toSend buffer is released by initiate() after all requests are sent
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(3, 2, 3));

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(3);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(2);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);
        when(lh.getCurrentEnsemble()).thenReturn(Arrays.asList(BookieId.parse("bookie1"),
                BookieId.parse("bookie2"), BookieId.parse("bookie3")));

        PendingWriteLacOp writeLacOp = new PendingWriteLacOp(lh, mockClientContext,
                lh.getCurrentEnsemble(), this, null);

        writeLacOp.setLac(1000);

        ByteBufList toSend = ByteBufList.get();
        assertEquals(1, toSend.refCnt());

        writeLacOp.initiate(toSend);

        // After initiate(), the caller's reference should be released
        assertEquals(0, toSend.refCnt());
    }

    @Override
    public synchronized void addLacComplete(int rc, LedgerHandle lh, Object ctx) {
        callbackInvoked = true;
    }
}
