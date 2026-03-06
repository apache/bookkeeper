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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link PendingWriteLacOp}.
 */
public class PendingWriteLacOpTest implements AsyncCallback.AddLacCallback {

    private LedgerHandle lh;
    private ClientContext mockClientContext;
    private ByteBufList toSend;
    private boolean callbackInvoked;

    @Before
    public void setup() {
        lh = mock(LedgerHandle.class);

        toSend = ByteBufList.get();
        callbackInvoked = false;
    }

    @Test
    public void testWriteLacOp() {

        // 332
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(3, 2, 3));
        PendingWriteLacOp writeLacOp = new PendingWriteLacOp(lh, mockClientContext,
                lh.getCurrentEnsemble(), this, null);

        LedgerMetadata ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(3);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(2);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);

        writeLacOp.setLac(1000);

        assertEquals(1000, writeLacOp.lac);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.toSend = toSend;
        assertEquals(1, toSend.refCnt());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 0);
        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 1);

        assertTrue(callbackInvoked);
        assertTrue(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());
        assertNotNull(writeLacOp.toSend);

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 2);
        assertTrue(writeLacOp.receivedResponseSet.isEmpty());
        assertNull(writeLacOp.toSend);
        assertEquals(0, toSend.refCnt());

        // 333
        callbackInvoked = false;
        toSend = ByteBufList.get();
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(3, 3, 3));
        writeLacOp = new PendingWriteLacOp(lh, mockClientContext, lh.getCurrentEnsemble(), this, null);

        ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(3);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(3);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);

        writeLacOp.setLac(1000);

        assertEquals(1000, writeLacOp.lac);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.toSend = toSend;
        assertEquals(1, toSend.refCnt());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 0);
        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 1);

        assertFalse(callbackInvoked);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());
        assertNotNull(writeLacOp.toSend);

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 2);
        assertTrue(writeLacOp.receivedResponseSet.isEmpty());
        assertNull(writeLacOp.toSend);
        assertEquals(0, toSend.refCnt());

        // 111
        callbackInvoked = false;
        toSend = ByteBufList.get();
        when(lh.getDistributionSchedule())
                .thenReturn(new RoundRobinDistributionSchedule(1, 1, 1));
        writeLacOp = new PendingWriteLacOp(lh, mockClientContext, lh.getCurrentEnsemble(), this, null);

        ledgerMetadata = mock(LedgerMetadata.class);
        when(ledgerMetadata.getWriteQuorumSize()).thenReturn(1);
        when(ledgerMetadata.getAckQuorumSize()).thenReturn(1);
        when(lh.getLedgerMetadata()).thenReturn(ledgerMetadata);

        writeLacOp.setLac(1000);

        assertEquals(1000, writeLacOp.lac);
        assertFalse(writeLacOp.completed);
        assertFalse(writeLacOp.receivedResponseSet.isEmpty());

        writeLacOp.toSend = toSend;
        assertEquals(1, toSend.refCnt());

        writeLacOp.writeLacComplete(BKException.Code.OK, 2000, null, 0);

        assertTrue(callbackInvoked);
        assertTrue(writeLacOp.completed);
        assertTrue(writeLacOp.receivedResponseSet.isEmpty());
        assertNull(writeLacOp.toSend);
        assertEquals(0, toSend.refCnt());

    }

    @Override
    public synchronized void addLacComplete(int rc, LedgerHandle lh, Object ctx) {
        callbackInvoked = true;
    }
}
