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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.api.LedgerType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Client side tests on volatile durability ledgers
 */
public class BookKeeperVolatileDurabilityTest extends MockBookKeeperTestCase {

    final static byte[] password = "password".getBytes();
    final static ByteBuf data = Unpooled.wrappedBuffer("foobar".getBytes());
    final static int numEntries = 100;

    @Test
    public void testAddEntryLastAddConfirmedDoesNotAdvance() throws Exception {
        try (WriteHandle wh = result(
            newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(password)
                .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                .execute())) {
            for (int i = 0; i < numEntries - 1; i++) {
                result(wh.append(data));
            }
            long lastEntryID = result(wh.append(data));
            assertEquals(numEntries - 1, lastEntryID);
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(numEntries - 1, lh.getLastAddPushed());
            assertEquals(-1, lh.getLastAddConfirmed());
        }
    }

    @Test
    public void testForceAndAddConfirmedAdvances() throws Exception {
        try (WriteHandle wh = result(
            newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(password)
                .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                .execute())) {
            for (int i = 0; i < numEntries - 1; i++) {
                result(wh.append(data));
            }
            long lastEntryID = result(wh.append(data));
            assertEquals(numEntries - 1, lastEntryID);
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(numEntries - 1, lh.getLastAddPushed());
            assertEquals(-1, lh.getLastAddConfirmed());

            long lastSynced = result(wh.force());
            assertEquals(lastSynced, lh.getLastAddConfirmed());

        }
    }

    @Test
    public void testForceNoEntries() throws Exception {
        try (WriteHandle wh = result(
            newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(password)
                .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                .execute())) {
            assertEquals(Long.valueOf(-1), result(wh.force()));
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(-1, lh.getLastAddConfirmed());
            assertEquals(-1, lh.getLastAddPushed());
        }
    }

    @Test(expected = BKBookieHandleNotAvailableException.class)
    public void testForceAllPausedBookies() throws Exception {
        try (WriteHandle wh = result(
            newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(password)
                .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                .execute())) {
            for (int i = 0; i < numEntries - 1; i++) {
                result(wh.append(data));
            }
            long lastEntryID = result(wh.append(data));
            assertEquals(numEntries - 1, lastEntryID);
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(numEntries - 1, lh.getLastAddPushed());
            assertEquals(-1, lh.getLastAddConfirmed());
            assertEquals(3, lh.getLedgerMetadata().currentEnsemble.size());
            for (BookieSocketAddress addr : lh.getLedgerMetadata().currentEnsemble) {
                pauseBookie(addr);
            }
            result(wh.force());
        }
    }

    @Test
    public void testForceBookiesResumed() throws Exception {
        try (WriteHandle wh = result(
            newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(password)
                .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                .execute())) {
            for (int i = 0; i < numEntries - 1; i++) {
                result(wh.append(data));
            }
            long lastEntryID = result(wh.append(data));
            assertEquals(numEntries - 1, lastEntryID);
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(numEntries - 1, lh.getLastAddPushed());
            assertEquals(-1, lh.getLastAddConfirmed());
            assertEquals(3, lh.getLedgerMetadata().currentEnsemble.size());
            for (BookieSocketAddress addr : lh.getLedgerMetadata().currentEnsemble) {
                pauseBookie(addr);
            }
            try {
                result(wh.force());
                fail("cannot force");
            } catch (BKBookieHandleNotAvailableException expected){
            }
            for (BookieSocketAddress addr : lh.getLedgerMetadata().currentEnsemble) {
                resumeBookie(addr);
            }
            result(wh.force());
            result(wh.append(data));
        }
    }

    @Test
    public void testForceSomePausedBookies() throws Exception {
        try (WriteHandle wh = result(
            newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(password)
                .withLedgerType(LedgerType.FORCE_DEFERRED_ON_JOURNAL)
                .execute())) {
            for (int i = 0; i < numEntries - 1; i++) {
                result(wh.append(data));
            }
            long lastEntryID = result(wh.append(data));
            assertEquals(numEntries - 1, lastEntryID);
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(numEntries - 1, lh.getLastAddPushed());
            assertEquals(-1, lh.getLastAddConfirmed());
            assertEquals(3, lh.getLedgerMetadata().currentEnsemble.size());

            BookieSocketAddress addr = lh.getLedgerMetadata().currentEnsemble.get(0);
            pauseBookie(addr);

            result(wh.force());
        }
    }
}
