/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.api.WriteFlag.DEFERRED_SYNC;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieShutdownTest;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests about bookie-side implementation of {@link WriteFlag#DEFERRED_SYNC} write flag.
 */
public class DeferredSyncBookieTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookieShutdownTest.class);
    private int numEntriesToWrite = 10;
    private Random rng = new Random(System.currentTimeMillis());

    public DeferredSyncBookieTest() {
        super(1);
        baseConf.setJournalFlushWhenQueueEmpty(true);
    }

    @Test
    public void testEarlyAcknowledgeEntry() throws Exception {
        try (WriteHandle lh = result(bkc.newCreateLedgerOp()
            .withAckQuorumSize(1)
            .withWriteQuorumSize(1)
            .withEnsembleSize(1)
            .withWriteFlags(DEFERRED_SYNC)
            .withDigestType(DigestType.CRC32)
            .withPassword(new byte[]{})
            .execute())) {
            byte[] payload = new byte[10];
            long ledgerLength = 0;
            byte[] masterkey = new byte[10];
            DigestManager macManager = new CRC32DigestManager(lh.getId());
            Bookie bookie = getBookieServer(0).getBookie();
            for (long entryId = 0; entryId < numEntriesToWrite; entryId++) {
                rng.nextBytes(payload);
                ByteBuf entry = Unpooled.copiedBuffer(macManager.computeDigestAndPackageForSending(entryId, -1,
                    ledgerLength, Unpooled.wrappedBuffer(payload)));
                ledgerLength += payload.length;
                CountDownLatch latch = new CountDownLatch(1);
                bookie.addEntry(entry, true, new WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId,
                                              BookieSocketAddress addr, Object ctx) {
                        if (rc == BKException.Code.OK && ledgerId == lh.getId() && entryId == (Long) ctx) {
                            latch.countDown();
                        }
                    }
                }, entryId, masterkey);
                assertTrue(latch.await(1, TimeUnit.MINUTES));
            }
            bookie.shutdown();
        }
    }

}
