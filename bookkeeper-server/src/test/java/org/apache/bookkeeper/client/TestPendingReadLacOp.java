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

import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.proto.MockBookieClient;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests PendingReadLacOp internals.
 */
public class TestPendingReadLacOp extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestPendingReadLacOp.class);
    byte[] pwd = "asdf".getBytes();
    byte[] data = "foo".getBytes();

    public TestPendingReadLacOp() {
        super(3);
    }

    @Test
    public void testPendingReadLacOpMissingExplicitLAC() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, 2, BookKeeper.DigestType.CRC32, pwd);
        lh.append(data);
        lh.append(data);
        lh.append(data);

        final CompletableFuture<Long> result = new CompletableFuture<>();
        PendingReadLacOp pro = new PendingReadLacOp(lh, bkc.getBookieClient(), lh.getCurrentEnsemble(),
                                                    (rc, lac) -> result.complete(lac)) {
            @Override
            public void initiate() {
                for (int i = 0; i < lh.getCurrentEnsemble().size(); i++) {
                    final int index = i;
                    ReferenceCounted toSend = lh.getDigestManager().computeDigestAndPackageForSending(
                            2,
                            1,
                            data.length,
                            Unpooled.wrappedBuffer(data),
                            new byte[20],
                            0);

                    bkc.scheduler.schedule(() -> {
                        readLacComplete(
                                0,
                                lh.getId(),
                                null,
                                MockBookieClient.copyData(toSend),
                                index);

                    }, 0, TimeUnit.SECONDS);
                    bookieClient.readLac(lh.getCurrentEnsemble().get(i),
                                         lh.ledgerId, this, i);
                }
            }
        };
        pro.initiate();
        assertEquals(1, result.get().longValue());
    }

    @Test
    public void testPendingReadLacOpMissingLAC() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, 2, BookKeeper.DigestType.MAC, pwd);
        lh.append(data);
        lh.append(data);
        lh.append(data);

        final CompletableFuture<Long> result = new CompletableFuture<>();
        PendingReadLacOp pro = new PendingReadLacOp(lh, bkc.getBookieClient(), lh.getCurrentEnsemble(),
            (rc, lac) -> result.complete(lac)) {
            @Override
            public void initiate() {
                for (int i = 0; i < lh.getCurrentEnsemble().size(); i++) {
                    final int index = i;
                    ByteBufList buffer = lh.getDigestManager().computeDigestAndPackageForSendingLac(1);
                    bkc.scheduler.schedule(() -> {
                        readLacComplete(
                                0,
                                lh.getId(),
                                buffer.getBuffer(0),
                                null,
                                index);
                    }, 0, TimeUnit.SECONDS);
                    bookieClient.readLac(lh.getCurrentEnsemble().get(i),
                            lh.ledgerId, this, i);
                }
            }
        };
        pro.initiate();
        assertEquals(result.get().longValue(), 1);
    }
}
