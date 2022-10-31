/**
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Bookie recovery use IO threads.
 */
public class BookieRecoveryUseIOThreadTest extends BookKeeperClusterTestCase {

    public BookieRecoveryUseIOThreadTest() {
        super(1);
    }

    @Override
    public void setUp() throws Exception {
        baseConf.setNumAddWorkerThreads(0);
        baseConf.setNumReadWorkerThreads(0);
        baseConf.setNumHighPriorityWorkerThreads(0);
        super.setUp();
    }

    @Test
    public void testRecoveryClosedLedger() throws BKException, IOException, InterruptedException {
        // test the v2 protocol when using IO thread to handle the request
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setUseV2WireProtocol(true);
        AtomicInteger finalRc = new AtomicInteger(Integer.MAX_VALUE);
        CountDownLatch latch = new CountDownLatch(1);
        try (BookKeeper bkc = new BookKeeper(conf)) {
            bkc.asyncCreateLedger(1, 1, BookKeeper.DigestType.CRC32, "".getBytes(),
                new AsyncCallback.CreateCallback() {
                    @Override
                    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                        lh.asyncAddEntry("hello".getBytes(), new AsyncCallback.AddCallback() {
                            @Override
                            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                                if (rc == BKException.Code.OK) {
                                    bkc.asyncOpenLedger(lh.ledgerId, BookKeeper.DigestType.CRC32, "".getBytes(),
                                        new AsyncCallback.OpenCallback() {
                                            @Override
                                            public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                                                finalRc.set(rc);
                                                latch.countDown();
                                            }
                                        }, null);
                                }
                            }
                        }, null);
                    }
                }, null);
            latch.await();
        }
        Assert.assertEquals(finalRc.get(), org.apache.bookkeeper.client.api.BKException.Code.OK);
    }
}
