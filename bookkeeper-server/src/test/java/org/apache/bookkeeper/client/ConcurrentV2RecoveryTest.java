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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests concurrent attempts to open and recovery a ledger with V2 protocol.
 */
public class ConcurrentV2RecoveryTest extends BookKeeperClusterTestCase  {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentV2RecoveryTest.class);
    private final DigestType digestType;

    public ConcurrentV2RecoveryTest() {
        super(4);
        this.digestType = DigestType.CRC32;
    }

    @Test
    void concurrentOpen() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri())
            .setNumChannelsPerBookie(16)
            .setUseV2WireProtocol(true)
            .setZkTimeout(20000)
            .setAddEntryTimeout(30)
            .setReadEntryTimeout(30)
            .setSpeculativeReadTimeout(0)
            .setThrottleValue(0)
            .setLedgerManagerFactoryClassName(HierarchicalLedgerManagerFactory.class.getName());

        BookKeeper bkc = new BookKeeper(conf);

        for (int j = 0; j < 10; j++) {
            LedgerHandle lh = bkc.createLedger(DigestType.CRC32, "testPasswd".getBytes());
            lh.addEntry("foobar".getBytes());

            long ledgerId = lh.getId();
            final long finalLedgerId = ledgerId;
            ExecutorService executor = Executors.newFixedThreadPool(10);
            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);
            for (int i = 0; i < 5; i++) {
                final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
                executor.submit(() -> {
                            latch.await();

                            bkc.asyncOpenLedger(finalLedgerId,
                                                DigestType.CRC32, "testPasswd".getBytes(),
                                                (rc, handle, ctx) -> {
                                                    if (rc != BKException.Code.OK) {
                                                        future.completeExceptionally(BKException.create(rc));
                                                    } else {
                                                        future.complete(handle);
                                                    }
                                                }, null);
                            return future;
                        });
                futures.add(future);
            }

            latch.countDown();
            for (Future<?> f : futures) {
                try {
                    f.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException ee) {
                    // also fine, recovery can currently fail because of metadata conflicts.
                    // We should fix this at some point by making the metadata immutable,
                    // and restarting the entire operation
                    assertEquals(BKException.BKLedgerRecoveryException.class, ee.getCause().getClass());
                }
            }
        }
        bkc.close();
    }
}
