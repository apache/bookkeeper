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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test bookie shutdown.
 */
public class BookieShutdownTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookieShutdownTest.class);

    public BookieShutdownTest() {
        super(3);
        baseConf.setAllowEphemeralPorts(false);
    }

    private LedgerHandle lh;
    private int numEntriesToWrite = 200;
    private int maxInt = 2147483647;
    private Random rng = new Random(System.currentTimeMillis());
    private DigestType digestType = DigestType.CRC32;

    class SyncObj {
    }

    /**
     * Tests verifies the bookkeeper shutdown while writing entries.
     * Continuously restarting the bookie server to see all the external
     * resources are releasing properly. BOOKKEEPER-678
     */
    @Test
    public void testBookieRestartContinuously() throws Exception {
        for (int index = 0; index < 10; index++) {
            SyncObj sync = new SyncObj();
            try {
                // Create a ledger
                lh = bkc.createLedger(3, 2, digestType, "aaa".getBytes());
                LOG.info("Ledger ID: " + lh.getId());
                for (int i = 0; i < numEntriesToWrite; i++) {
                    ByteBuffer entry = ByteBuffer.allocate(4);
                    entry.putInt(rng.nextInt(maxInt));
                    entry.position(0);

                    lh.asyncAddEntry(entry.array(),
                            new LedgerEntryAddCallback(), sync);
                }

                LOG.info("Wrote " + numEntriesToWrite
                        + " and now going to fail bookie.");
                // Shutdown one Bookie server and restarting new one to continue
                // writing
                killBookie(0);
                startNewBookie();
                LOG.info("Shutdown one bookie server and started new bookie server...");
            } catch (BKException e) {
                LOG.error("Caught BKException", e);
                fail(e.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Caught InterruptedException", e);
                fail(e.toString());
            }
        }
    }

    private class LedgerEntryAddCallback implements AddCallback {
        @Override
        public void addComplete(int rc, LedgerHandle lh, long entryId,
                Object ctx) {
            SyncObj x = (SyncObj) ctx;
            synchronized (x) {
                x.notify();
            }
        }
    }

    /**
     * Test whether Bookie can be shutdown when the call comes inside bookie thread.
     *
     * @throws Exception
     */
    @Test
    public void testBookieShutdownFromBookieThread() throws Exception {
        ServerConfiguration conf = confByIndex(0);
        killBookie(0);
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch shutdownComplete = new CountDownLatch(1);
        Bookie bookie = new TestBookieImpl(conf) {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Ignore
                }
                triggerBookieShutdown(ExitCode.BOOKIE_EXCEPTION);
            }

            @Override
            synchronized int shutdown(int exitCode) {
                super.shutdown(exitCode);
                shutdownComplete.countDown();
                return exitCode;
            }
        };
        bookie.start();
        // after 1 sec stop .
        Thread.sleep(1000);
        latch.countDown();
        shutdownComplete.await(5000, TimeUnit.MILLISECONDS);
    }
}
