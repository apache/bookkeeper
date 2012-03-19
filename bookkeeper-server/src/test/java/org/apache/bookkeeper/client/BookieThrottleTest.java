package org.apache.bookkeeper.client;

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

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.BookieFailureTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieThrottleTest extends BookKeeperClusterTestCase{

    private static Logger LOG = LoggerFactory.getLogger(BookieFailureTest.class);
    private DigestType digestType;
    private LedgerHandle lh;
    private static CountDownLatch countDownLatch;
    private int throttle = 5;
    private int TIME_OUT = 30;

    // Constructor
    public BookieThrottleTest() {
        super(4);
        this.digestType = DigestType.CRC32;
    }

    @Test
    public void testVerifyPermitRelaseInReadFailure() throws Exception {
        baseClientConf.setThrottleValue(numBookies);
        int numEntries = throttle * 2;
        System.setProperty("throttle", String.valueOf(throttle));

        lh = bkc.createLedger(numBookies, 1, digestType, "".getBytes());
        // Add ledger entries.
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("LedgerId: " + lh.getId() + ", EntryId: " + (i))
                    .getBytes());
        }
        LOG.info("Finished writing all ledger entries so shutdown all the bookies " +
                "to verify the read permits.");

        for (int i = 0; i < numBookies; i++) {
            bs.get(i).shutdown();
        }

        try {
            lh.readEntries(0, throttle-1);
        } catch (BKException e) {
            LOG.info( "Exception when reading the entries, since all bookies are stopped", e);
        }
        LOG.debug("*** READ COMPLETE ***");
        // grace period, just to avoid randomness
        Thread.sleep(2000);
        assertEquals("Permits is not released when read has failed from all replicas",
                throttle, lh.getAvailablePermits().availablePermits());
        lh.close();
    }

    @Test
    public void testVerifyPermitRelaseInAsyncReadFailure() throws Exception {
        baseClientConf.setThrottleValue(numBookies);
        System.setProperty("throttle", String.valueOf(throttle));

        lh = bkc.createLedger(numBookies, 1, digestType, ""
                .getBytes());
        // Add ledger entries.
        int numEntries = throttle * 2;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("LedgerId: " + lh.getId() + ", EntryId: " + (i))
                    .getBytes());
        }
        LOG.info("Finished writing all ledger entries so shutdown all the bookies "+
                "to verify the read permits.");

        for (int i = 0; i < numBookies; i++) {
            bs.get(i).shutdown();
        }
        BookieReadCallback bookieReadCallback = new BookieReadCallback();
        countDownLatch = new CountDownLatch(throttle);
        try {
            lh.asyncReadEntries(0, throttle-1, bookieReadCallback, null);
        } catch (Exception e) {
            LOG.info( "Exception when reading the entries, since all bookies are stopped", e);
        }
        countDownLatch.await(TIME_OUT, TimeUnit.SECONDS);
        LOG.debug("*** READ COMPLETE ***");
        // grace period, just to avoid randomness
        Thread.sleep(2000);
        assertEquals("Permits is not released when read has failed from all replicas",
                throttle, lh.getAvailablePermits().availablePermits());
        lh.close();
    }

    static class BookieReadCallback implements ReadCallback
    {
        @Override
        public void readComplete(int rc, LedgerHandle lh,
                Enumeration<LedgerEntry> seq, Object ctx) {
            assertTrue("Expected Not OK, since all bookies are stopped", rc != BKException.Code.OK);
            countDownLatch.countDown();
        }        
    }
}
