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
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a quorum timeout for add entry operations.
 */
public class TestAddEntryQuorumTimeout extends BookKeeperClusterTestCase implements AddCallback {

    private static final Logger logger = LoggerFactory.getLogger(TestAddEntryQuorumTimeout.class);

    final DigestType digestType;
    final byte[] testPasswd = "".getBytes();

    public TestAddEntryQuorumTimeout() {
        super(3);
        baseClientConf.setAddEntryTimeout(10);
        baseClientConf.setAddEntryQuorumTimeout(1);
        this.digestType = DigestType.CRC32;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
    }

    private static class SyncObj {
        volatile int counter = 0;
        volatile int rc = -1;
        public SyncObj() {
            counter = 0;
        }
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        synchronized (x) {
            x.rc = rc;
            x.counter++;
            x.notify();
        }
    }

    @Test
    public void testBasicTimeout() throws Exception {
        BookKeeperTestClient bkc = new BookKeeperTestClient(baseClientConf);
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, testPasswd);
        List<BookieId> curEns = lh.getCurrentEnsemble();
        byte[] data = "foobar".getBytes();
        lh.addEntry(data);
        sleepBookie(curEns.get(0), 5).await();
        try {
            lh.addEntry(data);
            fail("should have thrown");
        } catch (BKException.BKAddEntryQuorumTimeoutException ex) {
        }
    }

    private void waitForSyncObj(SyncObj syncObj) throws Exception {
        synchronized (syncObj) {
            while (syncObj.counter < 1) {
                logger.debug("Entries counter = " + syncObj.counter);
                syncObj.wait();
            }
        }
    }

    @Test
    public void testTimeoutWithPendingOps() throws Exception {
        BookKeeperTestClient bkc = new BookKeeperTestClient(baseClientConf);
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, testPasswd);
        List<BookieId> curEns = lh.getCurrentEnsemble();
        byte[] data = "foobar".getBytes();

        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        SyncObj syncObj3 = new SyncObj();

        lh.addEntry(data);
        sleepBookie(curEns.get(0), 5).await();
        lh.asyncAddEntry(data, this, syncObj1);
        lh.asyncAddEntry(data, this, syncObj2);
        lh.asyncAddEntry(data, this, syncObj3);

        waitForSyncObj(syncObj1);
        assertEquals(BKException.Code.AddEntryQuorumTimeoutException, syncObj1.rc);
        waitForSyncObj(syncObj2);
        assertEquals(BKException.Code.AddEntryQuorumTimeoutException, syncObj2.rc);
        waitForSyncObj(syncObj3);
        assertEquals(BKException.Code.AddEntryQuorumTimeoutException, syncObj3.rc);
    }

    @Test
    public void testLedgerClosedAfterTimeout() throws Exception {
        BookKeeperTestClient bkc = new BookKeeperTestClient(baseClientConf);
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, testPasswd);
        List<BookieId> curEns = lh.getCurrentEnsemble();
        byte[] data = "foobar".getBytes();
        CountDownLatch b0latch = sleepBookie(curEns.get(0), 5);
        try {
            lh.addEntry(data);
            fail("should have thrown");
        } catch (BKException.BKAddEntryQuorumTimeoutException ex) {
        }
        b0latch.await();
        try {
            lh.addEntry(data);
            fail("should have thrown");
        } catch (BKException.BKLedgerClosedException ex) {
        }
    }
}
