package org.apache.bookkeeper.test;

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

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.junit.Before;
import org.junit.Test;

/**
 * Test Create/Delete ledgers
 */
public class LedgerCreateDeleteTest extends BookKeeperClusterTestCase {

    public LedgerCreateDeleteTest() {
        super(1);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        baseConf.setOpenFileLimit(1); 
        super.setUp();
    }

    @Test(timeout=60000)
    public void testCreateDeleteLedgers() throws Exception {
        int numLedgers = 3;
        ArrayList<Long> ledgers = new ArrayList<Long>();
        for (int i=0; i<numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            for (int j=0; j<5; j++) {
                lh.addEntry("just test".getBytes());
            }
            ledgers.add(lh.getId());
            lh.close();
        }
        for (long ledgerId : ledgers) {
            bkc.deleteLedger(ledgerId);
        }
        ledgers.clear();
        Thread.sleep(baseConf.getGcWaitTime() * 2);
        for (int i=0; i<numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            for (int j=0; j<5; j++) {
                lh.addEntry("just test".getBytes());
            }
            ledgers.add(lh.getId());
            lh.close();
        }
    }

    @Test(timeout = 60000)
    public void testCreateLedgerWithBKNotEnoughBookiesException() throws Exception {
        try {
            bkc.createLedger(2, 2, DigestType.CRC32, "bk is cool".getBytes());
            fail("Should be able to throw BKNotEnoughBookiesException");
        } catch (BKException.BKNotEnoughBookiesException bkn) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testCreateLedgerWithZKException() throws Exception {
        stopZKCluster();
        try {
            bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            fail("Should be able to throw ZKException");
        } catch (BKException.ZKException zke) {
            // expected
        }
    }

}
