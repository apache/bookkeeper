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

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a piggyback LAC.
 */
@RunWith(Parameterized.class)
public class TestPiggybackLAC extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestPiggybackLAC.class);

    final DigestType digestType;

    public TestPiggybackLAC(Class<? extends LedgerStorage> storageClass) {
        super(1);
        this.digestType = DigestType.CRC32;
        baseConf.setLedgerStorageClass(storageClass.getName());
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { InterleavedLedgerStorage.class },
            { SortedLedgerStorage.class },
            { DbLedgerStorage.class },
        });
    }

    @Test
    public void testPiggybackLAC() throws Exception {
        int numEntries = 10;
        LedgerHandle lh = bkc.createLedger(1, 1, 1, digestType, "".getBytes());
        // tried to add entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
            LOG.info("Added entry {}.", i);
        }
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        long lastLAC = readLh.getLastAddConfirmed();
        assertEquals(numEntries - 2, lastLAC);
        // write add entries
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("data" + (i + numEntries)).getBytes());
            LOG.info("Added entry {}.", (i + numEntries));
        }
        int numReads = 0;
        int i = 0;
        while (true) {
            if (i > readLh.getLastAddConfirmed()) {
                break;
            }
            Enumeration<LedgerEntry> data = readLh.readEntries(i, i);
            while (data.hasMoreElements()) {
                LedgerEntry entry = data.nextElement();
                assertEquals("data" + i, new String(entry.getEntry()));
                ++numReads;
            }
            i++;
        }
        assertEquals(2 * numEntries - 1, numReads);
        assertEquals(2 * numEntries - 2, readLh.getLastAddConfirmed());
        readLh.close();
        lh.close();
    }
}
