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

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class TestWatchEnsembleChange extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestWatchEnsembleChange.class);

    final DigestType digestType;

    public TestWatchEnsembleChange() {
        super(7);
        this.digestType = DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testWatchEnsembleChange() throws Exception {
        int numEntries = 10;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        for (int i=0; i<numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
            LOG.info("Added entry {}.", i);
        }
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        long lastLAC = readLh.getLastAddConfirmed();
        assertEquals(numEntries - 2, lastLAC);
        ArrayList<InetSocketAddress> ensemble =
                lh.getLedgerMetadata().currentEnsemble;
        for (InetSocketAddress addr : ensemble) {
            killBookie(addr);
        }
        // write another batch of entries, which will trigger ensemble change
        for (int i=0; i<numEntries; i++) {
            lh.addEntry(("data" + (numEntries + i)).getBytes());
            LOG.info("Added entry {}.", (numEntries + i));
        }
        TimeUnit.SECONDS.sleep(5);
        readLh.readLastConfirmed();
        assertEquals(2 * numEntries - 2, readLh.getLastAddConfirmed());
        readLh.close();
        lh.close();
    }
}
