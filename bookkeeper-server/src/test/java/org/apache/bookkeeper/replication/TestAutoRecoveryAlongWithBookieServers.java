/**
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
package org.apache.bookkeeper.replication;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdapter;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.junit.Test;

public class TestAutoRecoveryAlongWithBookieServers extends
        BookKeeperClusterTestCase {

    private String basePath = "";

    public TestAutoRecoveryAlongWithBookieServers() {
        super(3);
        baseConf.setAutoRecoveryDaemonEnabled(true);
        basePath = baseClientConf.getZkLedgersRootPath() + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
    }

    /** Tests that the auto recovery service along with Bookie servers itself */
    @Test(timeout = 60000)
    public void testAutoRecoveryAlongWithBookieServers() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                "testpasswd".getBytes());
        byte[] testData = "testBuiltAutoRecovery".getBytes();

        for (int i = 0; i < 10; i++) {
            lh.addEntry(testData);
        }
        lh.close();
        InetSocketAddress replicaToKill = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(0L).get(0);

        killBookie(replicaToKill);

        int startNewBookie = startNewBookie();
        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);

        while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(),
                basePath)) {
            Thread.sleep(100);
        }

        // Killing all bookies except newly replicated bookie
        Set<Entry<Long, ArrayList<InetSocketAddress>>> entrySet = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().entrySet();
        for (Entry<Long, ArrayList<InetSocketAddress>> entry : entrySet) {
            ArrayList<InetSocketAddress> bookies = entry.getValue();
            for (InetSocketAddress bookie : bookies) {
                if (bookie.equals(newBkAddr)) {
                    continue;
                }
                killBookie(bookie);
            }
        }

        // Should be able to read the entries from 0-9
        LedgerHandle lhs = bkc.openLedgerNoRecovery(lh.getId(),
                BookKeeper.DigestType.CRC32, "testpasswd".getBytes());
        Enumeration<LedgerEntry> entries = lhs.readEntries(0, 9);
        assertTrue("Should have the elements", entries.hasMoreElements());
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("testBuiltAutoRecovery", new String(entry.getEntry()));
        }
    }
}
