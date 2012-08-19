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
 * 
 */
package org.apache.bookkeeper.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests BKAdmin that it should be able to replicate the failed bookie fragments
 * to target bookie.
 */
public class TestLedgerFragmentReplication extends BookKeeperClusterTestCase {

    private static Logger LOG = LoggerFactory
            .getLogger(TestLedgerFragmentReplication.class);

    public TestLedgerFragmentReplication() {
        super(3);
    }

    private static class CheckerCallback implements
            GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }
    }

    /**
     * Tests that replicate method should replicate the failed bookie fragments
     * to target bookie passed.
     */
    @Test
    public void testReplicateLFShouldCopyFailedBookieFragmentsToTargetBookie()
            throws Exception {
        byte[] data = "TestLedgerFragmentReplication".getBytes();
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                "testpasswd".getBytes());

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        InetSocketAddress replicaToKill = lh.getLedgerMetadata().getEnsembles()
                .get(0L).get(0);

        LOG.info("Killing Bookie", replicaToKill);
        killBookie(replicaToKill);

        int startNewBookie = startNewBookie();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        InetSocketAddress newBkAddr = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        LOG.info("New Bookie addr :" + newBkAddr);
        Set<LedgerFragment> result = getFragmentsToReplicate(lh);

        BookKeeperAdmin admin = new BookKeeperAdmin(baseClientConf);
        lh.close();
        // 0-9 entries should be copy to new bookie
        
        for (LedgerFragment lf : result) {
            assertTrue(admin.replicateLedgerFragment(lh, lf, newBkAddr));
        }

        // Killing all bookies except newly replicated bookie
        SortedMap<Long, ArrayList<InetSocketAddress>> allBookiesBeforeReplication = lh
                .getLedgerMetadata().getEnsembles();
        Set<Entry<Long, ArrayList<InetSocketAddress>>> entrySet = allBookiesBeforeReplication
                .entrySet();
        for (Entry<Long, ArrayList<InetSocketAddress>> entry : entrySet) {
            ArrayList<InetSocketAddress> bookies = entry.getValue();
            for (InetSocketAddress bookie : bookies) {
                if (newBkAddr.equals(bookie)) {
                    continue;
                }
                killBookie(bookie);
            }
        }

        // Should be able to read the entries from 0-9
        verifyRecoveredLedgers(lh, 0, 9);
    }

    /**
     * Tests that ReplicateLedgerFragment should return false if replication
     * fails
     */
    @Test
    public void testReplicateLFShouldReturnFalseIfTheReplicationFails()
            throws Exception {
        byte[] data = "TestLedgerFragmentReplication".getBytes();
        LedgerHandle lh = bkc.createLedger(2, 1, BookKeeper.DigestType.CRC32,
                "testpasswd".getBytes());

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        // Kill the first Bookie
        InetSocketAddress replicaToKill = lh.getLedgerMetadata().getEnsembles()
                .get(0L).get(0);
        killBookie(replicaToKill);
        LOG.info("Killed Bookie =" + replicaToKill);

        // Write some more entries
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        // Kill the second Bookie
        replicaToKill = lh.getLedgerMetadata().getEnsembles().get(0L).get(0);
        killBookie(replicaToKill);
        LOG.info("Killed Bookie =" + replicaToKill);

        Set<LedgerFragment> fragments = getFragmentsToReplicate(lh);
        BookKeeperAdmin admin = new BookKeeperAdmin(baseClientConf);
        int startNewBookie = startNewBookie();
        InetSocketAddress additionalBK = new InetSocketAddress(InetAddress
                .getLocalHost().getHostAddress(), startNewBookie);
        for (LedgerFragment lf : fragments) {
            assertFalse("Replication should fail", admin
                    .replicateLedgerFragment(lh, lf, additionalBK));
        }
    }

    private Set<LedgerFragment> getFragmentsToReplicate(LedgerHandle lh)
            throws InterruptedException {
        LedgerChecker checker = new LedgerChecker(bkc);
        CheckerCallback cb = new CheckerCallback();
        checker.checkLedger(lh, cb);
        Set<LedgerFragment> fragments = cb.waitAndGetResult();
        return fragments;
    }

    private void verifyRecoveredLedgers(LedgerHandle lh, long startEntryId,
            long endEntryId) throws BKException, InterruptedException {
        LedgerHandle lhs = bkc.openLedgerNoRecovery(lh.getId(),
                BookKeeper.DigestType.CRC32, "testpasswd".getBytes());
        Enumeration<LedgerEntry> entries = lhs.readEntries(startEntryId,
                endEntryId);
        assertTrue("Should have the elements", entries.hasMoreElements());
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("TestLedgerFragmentReplication", new String(entry
                    .getEntry()));
        }
    }

}
