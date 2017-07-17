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
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BKException.BKIllegalOpException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookKeeperAdminTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int numOfBookies = 6;
    private final int lostBookieRecoveryDelayInitValue = 1800;

    public BookKeeperAdminTest() {
        super(numOfBookies);
        baseConf.setAutoRecoveryDaemonEnabled(true);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
    }

    @Test(timeout = 60000)
    public void testLostBookieRecoveryDelayValue() throws Exception {
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayInitValue, bkAdmin.getLostBookieRecoveryDelay());
        int newLostBookieRecoveryDelayValue = 2400;
        bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
        assertEquals("LostBookieRecoveryDelay", newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        assertEquals("LostBookieRecoveryDelay", newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        newLostBookieRecoveryDelayValue = 3000;
        bkAdmin.setLostBookieRecoveryDelay(newLostBookieRecoveryDelayValue);
        assertEquals("LostBookieRecoveryDelay", newLostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        bkAdmin.close();
    }

    @Test(timeout = 60000)
    public void testTriggerAudit() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        int lostBookieRecoveryDelayValue = bkAdmin.getLostBookieRecoveryDelay();
        urLedgerMgr.disableLedgerReplication();
        try {
            bkAdmin.triggerAudit();
            Assert.fail("Trigger Audit should have failed because LedgerReplication is disabled");
        } catch (UnavailableException une) {
            // expected
        }
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        urLedgerMgr.enableLedgerReplication();
        bkAdmin.triggerAudit();
        assertEquals("LostBookieRecoveryDelay", lostBookieRecoveryDelayValue, bkAdmin.getLostBookieRecoveryDelay());
        long ledgerId = 1L;
        LedgerHandle ledgerHandle = bkc.createLedgerAdv(ledgerId, numBookies, numBookies, numBookies, digestType,
                PASSWORD.getBytes(), null);
        ledgerHandle.addEntry(0, "data".getBytes());
        ledgerHandle.close();

        killBookie(1);
        /*
         * since lostBookieRecoveryDelay is set, when a bookie is died, it will
         * not start Audit process immediately. But when triggerAudit is called
         * it will force audit process.
         */
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Long> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        assertTrue("There are supposed to be underreplicatedledgers", ledgersToRereplicate.hasNext());
        assertEquals("Underreplicated ledgerId", ledgerId, ledgersToRereplicate.next().longValue());
        bkAdmin.close();
    }

    @Test(timeout = 480000)
    public void testDecommissionBookie() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        int numOfLedgers = 2 * numOfBookies;
        int numOfEntries = 2 * numOfBookies;
        for (int i = 0; i < numOfLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 2, digestType, PASSWORD.getBytes());
            for (int j = 0; j < numOfEntries; j++) {
                lh.addEntry("entry".getBytes());
            }
            lh.close();
        }
        /*
         * create ledgers having empty segments (segment with no entries)
         */
        for (int i = 0; i < numOfLedgers; i++) {
            LedgerHandle emptylh = bkc.createLedger(3, 2, digestType, PASSWORD.getBytes());
            emptylh.close();
        }
        
        try {
            /*
             * if we try to call decommissionBookie for a bookie which is not
             * shutdown, then it should throw BKIllegalOpException
             */
            bkAdmin.decommissionBookie(bs.get(0).getLocalAddress());
            fail("Expected BKIllegalOpException because that bookie is not shutdown yet");
        } catch (BKIllegalOpException bkioexc) {
            // expected IllegalException
        }
        
        ServerConfiguration killedBookieConf = killBookie(1);
        /*
         * this decommisionBookie should make sure that there are no
         * underreplicated ledgers because of this bookie
         */
        bkAdmin.decommissionBookie(Bookie.getBookieAddress(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Long> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next();
                LOG.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }
        
        killedBookieConf = killBookie(0);
        bkAdmin.decommissionBookie(Bookie.getBookieAddress(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next();
                LOG.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }
        bkAdmin.close();
    }

    @Test(timeout = 240000)
    public void testDecommissionForLedgersWithMultipleSegmentsAndNotWriteClosed() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        int numOfEntries = 2 * numOfBookies;

        LedgerHandle lh1 = bkc.createLedgerAdv(1L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        LedgerHandle lh2 = bkc.createLedgerAdv(2L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        LedgerHandle lh3 = bkc.createLedgerAdv(3L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        LedgerHandle lh4 = bkc.createLedgerAdv(4L, numBookies, 3, 3, digestType, PASSWORD.getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            lh1.addEntry(j, "data".getBytes());
            lh2.addEntry(j, "data".getBytes());
            lh3.addEntry(j, "data".getBytes());
            lh4.addEntry(j, "data".getBytes());
        }

        startNewBookie();

        assertEquals("Number of Available Bookies", numOfBookies + 1, bkAdmin.getAvailableBookies().size());

        ServerConfiguration killedBookieConf = killBookie(0);

        /*
         * since one of the bookie is killed, ensemble change happens when next
         * write is made.So new segment will be created for those 2 ledgers.
         */
        for (int j = numOfEntries; j < 2 * numOfEntries; j++) {
            lh1.addEntry(j, "data".getBytes());
            lh2.addEntry(j, "data".getBytes());
        }
        
        /*
         * Here lh1 and lh2 have multiple segments and are writeclosed. But lh3 and lh4 are 
         * not writeclosed and contains only one segment.
         */
        lh1.close();
        lh2.close();
        
        /*
         * If the last segment of the ledger is underreplicated and if the
         * ledger is not closed then it will remain underreplicated for
         * openLedgerRereplicationGracePeriod (by default 30 secs). For more
         * info. Check BOOKKEEPER-237 and BOOKKEEPER-325. But later
         * ReplicationWorker will fence the ledger.
         */
        bkAdmin.decommissionBookie(Bookie.getBookieAddress(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<Long> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next();
                LOG.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }
        bkAdmin.close();
    }
}
