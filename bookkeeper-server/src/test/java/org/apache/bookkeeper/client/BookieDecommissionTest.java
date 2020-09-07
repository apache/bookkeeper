/*
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
 */
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BKException.BKIllegalOpException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Unit test of bookie decommission operations.
 */
@Slf4j
public class BookieDecommissionTest extends BookKeeperClusterTestCase {

    private static final int NUM_OF_BOOKIES = 6;
    private static DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";

    public BookieDecommissionTest() {
        super(NUM_OF_BOOKIES, 480);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);
    }

    @FlakyTest("https://github.com/apache/bookkeeper/issues/502")
    public void testDecommissionBookie() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());

        int numOfLedgers = 2 * NUM_OF_BOOKIES;
        int numOfEntries = 2 * NUM_OF_BOOKIES;
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
            bkAdmin.decommissionBookie(bs.get(0).getBookieId());
            fail("Expected BKIllegalOpException because that bookie is not shutdown yet");
        } catch (BKIllegalOpException bkioexc) {
            // expected IllegalException
        }

        ServerConfiguration killedBookieConf = killBookie(1);
        /*
         * this decommisionBookie should make sure that there are no
         * underreplicated ledgers because of this bookie
         */
        bkAdmin.decommissionBookie(Bookie.getBookieId(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<UnderreplicatedLedger> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next().getLedgerId();
                log.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }

        killedBookieConf = killBookie(0);
        bkAdmin.decommissionBookie(Bookie.getBookieId(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                Long ledgerId = ledgersToRereplicate.next().getLedgerId();
                log.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }
        bkAdmin.close();
    }

    @Test
    public void testDecommissionForLedgersWithMultipleSegmentsAndNotWriteClosed() throws Exception {
        ZkLedgerUnderreplicationManager urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString());
        int numOfEntries = 2 * NUM_OF_BOOKIES;

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

        assertEquals("Number of Available Bookies", NUM_OF_BOOKIES + 1, bkAdmin.getAvailableBookies().size());

        ServerConfiguration killedBookieConf = killBookie(0);

        /*
         * since one of the bookie is killed, ensemble change happens when next
         * write is made.So new fragment will be created for those 2 ledgers.
         */
        for (int j = numOfEntries; j < 2 * numOfEntries; j++) {
            lh1.addEntry(j, "data".getBytes());
            lh2.addEntry(j, "data".getBytes());
        }

        /*
         * Here lh1 and lh2 have multiple fragments and are writeclosed. But lh3 and lh4 are
         * not writeclosed and contains only one fragment.
         */
        lh1.close();
        lh2.close();

        /*
         * If the last fragment of the ledger is underreplicated and if the
         * ledger is not closed then it will remain underreplicated for
         * openLedgerRereplicationGracePeriod (by default 30 secs). For more
         * info. Check BOOKKEEPER-237 and BOOKKEEPER-325. But later
         * ReplicationWorker will fence the ledger.
         */
        bkAdmin.decommissionBookie(Bookie.getBookieId(killedBookieConf));
        bkAdmin.triggerAudit();
        Thread.sleep(500);
        Iterator<UnderreplicatedLedger> ledgersToRereplicate = urLedgerMgr.listLedgersToRereplicate(null);
        if (ledgersToRereplicate.hasNext()) {
            while (ledgersToRereplicate.hasNext()) {
                long ledgerId = ledgersToRereplicate.next().getLedgerId();
                log.error("Ledger: {} is underreplicated which is not expected", ledgerId);
            }
            fail("There are not supposed to be any underreplicatedledgers");
        }
        bkAdmin.close();
    }

}
