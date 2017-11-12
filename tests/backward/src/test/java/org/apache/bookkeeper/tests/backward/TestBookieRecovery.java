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
package org.apache.bookkeeper.tests.backward;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.MSLedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test backward compat issue on bookie recovery.
 */
public class TestBookieRecovery extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestBookieRecovery.class);

    // Object used for synchronizing async method calls
    class SyncObject {
        boolean value;

        public SyncObject() {
            value = false;
        }
    }

    // Object used for implementing the Bookie RecoverCallback for this jUnit
    // test. This verifies that the operation completed successfully.
    class BookieRecoverCallback implements RecoverCallback {
        boolean success = false;
        @Override
        public void recoverComplete(int rc, Object ctx) {
            LOG.info("Recovered bookie operation completed with rc: " + rc);
            success = rc == BKException.Code.OK;
            SyncObject sync = (SyncObject) ctx;
            synchronized (sync) {
                sync.value = true;
                sync.notify();
            }
        }
    }

    // Objects to use for this jUnit test.
    DigestType digestType;
    String ledgerManagerFactory;
    SyncObject sync;
    BookieRecoverCallback bookieRecoverCb;
    BookKeeperAdmin bkAdmin;

    // Constructor
    public TestBookieRecovery() {
        super(3);

        this.digestType = DigestType.CRC32;
        this.ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        LOG.info("Using ledger manager " + ledgerManagerFactory);
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseConf.setOpenFileLimit(200); // Limit the number of open files to avoid reaching the proc max
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseClientConf.setBookieRecoveryDigestType(digestType);
        baseClientConf.setBookieRecoveryPasswd("".getBytes());
        super.setUp();

        sync = new SyncObject();
        bookieRecoverCb = new BookieRecoverCallback();
        ClientConfiguration adminConf = new ClientConfiguration(baseClientConf);
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
        bkAdmin = new BookKeeperAdmin(adminConf);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Release any resources used by the BookKeeperTools instance.
        if (bkAdmin != null) {
            bkAdmin.close();
        }
        super.tearDown();
    }

    // Object used for synchronizing async method calls
    class SyncLedgerMetaObject {
        boolean value;
        int rc;
        LedgerMetadata meta;

        public SyncLedgerMetaObject() {
            value = false;
            meta = null;
        }
    }

    private LedgerMetadata getLedgerMetadata(LedgerHandle lh) throws Exception {
        final SyncLedgerMetaObject syncObj = new SyncLedgerMetaObject();
        bkc.getLedgerManager().readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {

            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                synchronized (syncObj) {
                    syncObj.rc = rc;
                    syncObj.meta = result;
                    syncObj.value = true;
                    syncObj.notify();
                }
            }

        });

        synchronized (syncObj) {
            while (!syncObj.value) {
                syncObj.wait();
            }
        }
        assertEquals(BKException.Code.OK, syncObj.rc);
        return syncObj.meta;
    }

    private static class ReplicationVerificationCallback implements ReadEntryCallback {
        final CountDownLatch latch;
        final AtomicLong numSuccess;

        ReplicationVerificationCallback(int numRequests) {
            latch = new CountDownLatch(numRequests);
            numSuccess = new AtomicLong(0);
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got " + rc + " for ledger " + ledgerId + " entry " + entryId + " from " + ctx);
            }
            if (rc == BKException.Code.OK) {
                numSuccess.incrementAndGet();
            }
            latch.countDown();
        }

        long await() throws InterruptedException {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                LOG.warn("Didn't get all responses in verification");
                return 0;
            } else {
                return numSuccess.get();
            }
        }
    }

    private boolean verifyFullyReplicated(LedgerHandle lh, long untilEntry) throws Exception {
        LedgerMetadata md = getLedgerMetadata(lh);

        Map<Long, ArrayList<BookieSocketAddress>> ensembles = md.getEnsembles();

        HashMap<Long, Long> ranges = new HashMap<Long, Long>();
        ArrayList<Long> keyList = Collections.list(
                Collections.enumeration(ensembles.keySet()));
        Collections.sort(keyList);
        for (int i = 0; i < keyList.size() - 1; i++) {
            ranges.put(keyList.get(i), keyList.get(i + 1));
        }
        ranges.put(keyList.get(keyList.size() - 1), untilEntry);

        for (Map.Entry<Long, ArrayList<BookieSocketAddress>> e : ensembles.entrySet()) {
            int quorum = md.getAckQuorumSize();
            long startEntryId = e.getKey();
            long endEntryId = ranges.get(startEntryId);
            long expectedSuccess = quorum * (endEntryId - startEntryId);
            int numRequests = e.getValue().size() * ((int) (endEntryId - startEntryId));

            ReplicationVerificationCallback cb = new ReplicationVerificationCallback(numRequests);
            for (long i = startEntryId; i < endEntryId; i++) {
                for (BookieSocketAddress addr : e.getValue()) {
                    bkc.getBookieClient().readEntry(addr, lh.getId(), i, cb, addr);
                }
            }

            long numSuccess = cb.await();
            if (numSuccess < expectedSuccess) {
                LOG.warn("Fragment not fully replicated ledgerId = " + lh.getId()
                         + " startEntryId = " + startEntryId
                         + " endEntryId = " + endEntryId
                         + " expectedSuccess = " + expectedSuccess
                         + " gotSuccess = " + numSuccess);
                return false;
            }
        }
        return true;
    }

    /**
     * Test that when we try to recover a ledger which doesn't have
     * the password stored in the configuration, we don't succeed.
     */
    @Test
    public void ensurePasswordUsedForOldLedgers() throws Exception {
        // This test bases on creating old ledgers in version 4.1.0, which only
        // supports ZooKeeper based flat and hierarchical LedgerManagerFactory.
        // So we ignore it for MSLedgerManagerFactory and LongHierarchicalLedgerManagerFactory.
        if (MSLedgerManagerFactory.class.getName().equals(ledgerManagerFactory)) {
            return;
        }
        if (LongHierarchicalLedgerManagerFactory.class.getName().equals(ledgerManagerFactory)) {
            return;
        }

        // stop all bookies
        // and wipe the ledger layout so we can use an old client
        zkUtil.getZooKeeperClient().delete("/ledgers/LAYOUT", -1);

        byte[] passwdCorrect = "AAAAAA".getBytes();
        byte[] passwdBad = "BBBBBB".getBytes();
        DigestType digestCorrect = digestType;
        DigestType digestBad = digestCorrect == DigestType.MAC ? DigestType.CRC32 : DigestType.MAC;

        org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType digestCorrect410 =
                org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType.valueOf(digestType.toString());

        org.apache.bk_v4_1_0.bookkeeper.conf.ClientConfiguration c =
                new org.apache.bk_v4_1_0.bookkeeper.conf.ClientConfiguration();
        c.setZkServers(zkUtil.getZooKeeperConnectString())
            .setLedgerManagerType(
                    ledgerManagerFactory.equals("org.apache.bookkeeper.meta.FlatLedgerManagerFactory")
                            ? "flat" : "hierarchical");

        // create client to set up layout, close it, restart bookies, and open a new client.
        // the new client is necessary to ensure that it has all the restarted bookies in the
        // its available bookie list
        org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper bkc41 =
                new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(c);
        bkc41.close();
        restartBookies();
        bkc41 = new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(c);

        org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle lh41 =
                bkc41.createLedger(3, 2, digestCorrect410, passwdCorrect);
        long ledgerId = lh41.getId();
        for (int i = 0; i < 100; i++) {
            lh41.addEntry("foobar".getBytes());
        }
        lh41.close();
        bkc41.close();

        // Startup a new bookie server
        startNewBookie();
        int removeIndex = 0;
        BookieSocketAddress bookieSrc = bs.get(removeIndex).getLocalAddress();
        bs.get(removeIndex).shutdown();
        bs.remove(removeIndex);

        // Check that entries are missing
        LedgerHandle lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        // Try to recover with bad password in conf
        // if the digest type is MAC
        // for CRC32, the password is only checked
        // when adding new entries, which recovery will
        // never do
        ClientConfiguration adminConf;
        BookKeeperAdmin bka;
        if (digestCorrect == DigestType.MAC) {
            adminConf = new ClientConfiguration();
            adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
            adminConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
            adminConf.setBookieRecoveryDigestType(digestCorrect);
            adminConf.setBookieRecoveryPasswd(passwdBad);

            bka = new BookKeeperAdmin(adminConf);
            try {
                bka.recoverBookieData(bookieSrc);
                fail("Shouldn't be able to recover with wrong password");
            } catch (BKException bke) {
                // correct behaviour
            } finally {
                bka.close();
            }
        }

        // Try to recover with bad digest in conf
        adminConf = new ClientConfiguration();
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
        adminConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        adminConf.setBookieRecoveryDigestType(digestBad);
        adminConf.setBookieRecoveryPasswd(passwdCorrect);

        bka = new BookKeeperAdmin(adminConf);
        try {
            bka.recoverBookieData(bookieSrc);
            fail("Shouldn't be able to recover with wrong digest");
        } catch (BKException bke) {
            // correct behaviour
        } finally {
            bka.close();
        }

        // Check that entries are still missing
        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        adminConf.setBookieRecoveryDigestType(digestCorrect);
        adminConf.setBookieRecoveryPasswd(passwdCorrect);

        bka = new BookKeeperAdmin(adminConf);
        bka.recoverBookieData(bookieSrc);
        bka.close();

        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertTrue("Should have recovered everything", verifyFullyReplicated(lh, 100));
        lh.close();
    }

}
