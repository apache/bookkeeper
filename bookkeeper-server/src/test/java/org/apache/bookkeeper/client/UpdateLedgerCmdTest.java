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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test an update command on a ledger.
 */
public class UpdateLedgerCmdTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateLedgerCmdTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";

    public UpdateLedgerCmdTest() {
        super(3);
        useUUIDasBookieId = false;
        baseConf.setGcWaitTime(100000);
        // disable advertised address since it takes precedence over setUseHostNameAsBookieID setting
        // which is used in this test
        baseConf.setAdvertisedAddress(null);
    }

    /**
     * updateledgers to hostname.
     */
    @Test
    public void testUpdateLedgersToHostname() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        List<LedgerHandle> ledgers = new ArrayList<LedgerHandle>();
        LedgerHandle lh1 = createLedgerWithEntries(bk, 0);
        ledgers.add(lh1);
        for (int i = 1; i < 40; i++) {
            ledgers.add(createLedgerWithEntries(bk, 0));
        }

        String[] argv = new String[] { "updateledgers", "-b", "hostname", "-v", "true", "-p", "2" };
        final ServerConfiguration conf = confByIndex(0);
        conf.setUseHostNameAsBookieID(true);
        BookieSocketAddress toBookieId = BookieImpl.getBookieAddress(conf);
        BookieId toBookieAddr = new BookieSocketAddress(toBookieId.getHostName() + ":"
                + conf.getBookiePort()).toBookieId();
        updateLedgerCmd(argv, 0, conf);

        int updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, toBookieAddr);
        assertEquals("Failed to update the ledger metadata to use bookie host name", 40, updatedLedgersCount);
    }

    /**
     * replace bookie address in ledger.
     */
    @Test
    public void testUpdateBookieInLedger() throws Exception {
        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        List<LedgerHandle> ledgers = new ArrayList<LedgerHandle>();
        LedgerHandle lh1 = createLedgerWithEntries(bk, 0);
        ledgers.add(lh1);
        for (int i = 1; i < 40; i++) {
            ledgers.add(createLedgerWithEntries(bk, 0));
        }
        BookieId srcBookie = getBookie(0);
        BookieId destBookie = new BookieSocketAddress("1.1.1.1", 2181).toBookieId();
        String[] argv = new String[] { "updateBookieInLedger", "-sb", srcBookie.toString(), "-db",
                destBookie.toString(), "-v", "true", "-p", "2" };
        final ServerConfiguration conf = confByIndex(0);
        killBookie(0);
        updateLedgerCmd(argv, 0, conf);
        int updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, srcBookie);
        assertEquals("Failed to update the ledger metadata with new bookie-address", 0, updatedLedgersCount);
        updatedLedgersCount = getUpdatedLedgersCount(bk, ledgers, destBookie);
        assertEquals("Failed to update the ledger metadata with new bookie-address", 40, updatedLedgersCount);
    }

    private void updateLedgerCmd(String[] argv, int exitCode, ServerConfiguration conf) throws KeeperException,
            InterruptedException, IOException, UnknownHostException, Exception {
        LOG.info("Perform updateledgers command");
        BookieShell bkShell = new BookieShell();
        bkShell.setConf(conf);

        assertEquals("Failed to return exit code!", exitCode, bkShell.run(argv));
    }

    private int getUpdatedLedgersCount(BookKeeper bk, List<LedgerHandle> ledgers, BookieId toBookieAddr)
            throws InterruptedException, BKException {
        List<BookieId> ensemble;
        int updatedLedgersCount = 0;
        for (LedgerHandle lh : ledgers) {
            lh.close();
            LedgerHandle openLedger = bk.openLedger(lh.getId(), digestType, PASSWORD.getBytes());
            ensemble = openLedger.getLedgerMetadata().getEnsembleAt(0);
            if (ensemble.contains(toBookieAddr)) {
                updatedLedgersCount++;
            }
        }
        return updatedLedgersCount;
    }

    private LedgerHandle createLedgerWithEntries(BookKeeper bk, int numOfEntries) throws Exception {
        LedgerHandle lh = bk.createLedger(3, 3, digestType, PASSWORD.getBytes());
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch latch = new CountDownLatch(numOfEntries);

        final AddCallback cb = new AddCallback() {
            public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                rc.compareAndSet(BKException.Code.OK, rccb);
                latch.countDown();
            }
        };
        for (int i = 0; i < numOfEntries; i++) {
            lh.asyncAddEntry(("foobar" + i).getBytes(), cb, null);
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        return lh;
    }
}
