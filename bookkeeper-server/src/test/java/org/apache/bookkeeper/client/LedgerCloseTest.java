/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the ledger close logic.
 */
public class LedgerCloseTest extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(LedgerCloseTest.class);

    static final int READ_TIMEOUT = 1;

    final DigestType digestType;

    public LedgerCloseTest() {
        super(6);
        this.digestType = DigestType.CRC32;
        // set timeout to a large value which disable it.
        baseClientConf.setReadTimeout(99999);
        baseConf.setGcWaitTime(999999);
    }

    @Test(timeout = 60000)
    public void testLedgerCloseDuringUnrecoverableErrors() throws Exception {
        int numEntries = 3;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        verifyMetadataConsistency(numEntries, lh);
    }

    @Test(timeout = 60000)
    public void testLedgerCheckerShouldnotSelectInvalidLastFragments() throws Exception {
        int numEntries = 10;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        // Add some entries before bookie failures
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry("data".getBytes());
        }
        numEntries = 4; // add n*ensemleSize+1 entries async after bookies
                        // failed.
        verifyMetadataConsistency(numEntries, lh);

        LedgerChecker checker = new LedgerChecker(bkc);
        CheckerCallback cb = new CheckerCallback();
        checker.checkLedger(lh, cb);
        Set<LedgerFragment> result = cb.waitAndGetResult();
        assertEquals("No fragments should be selected", 0, result.size());
    }

    class CheckerCallback implements GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }

        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }
    }

    private void verifyMetadataConsistency(int numEntries, LedgerHandle lh)
            throws Exception {
        final CountDownLatch addDoneLatch = new CountDownLatch(1);
        final CountDownLatch deadIOLatch = new CountDownLatch(1);
        final CountDownLatch recoverDoneLatch = new CountDownLatch(1);
        final CountDownLatch failedLatch = new CountDownLatch(1);
        // kill first bookie to replace with a unauthorize bookie
        InetSocketAddress bookie = lh.getLedgerMetadata().currentEnsemble.get(0);
        ServerConfiguration conf = killBookie(bookie);
        // replace a unauthorize bookie
        startUnauthorizedBookie(conf, addDoneLatch);
        // kill second bookie to replace with a dead bookie
        bookie = lh.getLedgerMetadata().currentEnsemble.get(1);
        conf = killBookie(bookie);
        // replace a slow dead bookie
        startDeadBookie(conf, deadIOLatch);

        // tried to add entries
        for (int i = 0; i < numEntries; i++) {
            lh.asyncAddEntry("data".getBytes(), new AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (BKException.Code.OK != rc) {
                        failedLatch.countDown();
                        deadIOLatch.countDown();
                    }
                    if (0 == entryId) {
                        try {
                            recoverDoneLatch.await();
                        } catch (InterruptedException ie) {
                        }
                    }
                }
            }, null);
        }
        // add finished
        addDoneLatch.countDown();
        // wait until entries failed due to UnauthorizedAccessException
        failedLatch.await();
        // simulate the ownership of this ledger is transfer to another host (which is actually
        // what we did in Hedwig).
        LOG.info("Recover ledger {}.", lh.getId());
        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        BookKeeper newBkc = new BookKeeperTestClient(newConf.setReadTimeout(1));
        LedgerHandle recoveredLh = newBkc.openLedger(lh.getId(), digestType, "".getBytes());
        LOG.info("Recover ledger {} done.", lh.getId());
        recoverDoneLatch.countDown();
        // wait a bit until add operations failed from second bookie due to IOException
        TimeUnit.SECONDS.sleep(5);
        // open the ledger again to make sure we ge the right last confirmed.
        LedgerHandle newLh = newBkc.openLedger(lh.getId(), digestType, "".getBytes());
        assertEquals("Metadata should be consistent across different opened ledgers",
                recoveredLh.getLastAddConfirmed(), newLh.getLastAddConfirmed());
    }

    private void startUnauthorizedBookie(ServerConfiguration conf, final CountDownLatch latch)
            throws Exception {
        Bookie sBookie = new Bookie(conf) {
            @Override
            public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                }
                throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
            }

            @Override
            public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                throw new IOException("Dead bookie for recovery adds.");
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, sBookie));
    }

    // simulate slow adds, then become normal when recover,
    // so no ensemble change when recovering ledger on this bookie.
    private void startDeadBookie(ServerConfiguration conf, final CountDownLatch latch) throws Exception {
        Bookie dBookie = new Bookie(conf) {
            @Override
            public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                }
                // simulate slow adds.
                throw new IOException("Dead bookie");
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, dBookie));
    }
}
