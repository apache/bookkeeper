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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestCallbacks.AddCallbackFuture;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the ledger close logic.
 */
@SuppressWarnings("deprecation")
public class LedgerCloseTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerCloseTest.class);

    static final int READ_TIMEOUT = 1;

    final DigestType digestType;

    public LedgerCloseTest() {
        super(6);
        this.digestType = DigestType.CRC32;
        // set timeout to a large value which disable it.
        baseClientConf.setReadTimeout(99999);
        baseConf.setGcWaitTime(999999);
    }

    @Test
    public void testLedgerCloseWithConsistentLength() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setReadTimeout(1);

        BookKeeper bkc = new BookKeeper(conf);
        LedgerHandle lh = bkc.createLedger(6, 3, DigestType.CRC32, new byte[] {});
        final CountDownLatch latch = new CountDownLatch(1);
        stopBKCluster();
        final AtomicInteger i = new AtomicInteger(0xdeadbeef);
        AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                i.set(rc);
                latch.countDown();
            }
        };
        lh.asyncAddEntry("Test Entry".getBytes(), cb, null);
        latch.await();
        assertEquals(i.get(), BKException.Code.NotEnoughBookiesException);
        assertEquals(0, lh.getLength());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, lh.getLastAddConfirmed());
        startBKCluster(zkUtil.getMetadataServiceUri());
        LedgerHandle newLh = bkc.openLedger(lh.getId(), DigestType.CRC32, new byte[] {});
        assertEquals(0, newLh.getLength());
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, newLh.getLastAddConfirmed());
    }

    @Test
    public void testLedgerCloseDuringUnrecoverableErrors() throws Exception {
        int numEntries = 3;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        verifyMetadataConsistency(numEntries, lh);
    }

    @Test
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
        BookieId bookie = lh.getCurrentEnsemble().get(0);
        ServerConfiguration conf = killBookie(bookie);
        // replace a unauthorize bookie
        startUnauthorizedBookie(conf, addDoneLatch);
        // kill second bookie to replace with a dead bookie
        bookie = lh.getCurrentEnsemble().get(1);
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
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, null);
        }
        // add finished
        addDoneLatch.countDown();
        // wait until entries failed due to UnauthorizedAccessException
        failedLatch.await();
        // simulate the ownership of this ledger is transfer to another host
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
        Bookie sBookie = new TestBookieImpl(conf) {
            @Override
            public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
            }

            @Override
            public void recoveryAddEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                throw new IOException("Dead bookie for recovery adds.");
            }
        };
        startAndAddBookie(conf, sBookie);
    }

    // simulate slow adds, then become normal when recover,
    // so no ensemble change when recovering ledger on this bookie.
    private void startDeadBookie(ServerConfiguration conf, final CountDownLatch latch) throws Exception {
        Bookie dBookie = new TestBookieImpl(conf) {
            @Override
            public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // simulate slow adds.
                throw new IOException("Dead bookie");
            }
        };
        startAndAddBookie(conf, dBookie);
    }

    @Test
    public void testAllWritesAreCompletedOnClosedLedger() throws Exception {
        for (int i = 0; i < 100; i++) {
            LOG.info("Iteration {}", i);

            List<AddCallbackFuture> futures = new ArrayList<AddCallbackFuture>();
            LedgerHandle w = bkc.createLedger(DigestType.CRC32, new byte[0]);
            AddCallbackFuture f = new AddCallbackFuture(0L);
            w.asyncAddEntry("foobar".getBytes(UTF_8), f, null);
            f.get();

            LedgerHandle r = bkc.openLedger(w.getId(), DigestType.CRC32, new byte[0]);
            for (int j = 0; j < 100; j++) {
                AddCallbackFuture f1 = new AddCallbackFuture(1L + j);
                w.asyncAddEntry("foobar".getBytes(), f1, null);
                futures.add(f1);
            }

            for (AddCallbackFuture f2: futures) {
                try {
                    f2.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException ee) {
                    // we don't care about errors
                } catch (TimeoutException te) {
                    LOG.error("Error on waiting completing entry {} : ", f2.getExpectedEntryId(), te);
                    fail("Should succeed on waiting completing entry " + f2.getExpectedEntryId());
                }
            }
        }
    }
}
