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

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieAccessor;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdapter;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies that the period check on the auditor
 * will pick up on missing data in the client.
 */
public class AuditorPeriodicCheckTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorPeriodicCheckTest.class);

    private MetadataBookieDriver driver;
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();

    private static final int CHECK_INTERVAL = 1; // run every second

    public AuditorPeriodicCheckTest() {
        super(3);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = new ServerConfiguration(bsConfs.get(i));
            conf.setAuditorPeriodicCheckInterval(CHECK_INTERVAL);

            String addr = bs.get(i).getLocalAddress().toString();

            AuditorElector auditorElector = new AuditorElector(addr, conf);
            auditorElectors.put(addr, auditorElector);
            auditorElector.start();
            LOG.debug("Starting Auditor Elector");
        }

        driver = MetadataDrivers.getBookieDriver(
            URI.create(bsConfs.get(0).getMetadataServiceUri()));
        driver.initialize(
            bsConfs.get(0),
            () -> {},
            NullStatsLogger.INSTANCE);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != driver) {
            driver.close();
        }

        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }
        super.tearDown();
    }

    /**
     * test that the periodic checking will detect corruptions in
     * the bookie entry log.
     */
    @Test
    public void testEntryLogCorruption() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        underReplicationManager.disableLedgerReplication();

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        long ledgerId = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        BookieAccessor.forceFlush(bs.get(0).getBookie());


        File ledgerDir = bsConfs.get(0).getLedgerDirs()[0];
        ledgerDir = Bookie.getCurrentDirectory(ledgerDir);
        // corrupt of entryLogs
        File[] entryLogs = ledgerDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".log");
                }
            });
        ByteBuffer junk = ByteBuffer.allocate(1024 * 1024);
        for (File f : entryLogs) {
            FileOutputStream out = new FileOutputStream(f);
            out.getChannel().write(junk);
            out.close();
        }
        restartBookies(); // restart to clear read buffers

        underReplicationManager.enableLedgerReplication();
        long underReplicatedLedger = -1;
        for (int i = 0; i < 10; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", ledgerId, underReplicatedLedger);
        underReplicationManager.close();
    }

    /**
     * test that the period checker will detect corruptions in
     * the bookie index files.
     */
    @Test
    public void testIndexCorruption() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        long ledgerToCorrupt = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        // push ledgerToCorrupt out of page cache (bookie is configured to only use 1 page)
        lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        BookieAccessor.forceFlush(bs.get(0).getBookie());

        File ledgerDir = bsConfs.get(0).getLedgerDirs()[0];
        ledgerDir = Bookie.getCurrentDirectory(ledgerDir);

        // corrupt of entryLogs
        File index = new File(ledgerDir, IndexPersistenceMgr.getLedgerName(ledgerToCorrupt));
        LOG.info("file to corrupt{}" , index);
        ByteBuffer junk = ByteBuffer.allocate(1024 * 1024);
        FileOutputStream out = new FileOutputStream(index);
        out.getChannel().write(junk);
        out.close();

        long underReplicatedLedger = -1;
        for (int i = 0; i < 15; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", ledgerToCorrupt, underReplicatedLedger);
        underReplicationManager.close();
    }

    /**
     * Test that the period checker will not run when auto replication has been disabled.
     */
    @Test
    public void testPeriodicCheckWhenDisabled() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        final LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        final int numLedgers = 10;
        final int numMsgs = 2;
        final CountDownLatch completeLatch = new CountDownLatch(numMsgs * numLedgers);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            lhs.add(lh);
            for (int j = 0; j < 2; j++) {
                lh.asyncAddEntry("testdata".getBytes(), new AddCallback() {
                        public void addComplete(int rc2, LedgerHandle lh, long entryId, Object ctx) {
                            if (rc.compareAndSet(BKException.Code.OK, rc2)) {
                                LOG.info("Failed to add entry : {}", BKException.getMessage(rc2));
                            }
                            completeLatch.countDown();
                        }
                    }, null);
            }
        }
        completeLatch.await();
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        underReplicationManager.disableLedgerReplication();

        final AtomicInteger numReads = new AtomicInteger(0);
        ServerConfiguration conf = killBookie(0);

        Bookie deadBookie = new Bookie(conf) {
            @Override
            public ByteBuf readEntry(long ledgerId, long entryId)
                    throws IOException, NoLedgerException {
                // we want to disable during checking
                numReads.incrementAndGet();
                throw new IOException("Fake I/O exception");
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, deadBookie));

        Thread.sleep(CHECK_INTERVAL * 2000);
        assertEquals("Nothing should have tried to read", 0, numReads.get());
        underReplicationManager.enableLedgerReplication();
        Thread.sleep(CHECK_INTERVAL * 2000); // give it time to run

        underReplicationManager.disableLedgerReplication();
        // give it time to stop, from this point nothing new should be marked
        Thread.sleep(CHECK_INTERVAL * 2000);

        int numUnderreplicated = 0;
        long underReplicatedLedger = -1;
        do {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger == -1) {
                break;
            }
            numUnderreplicated++;

            underReplicationManager.markLedgerReplicated(underReplicatedLedger);
        } while (underReplicatedLedger != -1);

        Thread.sleep(CHECK_INTERVAL * 2000); // give a chance to run again (it shouldn't, it's disabled)

        // ensure that nothing is marked as underreplicated
        underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
        assertEquals("There should be no underreplicated ledgers", -1, underReplicatedLedger);

        LOG.info("{} of {} ledgers underreplicated", numUnderreplicated, numUnderreplicated);
        assertTrue("All should be underreplicated",
                numUnderreplicated <= numLedgers && numUnderreplicated > 0);
    }

    /**
     * Test that the period check will succeed if a ledger is deleted midway.
     */
    @Test
    public void testPeriodicCheckWhenLedgerDeleted() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        try (final Auditor auditor = new Auditor(
                Bookie.getBookieAddress(bsConfs.get(0)).toString(),
                bsConfs.get(0), NullStatsLogger.INSTANCE)) {
            final AtomicBoolean exceptionCaught = new AtomicBoolean(false);
            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new Thread() {
                public void run() {
                    try {
                        latch.countDown();
                        for (int i = 0; i < numLedgers; i++) {
                            auditor.checkAllLedgers();
                        }
                    } catch (Exception e) {
                        LOG.error("Caught exception while checking all ledgers", e);
                        exceptionCaught.set(true);
                    }
                }
            };
            t.start();
            latch.await();
            for (Long id : ids) {
                bkc.deleteLedger(id);
            }
            t.join();
            assertFalse("Shouldn't have thrown exception", exceptionCaught.get());
        }
    }

    @Test
    public void testInitialDelayOfCheckAllLedgers() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(bsConfs.get(0));
        validateInitialDelayOfCheckAllLedgers(urm, -1, 1000, servConf, bkc);
        validateInitialDelayOfCheckAllLedgers(urm, 999, 1000, servConf, bkc);
        validateInitialDelayOfCheckAllLedgers(urm, 1001, 1000, servConf, bkc);
    }

    void validateInitialDelayOfCheckAllLedgers(LedgerUnderreplicationManager urm, long timeSinceLastExecutedInSecs,
            long auditorPeriodicCheckInterval, ServerConfiguration servConf, BookKeeper bkc)
            throws UnavailableException, UnknownHostException, InterruptedException {
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger checkAllLedgersStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.CHECK_ALL_LEDGERS_TIME);
        servConf.setAuditorPeriodicCheckInterval(auditorPeriodicCheckInterval);
        final TestAuditor auditor = new TestAuditor(Bookie.getBookieAddress(servConf).toString(), servConf, bkc, false,
                statsLogger);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("CHECK_ALL_LEDGERS_TIME SuccessCount", 0, checkAllLedgersStatsLogger.getSuccessCount());
        long curTimeBeforeStart = System.currentTimeMillis();
        long checkAllLedgersCTime = -1;
        long initialDelayInMsecs = -1;
        long nextExpectedCheckAllLedgersExecutionTime = -1;
        long bufferTimeInMsecs = 12000L;
        if (timeSinceLastExecutedInSecs == -1) {
            /*
             * if we are setting checkAllLedgersCTime to -1, it means that
             * checkAllLedgers hasn't run before. So initialDelay for
             * checkAllLedgers should be 0.
             */
            checkAllLedgersCTime = -1;
            initialDelayInMsecs = 0;
        } else {
            checkAllLedgersCTime = curTimeBeforeStart - timeSinceLastExecutedInSecs * 1000L;
            initialDelayInMsecs = timeSinceLastExecutedInSecs > auditorPeriodicCheckInterval ? 0
                    : (auditorPeriodicCheckInterval - timeSinceLastExecutedInSecs) * 1000L;
        }
        /*
         * next checkAllLedgers should happen atleast after
         * nextExpectedCheckAllLedgersExecutionTime.
         */
        nextExpectedCheckAllLedgersExecutionTime = curTimeBeforeStart + initialDelayInMsecs;

        urm.setCheckAllLedgersCTime(checkAllLedgersCTime);
        auditor.start();
        /*
         * since auditorPeriodicCheckInterval are higher values (in the order of
         * 100s of seconds), its ok bufferTimeInMsecs to be ` 10 secs.
         */
        assertTrue("checkAllLedgers should have executed with initialDelay " + initialDelayInMsecs,
                latch.await(initialDelayInMsecs + bufferTimeInMsecs, TimeUnit.MILLISECONDS));
        for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            if (checkAllLedgersStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("CHECK_ALL_LEDGERS_TIME SuccessCount", 1, checkAllLedgersStatsLogger.getSuccessCount());
        long currentCheckAllLedgersCTime = urm.getCheckAllLedgersCTime();
        assertTrue(
                "currentCheckAllLedgersCTime: " + currentCheckAllLedgersCTime
                        + " should be greater than nextExpectedCheckAllLedgersExecutionTime: "
                        + nextExpectedCheckAllLedgersExecutionTime,
                currentCheckAllLedgersCTime > nextExpectedCheckAllLedgersExecutionTime);
        assertTrue(
                "currentCheckAllLedgersCTime: " + currentCheckAllLedgersCTime
                        + " should be lesser than nextExpectedCheckAllLedgersExecutionTime+bufferTimeInMsecs: "
                        + (nextExpectedCheckAllLedgersExecutionTime + bufferTimeInMsecs),
                currentCheckAllLedgersCTime < (nextExpectedCheckAllLedgersExecutionTime + bufferTimeInMsecs));
        auditor.close();
    }

    class TestAuditor extends Auditor {

        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<CountDownLatch>(new CountDownLatch(1));

        public TestAuditor(String bookieIdentifier, ServerConfiguration conf, BookKeeper bkc, boolean ownBkc,
                StatsLogger statsLogger) throws UnavailableException {
            super(bookieIdentifier, conf, bkc, ownBkc, statsLogger);
        }

        void checkAllLedgers() throws BKException, IOException, InterruptedException, KeeperException {
            super.checkAllLedgers();
            latchRef.get().countDown();
        }

        CountDownLatch getLatch() {
            return latchRef.get();
        }

        void setLatch(CountDownLatch latch) {
            latchRef.set(latch);
        }
    }

    private BookieSocketAddress replaceBookieWithWriteFailingBookie(LedgerHandle lh) throws Exception {
        int bookieIdx = -1;
        Long entryId = LedgerHandleAdapter.getLedgerMetadata(lh).getEnsembles().firstKey();
        List<BookieSocketAddress> curEnsemble = LedgerHandleAdapter
                .getLedgerMetadata(lh).getEnsembles().get(entryId);

        // Identify a bookie in the current ledger ensemble to be replaced
        BookieSocketAddress replacedBookie = null;
        for (int i = 0; i < numBookies; i++) {
            if (curEnsemble.contains(bs.get(i).getLocalAddress())) {
                bookieIdx = i;
                replacedBookie = bs.get(i).getLocalAddress();
                break;
            }
        }
        assertNotEquals("Couldn't find ensemble bookie in bookie list", -1, bookieIdx);

        LOG.info("Killing bookie " + bs.get(bookieIdx).getLocalAddress());
        ServerConfiguration conf = killBookie(bookieIdx);
        Bookie writeFailingBookie = new Bookie(conf) {
            @Override
            public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb,
                             Object ctx, byte[] masterKey)
                             throws IOException, BookieException {
                try {
                    LOG.info("Failing write to entry ");
                    // sleep a bit so that writes to other bookies succeed before
                    // the client hears about the failure on this bookie. If the
                    // client gets ack-quorum number of acks first, it won't care
                    // about any failures and won't reform the ensemble.
                    Thread.sleep(100);
                    throw new IOException();
                } catch (InterruptedException ie) {
                    // ignore, only interrupted if shutting down,
                    // and an exception would spam the logs
                    Thread.currentThread().interrupt();
                }
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, writeFailingBookie));
        return replacedBookie;
    }

    /*
     * Validates that the periodic ledger check will fix entries with a failed write.
     */
    @Test
    public void testFailedWriteRecovery() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        underReplicationManager.disableLedgerReplication();

        LedgerHandle lh = bkc.createLedger(2, 2, 1, DigestType.CRC32, "passwd".getBytes());

        // kill one of the bookies and replace it with one that rejects write;
        // This way we get into the under replication state
        BookieSocketAddress replacedBookie = replaceBookieWithWriteFailingBookie(lh);

        // Write a few entries; this should cause under replication
        byte[] data = "foobar".getBytes();
        data = "foobar".getBytes();
        lh.addEntry(data);
        lh.addEntry(data);
        lh.addEntry(data);

        lh.close();

        // enable under replication detection and wait for it to report
        // under replicated ledger
        underReplicationManager.enableLedgerReplication();
        long underReplicatedLedger = -1;
        for (int i = 0; i < 5; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", lh.getId(), underReplicatedLedger);

        // now start the replication workers
        List<ReplicationWorker> l = new ArrayList<ReplicationWorker>();
        for (int i = 0; i < numBookies; i++) {
            ReplicationWorker rw = new ReplicationWorker(bsConfs.get(i), NullStatsLogger.INSTANCE);
            rw.start();
            l.add(rw);
        }
        underReplicationManager.close();

        // Wait for ensemble to change after replication
        Thread.sleep(3000);
        for (ReplicationWorker rw : l) {
            rw.shutdown();
        }

        // check that ensemble has changed and the bookie that rejected writes has
        // been replaced in the ensemble
        LedgerHandle newLh = bkc.openLedger(lh.getId(), DigestType.CRC32, "passwd".getBytes());
        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : LedgerHandleAdapter.getLedgerMetadata(newLh).
                getEnsembles().entrySet()) {
            List<BookieSocketAddress> ensemble = e.getValue();
            assertFalse("Ensemble hasn't been updated", ensemble.contains(replacedBookie));
        }
        newLh.close();
    }
}
