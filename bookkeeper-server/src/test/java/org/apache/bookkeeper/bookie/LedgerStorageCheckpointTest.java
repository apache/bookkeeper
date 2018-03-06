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
package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LedgerStorageCheckpointTest.
 *
 */
public class LedgerStorageCheckpointTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(LedgerStorageCheckpointTest.class);

    @Rule
    public final TestName runtime = new TestName();

    public LedgerStorageCheckpointTest() {
        super(0);
    }

    private LogMark readLastMarkFile(File lastMarkFile) throws IOException {
        byte buff[] = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        LogMark rolledLogMark = new LogMark();
        FileInputStream fis = new FileInputStream(lastMarkFile);
        int bytesRead = fis.read(buff);
        fis.close();
        if (bytesRead != 16) {
            throw new IOException("Couldn't read enough bytes from lastMark." + " Wanted " + 16 + ", got " + bytesRead);
        }
        bb.clear();
        rolledLogMark.readLogMark(bb);
        return rolledLogMark;
    }

    /*
     * In this testcase, InterleavedLedgerStorage is used and validate if the
     * checkpoint is called for every flushinterval period.
     */
    @Test
    public void testPeriodicCheckpointForInterleavedLedgerStorage() throws Exception {
        testPeriodicCheckpointForLedgerStorage(InterleavedLedgerStorage.class.getName());
    }

    /*
     * In this testcase, SortedLedgerStorage is used and validate if the
     * checkpoint is called for every flushinterval period.
     */
    @Test
    public void testPeriodicCheckpointForSortedLedgerStorage() throws Exception {
        testPeriodicCheckpointForLedgerStorage(SortedLedgerStorage.class.getName());
    }

    public void testPeriodicCheckpointForLedgerStorage(String ledgerStorageClassName) throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                .setFlushInterval(2000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(true)
                .setLedgerStorageClass(ledgerStorageClassName);
        Assert.assertEquals("Number of JournalDirs", 1, conf.getJournalDirs().length);
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);

        int numOfLedgers = 2;
        int numOfEntries = 5;
        byte[] dataBytes = "data".getBytes();

        for (int i = 0; i < numOfLedgers; i++) {
            int ledgerIndex = i;
            LedgerHandle handle = bkClient.createLedgerAdv((long) i, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(),
                    null);
            for (int j = 0; j < numOfEntries; j++) {
                handle.addEntry(j, dataBytes);
            }
            handle.close();
        }

        LastLogMark lastLogMarkAfterFirstSetOfAdds = server.getBookie().journals.get(0).getLastLogMark();
        LogMark curMarkAfterFirstSetOfAdds = lastLogMarkAfterFirstSetOfAdds.getCurMark();

        File lastMarkFile = new File(ledgerDir, "lastMark");
        // lastMark file should be zero, because checkpoint hasn't happenend
        LogMark logMarkFileBeforeCheckpoint = readLastMarkFile(lastMarkFile);
        Assert.assertEquals("lastMarkFile before checkpoint should be zero", 0,
                logMarkFileBeforeCheckpoint.compare(new LogMark()));

        // wait for flushInterval for SyncThread to do next iteration of checkpoint
        Thread.sleep(conf.getFlushInterval() + 500);
        /*
         * since we have waited for more than flushInterval SyncThread should
         * have checkpointed. if entrylogperledger is not enabled, then we
         * checkpoint only when currentLog in EntryLogger is rotated. but if
         * entrylogperledger is enabled, then we checkpoint for every
         * flushInterval period
         */
        Assert.assertTrue("lastMark file must be existing, because checkpoint should have happened",
                lastMarkFile.exists());

        LastLogMark lastLogMarkAfterCheckpoint = server.getBookie().journals.get(0).getLastLogMark();
        LogMark curMarkAfterCheckpoint = lastLogMarkAfterCheckpoint.getCurMark();

        LogMark rolledLogMark = readLastMarkFile(lastMarkFile);
        Assert.assertNotEquals("rolledLogMark should not be zero, since checkpoint has happenend", 0,
                rolledLogMark.compare(new LogMark()));
        /*
         * Curmark should be equal before and after checkpoint, because we didnt
         * add new entries during this period
         */
        Assert.assertTrue("Curmark should be equal before and after checkpoint",
                curMarkAfterCheckpoint.compare(curMarkAfterFirstSetOfAdds) == 0);
        /*
         * Curmark after checkpoint should be equal to rolled logmark, because
         * we checkpointed
         */
        Assert.assertTrue("Curmark after first set of adds should be equal to rolled logmark",
                curMarkAfterCheckpoint.compare(rolledLogMark) == 0);

        // add more ledger/entries
        for (int i = numOfLedgers; i < 2 * numOfLedgers; i++) {
            int ledgerIndex = i;
            LedgerHandle handle = bkClient.createLedgerAdv((long) i, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(),
                    null);
            for (int j = 0; j < numOfEntries; j++) {
                handle.addEntry(j, dataBytes);
            }
            handle.close();
        }

        // wait for flushInterval for SyncThread to do next iteration of checkpoint
        Thread.sleep(conf.getFlushInterval() + 500);

        LastLogMark lastLogMarkAfterSecondSetOfAdds = server.getBookie().journals.get(0).getLastLogMark();
        LogMark curMarkAfterSecondSetOfAdds = lastLogMarkAfterSecondSetOfAdds.getCurMark();

        rolledLogMark = readLastMarkFile(lastMarkFile);
        /*
         * Curmark after checkpoint should be equal to rolled logmark, because
         * we checkpointed
         */
        Assert.assertTrue("Curmark after second set of adds should be equal to rolled logmark",
                curMarkAfterSecondSetOfAdds.compare(rolledLogMark) == 0);

        server.shutdown();
        bkClient.close();
    }

    /*
     * In this testcase, InterleavedLedgerStorage is used, entrylogperledger is
     * enabled and validate that when entrylog is rotated it doesn't do
     * checkpoint.
     */
    @Test
    public void testCheckpointOfILSEntryLogIsRotatedWithELPLEnabled() throws Exception {
        testCheckpointofILSWhenEntryLogIsRotated(true);
    }

    /*
     * In this testcase, InterleavedLedgerStorage is used, entrylogperledger is
     * not enabled and validate that when entrylog is rotated it does
     * checkpoint.
     */
    @Test
    public void testCheckpointOfILSEntryLogIsRotatedWithELPLDisabled() throws Exception {
        testCheckpointofILSWhenEntryLogIsRotated(false);
    }

    public void testCheckpointofILSWhenEntryLogIsRotated(boolean entryLogPerLedgerEnabled) throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                //set very high period for flushInterval
                .setFlushInterval(30000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(entryLogPerLedgerEnabled)
                .setLedgerStorageClass(InterleavedLedgerStorage.class.getName());

        Assert.assertEquals("Number of JournalDirs", 1, conf.getJournalDirs().length);
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);
        InterleavedLedgerStorage ledgerStorage = (InterleavedLedgerStorage) server.getBookie().ledgerStorage;

        int numOfEntries = 5;
        byte[] dataBytes = "data".getBytes();

        long ledgerId = 10;
        LedgerHandle handle = bkClient.createLedgerAdv(ledgerId, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            handle.addEntry(j, dataBytes);
        }
        handle.close();
        // simulate rolling entrylog
        ledgerStorage.entryLogger.rollLog();
        // sleep for a bit for checkpoint to do its task
        Thread.sleep(1000);

        File lastMarkFile = new File(ledgerDir, "lastMark");
        LogMark rolledLogMark = readLastMarkFile(lastMarkFile);
        if (entryLogPerLedgerEnabled) {
            Assert.assertEquals(
                    "rolledLogMark should be zero, since checkpoint"
                            + "shouldn't have happened when entryLog is rotated",
                    0, rolledLogMark.compare(new LogMark()));
        } else {
            Assert.assertNotEquals("rolledLogMark shouldn't be zero, since checkpoint"
                    + "should have happened when entryLog is rotated", 0, rolledLogMark.compare(new LogMark()));
        }
        bkClient.close();
        server.shutdown();
    }

    /*
     * In this testcase, SortedLedgerStorage is used, entrylogperledger is
     * enabled and validate that when entrylog is rotated it doesn't do
     * checkpoint.
     */
    @Test
    public void testCheckpointOfSLSEntryLogIsRotatedWithELPLEnabled() throws Exception {
        testCheckpointOfSLSWhenEntryLogIsRotated(true);
    }

    /*
     * In this testcase, SortedLedgerStorage is used, entrylogperledger is
     * not enabled and validate that when entrylog is rotated it does
     * checkpoint.
     */
    @Test
    public void testCheckpointOfSLSEntryLogIsRotatedWithELPLDisabled() throws Exception {
        testCheckpointOfSLSWhenEntryLogIsRotated(false);
    }

    public void testCheckpointOfSLSWhenEntryLogIsRotated(boolean entryLogPerLedgerEnabled) throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                //set very high period for flushInterval
                .setFlushInterval(30000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(entryLogPerLedgerEnabled)
                .setLedgerStorageClass(SortedLedgerStorage.class.getName())
                // set very low skipListSizeLimit and entryLogSizeLimit to simulate log file rotation
                .setSkipListSizeLimit(1 * 1000 * 1000)
                .setEntryLogSizeLimit(2 * 1000 * 1000);

        Assert.assertEquals("Number of JournalDirs", 1, conf.getJournalDirs().length);
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);
        InterleavedLedgerStorage ledgerStorage = (InterleavedLedgerStorage) server.getBookie().ledgerStorage;

        Random rand = new Random();
        byte[] dataBytes = new byte[10 * 1000];
        rand.nextBytes(dataBytes);
        int numOfEntries = ((int) conf.getEntryLogSizeLimit() + (100 * 1000)) / dataBytes.length;

        LedgerHandle handle = bkClient.createLedgerAdv(10, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            handle.addEntry(j, dataBytes);
        }
        handle.close();

        // sleep for a bit for checkpoint to do its task
        Thread.sleep(1000);

        File lastMarkFile = new File(ledgerDir, "lastMark");
        LogMark rolledLogMark = readLastMarkFile(lastMarkFile);
        if (entryLogPerLedgerEnabled) {
            Assert.assertEquals(
                    "rolledLogMark should be zero, since checkpoint"
                            + "shouldn't have happened when entryLog is rotated",
                    0, rolledLogMark.compare(new LogMark()));
        } else {
            Assert.assertNotEquals("rolledLogMark shouldn't be zero, since checkpoint"
                    + "should have happened when entryLog is rotated", 0, rolledLogMark.compare(new LogMark()));
        }
        bkClient.close();
        server.shutdown();
    }

    /*
     * in this method it checks if entryLogPerLedger is enabled, then
     * InterLeavedLedgerStorage.checkpoint flushes current activelog and flushes
     * all rotatedlogs and closes them.
     *
     */
    @Test
    public void testIfEntryLogPerLedgerEnabledCheckpointFlushesAllLogs() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                //set very high period for flushInterval
                .setFlushInterval(30000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(true)
                .setLedgerStorageClass(InterleavedLedgerStorage.class.getName())
                // set flushInterval to some very high number
                .setFlushIntervalInBytes(10000000);

        Assert.assertEquals("Number of JournalDirs", 1, conf.getJournalDirs().length);
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);
        InterleavedLedgerStorage ledgerStorage = (InterleavedLedgerStorage) server.getBookie().ledgerStorage;
        EntryLogger entryLogger = ledgerStorage.entryLogger;

        int numOfEntries = 5;
        byte[] dataBytes = "data".getBytes();

        long ledgerId = 10;
        LedgerHandle handle = bkClient.createLedgerAdv(ledgerId, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            handle.addEntry(j, dataBytes);
        }
        handle.close();
        // simulate rolling entrylog
        ledgerStorage.entryLogger.rollLog();

        ledgerId = 20;
        handle = bkClient.createLedgerAdv(ledgerId, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            handle.addEntry(j, dataBytes);
        }
        handle.close();
        // simulate rolling entrylog
        ledgerStorage.entryLogger.rollLog();

        ledgerId = 30;
        handle = bkClient.createLedgerAdv(ledgerId, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
        for (int j = 0; j < numOfEntries; j++) {
            handle.addEntry(j, dataBytes);
        }
        handle.close();

        Assert.assertNotEquals("bytesWrittenSinceLastFlush shouldn't be zero", 0,
                entryLogger.bytesWrittenSinceLastFlush);
        Assert.assertNotEquals("There should be logChannelsToFlush", 0, entryLogger.logChannelsToFlush.size());

        ledgerStorage.checkpoint(ledgerStorage.checkpointSource.newCheckpoint());

        Assert.assertTrue("There shouldn't be logChannelsToFlush",
                ((entryLogger.logChannelsToFlush == null) || (entryLogger.logChannelsToFlush.size() == 0)));

        Assert.assertEquals("bytesWrittenSinceLastFlush should be zero", 0, entryLogger.bytesWrittenSinceLastFlush);
    }

    static class MockInterleavedLedgerStorage extends InterleavedLedgerStorage {
        @Override
        public void shutdown() {
            // During BookieServer shutdown this method will be called
            // and we want it to be noop.
            // do nothing
        }

        @Override
        public synchronized void flush() throws IOException {
            // this method will be called by SyncThread.shutdown.
            // During BookieServer shutdown we want this method to be noop
            // do nothing
        }
    }

    /*
     * This is complete end-to-end scenario.
     *
     * 1) This testcase uses MockInterleavedLedgerStorage, which extends
     * InterleavedLedgerStorage but doesn't do anything when Bookie is shutdown.
     * This is needed to simulate Bookie crash.
     * 2) entryLogPerLedger is enabled
     * 3) ledgers are created and entries are added.
     * 4) wait for flushInterval period for checkpoint to complete
     * 6) delete the journal files and lastmark file
     * 7) Now restart the Bookie
     * 8) validate that the entries which were written can be read successfully.
     */
    @Test
    public void testCheckPointForEntryLoggerWithMultipleActiveEntryLogs() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                .setFlushInterval(3000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(true)
                .setLedgerStorageClass(MockInterleavedLedgerStorage.class.getName());
        Assert.assertEquals("Number of JournalDirs", 1, conf.getJournalDirs().length);
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper bkClient = new BookKeeper(clientConf);

        ExecutorService threadPool = Executors.newFixedThreadPool(30);
        int numOfLedgers = 12;
        int numOfEntries = 100;
        byte[] dataBytes = "data".getBytes();
        LedgerHandle[] handles = new LedgerHandle[numOfLedgers];
        AtomicBoolean receivedExceptionForAdd = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(numOfLedgers * numOfEntries);
        for (int i = 0; i < numOfLedgers; i++) {
            int ledgerIndex = i;
            handles[i] = bkClient.createLedgerAdv((long) i, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
            for (int j = 0; j < numOfEntries; j++) {
                int entryIndex = j;
                threadPool.submit(() -> {
                    try {
                        handles[ledgerIndex].addEntry(entryIndex, dataBytes);
                    } catch (Exception e) {
                        LOG.error("Got Exception while trying to addEntry for ledgerId: " + ledgerIndex + " entry: "
                                + entryIndex, e);
                        receivedExceptionForAdd.set(true);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        Assert.assertTrue("It is expected add requests are supposed to be completed in 20000 secs",
                countDownLatch.await(20000, TimeUnit.MILLISECONDS));
        Assert.assertFalse("there shouldn't be any exceptions for addentry requests", receivedExceptionForAdd.get());
        for (int i = 0; i < numOfLedgers; i++) {
            handles[i].close();
        }
        threadPool.shutdown();

        LastLogMark lastLogMarkBeforeCheckpoint = server.getBookie().journals.get(0).getLastLogMark();
        LogMark curMarkBeforeCheckpoint = lastLogMarkBeforeCheckpoint.getCurMark();

        Thread.sleep(conf.getFlushInterval() + 1000);
        // since we have waited for more than flushInterval SyncThread should have checkpointed.
        // if entrylogperledger is not enabled, then we checkpoint only when currentLog in EntryLogger
        // is rotated. but if entrylogperledger is enabled, then we checkpoint for every flushInterval period
        File lastMarkFile = new File(ledgerDir, "lastMark");
        Assert.assertTrue("lastMark file must be existing, because checkpoint should have happened",
                lastMarkFile.exists());

        bkClient.close();
        // here we are calling shutdown, but MockInterleavedLedgerStorage shudown/flush
        // methods are noop, so entrylogger is not flushed as part of this shutdown
        // here we are trying to simulate Bookie crash, but there is no way to
        // simulate bookie abrupt crash
        server.shutdown();

        // delete journal files and lastMark, to make sure that we are not reading from
        // Journal file
        File journalDirectory = Bookie.getCurrentDirectory(conf.getJournalDirs()[0]);
        List<Long> journalLogsId = Journal.listJournalIds(journalDirectory, null);
        for (long journalId : journalLogsId) {
            File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
            journalFile.delete();
        }

        // we know there is only one ledgerDir
        lastMarkFile = new File(ledgerDir, "lastMark");
        lastMarkFile.delete();

        // now we are restarting BookieServer
        server = new BookieServer(conf);
        server.start();
        bkClient = new BookKeeper(clientConf);
        // since Bookie checkpointed successfully before shutdown/crash,
        // we should be able to read from entryLogs though journal is deleted
        for (int i = 0; i < numOfLedgers; i++) {
            LedgerHandle lh = bkClient.openLedger(i, DigestType.CRC32, "passwd".getBytes());
            Enumeration<LedgerEntry> entries = lh.readEntries(0, numOfEntries - 1);
            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                byte[] readData = entry.getEntry();
                Assert.assertEquals("Ledger Entry Data should match", new String("data".getBytes()),
                        new String(readData));
            }
        }
        bkClient.close();
        server.shutdown();
    }
}
