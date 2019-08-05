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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.bookkeeper.bookie.EntryLogManagerForEntryLogPerLedger.BufferedLogChannelWithDirInfo;
import org.apache.bookkeeper.bookie.EntryLogger.BufferedLogChannel;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LedgerStorageCheckpointTest.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SyncThread.class)
@PowerMockIgnore("javax.*")
public class LedgerStorageCheckpointTest {
    private static final Logger LOG = LoggerFactory
            .getLogger(LedgerStorageCheckpointTest.class);

    @Rule
    public final TestName runtime = new TestName();

    // ZooKeeper related variables
    protected final ZooKeeperUtil zkUtil = new ZooKeeperUtil();

    // BookKeeper related variables
    protected final List<File> tmpDirs = new LinkedList<File>();

    // ScheduledExecutorService used by SyncThread
    MockExecutorController executorController;

    @Before
    public void setUp() throws Exception {
        LOG.info("Setting up test {}", getClass());
        PowerMockito.mockStatic(Executors.class);

        try {
            // start zookeeper service
            startZKCluster();
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }

        ScheduledExecutorService scheduledExecutorService = PowerMockito.mock(ScheduledExecutorService.class);
        executorController = new MockExecutorController()
                .controlSubmit(scheduledExecutorService)
                .controlScheduleAtFixedRate(scheduledExecutorService, 10);
        PowerMockito.when(scheduledExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        PowerMockito.when(Executors.newSingleThreadScheduledExecutor(any())).thenReturn(scheduledExecutorService);
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("TearDown");
        Exception tearDownException = null;
        // stop zookeeper service
        try {
            stopZKCluster();
        } catch (Exception e) {
            LOG.error("Got Exception while trying to stop ZKCluster", e);
            tearDownException = e;
        }
        // cleanup temp dirs
        try {
            cleanupTempDirs();
        } catch (Exception e) {
            LOG.error("Got Exception while trying to cleanupTempDirs", e);
            tearDownException = e;
        }
        if (tearDownException != null) {
            throw tearDownException;
        }
    }

    /**
     * Start zookeeper cluster.
     *
     * @throws Exception
     */
    protected void startZKCluster() throws Exception {
        zkUtil.startCluster();
    }

    /**
     * Stop zookeeper cluster.
     *
     * @throws Exception
     */
    protected void stopZKCluster() throws Exception {
        zkUtil.killCluster();
    }

    protected void cleanupTempDirs() throws Exception {
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    protected File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tmpDirs.add(dir);
        return dir;
    }

    private LogMark readLastMarkFile(File lastMarkFile) throws IOException {
        byte[] buff = new byte[16];
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
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
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
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
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
        executorController.advance(Duration.ofMillis(conf.getFlushInterval()));
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
        executorController.advance(Duration.ofMillis(conf.getFlushInterval()));

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
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
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
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
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
        ((EntryLogManagerBase) ledgerStorage.getEntryLogger().getEntryLogManager()).createNewLog(ledgerId);
        // sleep for a bit for checkpoint to do its task
        executorController.advance(Duration.ofMillis(500));

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
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
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
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkClient = new BookKeeper(clientConf);

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
        executorController.advance(Duration.ofMillis(500));

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
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
                .setZkTimeout(5000)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setAutoRecoveryDaemonEnabled(false)
                //set flushInterval
                .setFlushInterval(3000)
                .setBookiePort(PortManager.nextFreePort())
                // entrylog per ledger is enabled
                .setEntryLogPerLedgerEnabled(true)
                .setLedgerStorageClass(InterleavedLedgerStorage.class.getName())
                // set setFlushIntervalInBytes to some very high number
                .setFlushIntervalInBytes(10000000);

        Assert.assertEquals("Number of JournalDirs", 1, conf.getJournalDirs().length);
        // we know there is only one ledgerDir
        File ledgerDir = Bookie.getCurrentDirectories(conf.getLedgerDirs())[0];
        BookieServer server = new BookieServer(conf);
        server.start();
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkClient = new BookKeeper(clientConf);
        InterleavedLedgerStorage ledgerStorage = (InterleavedLedgerStorage) server.getBookie().ledgerStorage;
        EntryLogger entryLogger = ledgerStorage.entryLogger;
        EntryLogManagerForEntryLogPerLedger entryLogManager = (EntryLogManagerForEntryLogPerLedger) entryLogger
                .getEntryLogManager();

        Random rand = new Random();
        int numOfEntries = 5;
        byte[] dataBytes = "data".getBytes();

        int numOfLedgers = 3;
        long[] ledgerIds = new long[numOfLedgers];
        LedgerHandle handle;
        for (int i = 0; i < numOfLedgers; i++) {
            ledgerIds[i] = rand.nextInt(100000) + 1;
            handle = bkClient.createLedgerAdv(ledgerIds[i], 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
            for (int j = 0; j < numOfEntries; j++) {
                handle.addEntry(j, dataBytes);
            }
            // simulate rolling entrylog
            entryLogManager.createNewLog(ledgerIds[i]);
        }

        Set<BufferedLogChannelWithDirInfo> copyOfCurrentLogsWithDirInfo = entryLogManager.getCopyOfCurrentLogs();
        for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            Assert.assertNotEquals("bytesWrittenSinceLastFlush shouldn't be zero", 0,
                    currentLogWithDirInfo.getLogChannel().getUnpersistedBytes());
        }
        Assert.assertNotEquals("There should be logChannelsToFlush", 0,
                entryLogManager.getRotatedLogChannels().size());

        /*
         * wait for atleast flushInterval period, so that checkpoint can happen.
         */
        executorController.advance(Duration.ofMillis(conf.getFlushInterval()));

        /*
         * since checkpoint happenend, there shouldn't be any logChannelsToFlush
         * and bytesWrittenSinceLastFlush should be zero.
         */
        List<BufferedLogChannel> copyOfRotatedLogChannels = entryLogManager.getRotatedLogChannels();
        Assert.assertTrue("There shouldn't be logChannelsToFlush",
                ((copyOfRotatedLogChannels == null) || (copyOfRotatedLogChannels.size() == 0)));

        copyOfCurrentLogsWithDirInfo = entryLogManager.getCopyOfCurrentLogs();
        for (BufferedLogChannelWithDirInfo currentLogWithDirInfo : copyOfCurrentLogsWithDirInfo) {
            Assert.assertEquals("bytesWrittenSinceLastFlush should be zero", 0,
                    currentLogWithDirInfo.getLogChannel().getUnpersistedBytes());
        }
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
     * 5) simulate bookie crash
     * 6) delete the journal files and lastmark file
     * 7) Now restart the Bookie
     * 8) validate that the entries which were written can be read successfully.
     */
    @Test
    public void testCheckPointForEntryLoggerWithMultipleActiveEntryLogs() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri())
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
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        final BookKeeper bkClient = new BookKeeper(clientConf);

        int numOfLedgers = 12;
        int numOfEntries = 100;
        byte[] dataBytes = "data".getBytes();
        AtomicBoolean receivedExceptionForAdd = new AtomicBoolean(false);
        LongStream.range(0, numOfLedgers).parallel().mapToObj((ledgerId) -> {
            LedgerHandle handle = null;
            try {
                handle = bkClient.createLedgerAdv(ledgerId, 1, 1, 1, DigestType.CRC32, "passwd".getBytes(), null);
            } catch (BKException | InterruptedException exc) {
                receivedExceptionForAdd.compareAndSet(false, true);
                LOG.error("Got Exception while trying to create LedgerHandle for ledgerId: " + ledgerId, exc);
            }
            return handle;
        }).forEach((writeHandle) -> {
            IntStream.range(0, numOfEntries).forEach((entryId) -> {
                try {
                    writeHandle.addEntry(entryId, dataBytes);
                } catch (BKException | InterruptedException exc) {
                    receivedExceptionForAdd.compareAndSet(false, true);
                    LOG.error("Got Exception while trying to AddEntry of ledgerId: " + writeHandle.getId()
                            + " entryId: " + entryId, exc);
                }
            });
            try {
                writeHandle.close();
            } catch (BKException | InterruptedException e) {
                receivedExceptionForAdd.compareAndSet(false, true);
                LOG.error("Got Exception while trying to close writeHandle of ledgerId: " + writeHandle.getId(), e);
            }
        });

        Assert.assertFalse(
                "There shouldn't be any exceptions while creating writeHandle and adding entries to writeHandle",
                receivedExceptionForAdd.get());

        executorController.advance(Duration.ofMillis(conf.getFlushInterval()));
        // since we have waited for more than flushInterval SyncThread should have checkpointed.
        // if entrylogperledger is not enabled, then we checkpoint only when currentLog in EntryLogger
        // is rotated. but if entrylogperledger is enabled, then we checkpoint for every flushInterval period
        File lastMarkFile = new File(ledgerDir, "lastMark");
        Assert.assertTrue("lastMark file must be existing, because checkpoint should have happened",
                lastMarkFile.exists());
        LogMark rolledLogMark = readLastMarkFile(lastMarkFile);
        Assert.assertNotEquals("rolledLogMark should not be zero, since checkpoint has happenend", 0,
                rolledLogMark.compare(new LogMark()));

        bkClient.close();
        // here we are calling shutdown, but MockInterleavedLedgerStorage shudown/flush
        // methods are noop, so entrylogger is not flushed as part of this shutdown
        // here we are trying to simulate Bookie crash, but there is no way to
        // simulate bookie abrupt crash
        server.shutdown();

        // delete journal files and lastMark, to make sure that we are not reading from
        // Journal file
        File[] journalDirs = conf.getJournalDirs();
        for (File journalDir : journalDirs) {
            File journalDirectory = Bookie.getCurrentDirectory(journalDir);
            List<Long> journalLogsId = Journal.listJournalIds(journalDirectory, null);
            for (long journalId : journalLogsId) {
                File journalFile = new File(journalDirectory, Long.toHexString(journalId) + ".txn");
                journalFile.delete();
            }
        }

        // we know there is only one ledgerDir
        lastMarkFile = new File(ledgerDir, "lastMark");
        lastMarkFile.delete();

        // now we are restarting BookieServer
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        server = new BookieServer(conf);
        server.start();
        BookKeeper newBKClient = new BookKeeper(clientConf);
        // since Bookie checkpointed successfully before shutdown/crash,
        // we should be able to read from entryLogs though journal is deleted

        AtomicBoolean receivedExceptionForRead = new AtomicBoolean(false);

        LongStream.range(0, numOfLedgers).parallel().forEach((ledgerId) -> {
            try {
                LedgerHandle lh = newBKClient.openLedger(ledgerId, DigestType.CRC32, "passwd".getBytes());
                Enumeration<LedgerEntry> entries = lh.readEntries(0, numOfEntries - 1);
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    byte[] readData = entry.getEntry();
                    Assert.assertEquals("Ledger Entry Data should match", new String("data".getBytes()),
                            new String(readData));
                }
                lh.close();
            } catch (BKException | InterruptedException e) {
                receivedExceptionForRead.compareAndSet(false, true);
                LOG.error("Got Exception while trying to read entries of ledger, ledgerId: " + ledgerId, e);
            }
        });
        Assert.assertFalse("There shouldn't be any exceptions while creating readHandle and while reading"
                + "entries using readHandle", receivedExceptionForRead.get());

        newBKClient.close();
        server.shutdown();
    }
}
