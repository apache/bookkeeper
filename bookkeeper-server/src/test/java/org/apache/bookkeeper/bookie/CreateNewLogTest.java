/**
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

package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogManagerBase;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogManagerForSingleEntryLog;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test new log creation.
 */
public class CreateNewLogTest {
    private static final Logger LOG = LoggerFactory
    .getLogger(CreateNewLogTest.class);

    private String[] ledgerDirs;
    private int numDirs = 100;

    @Before
    public void setUp() throws Exception{
        ledgerDirs = new String[numDirs];
        for (int i = 0; i < numDirs; i++){
            File temp = File.createTempFile("bookie", "test");
            temp.delete();
            temp.mkdir();
            File currentTemp = new File(temp.getAbsoluteFile() + "/current");
            currentTemp.mkdir();
            ledgerDirs[i] = temp.getPath();
        }
    }

    @After
    public void tearDown() throws Exception{
        for (int i = 0; i < numDirs; i++){
            File f = new File(ledgerDirs[i]);
            deleteRecursive(f);
        }
    }

    private void deleteRecursive(File f) {
        if (f.isDirectory()){
            for (File c : f.listFiles()){
                deleteRecursive(c);
            }
        }

        f.delete();
    }

    /**
     * Checks if new log file id is verified against all directories.
     *
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-465}
     *
     * @throws Exception
     */
    @Test
    public void testCreateNewLog() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        EntryLogManagerForSingleEntryLog entryLogManager = (EntryLogManagerForSingleEntryLog) el.getEntryLogManager();
        entryLogManager.createNewLog(0L);
        LOG.info("This is the current log id: {}", entryLogManager.getCurrentLogId());
        assertTrue("Wrong log id", entryLogManager.getCurrentLogId() > 1);
    }

    @Test
    public void testCreateNewLogWithNoWritableLedgerDirs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setIsForceGCAllowWhenNoSpace(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        // Now let us move all dirs to filled dirs
        List<File> wDirs = ledgerDirsManager.getWritableLedgerDirs();
        for (File tdir: wDirs) {
            ledgerDirsManager.addToFilledDirs(tdir);
        }

        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        // Calls createNewLog, and with the number of directories we
        // are using, if it picks one at random it will fail.
        EntryLogManagerForSingleEntryLog entryLogManager = (EntryLogManagerForSingleEntryLog) el.getEntryLogManager();
        entryLogManager.createNewLog(0L);
        LOG.info("This is the current log id: {}", entryLogManager.getCurrentLogId());
        assertTrue("Wrong log id", entryLogManager.getCurrentLogId() > 1);
    }

    @Test
    public void testConcurrentCreateNewLogWithEntryLogFilePreAllocationEnabled() throws Exception {
        testConcurrentCreateNewLog(true);
    }

    @Test
    public void testConcurrentCreateNewLogWithEntryLogFilePreAllocationDisabled() throws Exception {
        testConcurrentCreateNewLog(false);
    }

    public void testConcurrentCreateNewLog(boolean entryLogFilePreAllocationEnabled) throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(entryLogFilePreAllocationEnabled);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManager = (EntryLogManagerBase) el.getEntryLogManager();
        Assert.assertEquals("previousAllocatedEntryLogId after initialization", -1,
                el.getPreviousAllocatedEntryLogId());
        Assert.assertEquals("leastUnflushedLogId after initialization", 0, el.getLeastUnflushedLogId());
        int createNewLogNumOfTimes = 10;
        AtomicBoolean receivedException = new AtomicBoolean(false);

        IntStream.range(0, createNewLogNumOfTimes).parallel().forEach((i) -> {
            try {
                (entryLogManager).createNewLog((long) i);
            } catch (IOException e) {
                LOG.error("Received exception while creating newLog", e);
                receivedException.set(true);
            }
        });
        // wait for the pre-allocation to complete
        Thread.sleep(1000);

        Assert.assertFalse("There shouldn't be any exceptions while creating newlog", receivedException.get());
        int expectedPreviousAllocatedEntryLogId = createNewLogNumOfTimes - 1;
        if (entryLogFilePreAllocationEnabled) {
            expectedPreviousAllocatedEntryLogId = createNewLogNumOfTimes;
        }

        Assert.assertEquals(
                "previousAllocatedEntryLogId after " + createNewLogNumOfTimes
                        + " number of times createNewLog is called",
                expectedPreviousAllocatedEntryLogId, el.getPreviousAllocatedEntryLogId());
        Assert.assertEquals("Number of RotatedLogChannels", createNewLogNumOfTimes - 1,
                entryLogManager.getRotatedLogChannels().size());
    }

    @Test
    public void testCreateNewLogWithGaps() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(false);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        EntryLogManagerBase entryLogManagerBase = (EntryLogManagerBase) el.getEntryLogManager();
        entryLogManagerBase.createNewLog(0L);

        Assert.assertEquals("previousAllocatedEntryLogId after initialization", 0, el.getPreviousAllocatedEntryLogId());

        // Extracted from createNewLog()
        String logFileName = Long.toHexString(1) + ".log";
        File dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        File newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        entryLogManagerBase.createNewLog(0L);
        Assert.assertEquals("previousAllocatedEntryLogId since entrylogid 1 is already taken", 2,
                el.getPreviousAllocatedEntryLogId());

        // Extracted from createNewLog()
        logFileName = Long.toHexString(3) + ".log";
        dir = ledgerDirsManager.pickRandomWritableDir();
        LOG.info("Picked this directory: {}", dir);
        newLogFile = new File(dir, logFileName);
        newLogFile.createNewFile();

        entryLogManagerBase.createNewLog(0L);
        Assert.assertEquals("previousAllocatedEntryLogId since entrylogid 3 is already taken", 4,
                el.getPreviousAllocatedEntryLogId());
    }

    @Test
    public void testCreateNewLogAndCompactionLog() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

        // Creating a new configuration with a number of
        // ledger directories.
        conf.setLedgerDirNames(ledgerDirs);
        conf.setEntryLogFilePreAllocationEnabled(true);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        EntryLogger el = new EntryLogger(conf, ledgerDirsManager);
        AtomicBoolean receivedException = new AtomicBoolean(false);

        IntStream.range(0, 2).parallel().forEach((i) -> {
            try {
                if (i % 2 == 0) {
                    ((EntryLogManagerBase) el.getEntryLogManager()).createNewLog((long) i);
                } else {
                    el.createNewCompactionLog();
                }
            } catch (IOException e) {
                LOG.error("Received exception while creating newLog", e);
                receivedException.set(true);
            }
        });
        // wait for the pre-allocation to complete
        Thread.sleep(1000);

        Assert.assertFalse("There shouldn't be any exceptions while creating newlog", receivedException.get());
        Assert.assertEquals(
                "previousAllocatedEntryLogId after 2 times createNewLog is called", 2,
                el.getPreviousAllocatedEntryLogId());
    }
}
