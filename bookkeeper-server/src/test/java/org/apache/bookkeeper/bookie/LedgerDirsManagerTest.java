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
package org.apache.bookkeeper.bookie;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Test LedgerDirsManager.
 */
@RunWith(MockitoJUnitRunner.class)
public class LedgerDirsManagerTest {

    ServerConfiguration conf;
    File curDir;
    LedgerDirsManager dirsManager;
    LedgerDirsMonitor ledgerMonitor;
    MockDiskChecker mockDiskChecker;
    private TestStatsProvider statsProvider;
    private TestStatsProvider.TestStatsLogger statsLogger;
    int diskCheckInterval = 1000;
    float threshold = 0.5f;
    float warnThreshold = 0.5f;

    final List<File> tempDirs = new ArrayList<File>();

    // Thread used by monitor
    ScheduledExecutorService executor;
    MockExecutorController executorController;
    MockedStatic<Executors> executorsMockedStatic;

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @Before
    public void setUp() throws Exception {
        executorsMockedStatic = mockStatic(Executors.class);

        File tmpDir = createTempDir("bkTest", ".dir");
        curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setDiskLowWaterMarkUsageThreshold(conf.getDiskUsageThreshold());
        conf.setDiskCheckInterval(diskCheckInterval);
        conf.setIsForceGCAllowWhenNoSpace(true);
        conf.setMinUsableSizeForEntryLogCreation(Long.MIN_VALUE);

        executor = mock(ScheduledExecutorService.class);
        executorController = new MockExecutorController()
            .controlScheduleAtFixedRate(executor, 10);
        executorsMockedStatic.when(()->Executors.newSingleThreadScheduledExecutor(any())).thenReturn(executor);

        mockDiskChecker = new MockDiskChecker(threshold, warnThreshold);
        statsProvider = new TestStatsProvider();
        statsLogger = statsProvider.getStatsLogger("test");
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), statsLogger);
        ledgerMonitor = new LedgerDirsMonitor(conf,
                mockDiskChecker, Collections.singletonList(dirsManager));
        ledgerMonitor.init();
    }

    @After
    public void tearDown() throws Exception {
        executorsMockedStatic.close();
        ledgerMonitor.shutdown();
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    @Test
    public void testGetWritableDir() throws Exception {
        try {
            List<File> writeDirs = dirsManager.getWritableLedgerDirs();
            assertTrue("Must have a writable ledgerDir", writeDirs.size() > 0);
        } catch (NoWritableLedgerDirException nwlde) {
            fail("We should have a writable ledgerDir");
        }
    }

    @Test
    public void testPickWritableDirExclusive() throws Exception {
        try {
            dirsManager.pickRandomWritableDir(curDir);
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            assertTrue(true);
        }
    }

    @Test
    public void testNoWritableDir() throws Exception {
        try {
            dirsManager.addToFilledDirs(curDir);
            dirsManager.pickRandomWritableDir();
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            assertEquals("Should got NoWritableLedgerDirException w/ 'All ledger directories are non writable'.",
                         "All ledger directories are non writable", nwlde.getMessage());
        }
    }

    @Test
    public void testGetWritableDirForLog() throws Exception {
        List<File> writeDirs;
        try {
            dirsManager.addToFilledDirs(curDir);
            dirsManager.getWritableLedgerDirs();
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            // Now make sure we can get one for log
            try {
                writeDirs = dirsManager.getWritableLedgerDirsForNewLog();
                assertTrue("Must have a writable ledgerDir", writeDirs.size() > 0);
            } catch (NoWritableLedgerDirException e) {
                fail("We should have a writeble ledgerDir");
            }
        }
    }

    @Test
    public void testGetWritableDirForLogNoEnoughDiskSpace() throws Exception {
        conf.setMinUsableSizeForEntryLogCreation(curDir.getUsableSpace() + 1024);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), statsLogger);
        try {
            dirsManager.addToFilledDirs(curDir);
            dirsManager.getWritableLedgerDirs();
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            // Now make sure we can get one for log
            try {
                dirsManager.getWritableLedgerDirsForNewLog();
                fail("Should not reach here due to there is no enough disk space left");
            } catch (NoWritableLedgerDirException e) {
                // expected.
            }
        }
    }

    @Test
    public void testLedgerDirsMonitorDuringTransition() throws Exception {
        testLedgerDirsMonitorDuringTransition(true);
    }

    @Test
    public void testHighPriorityWritesDisallowedDuringTransition() throws Exception {
        testLedgerDirsMonitorDuringTransition(false);
    }

    private void testLedgerDirsMonitorDuringTransition(boolean highPriorityWritesAllowed) throws Exception {
        if (!highPriorityWritesAllowed) {
            ledgerMonitor.shutdown();
            conf.setMinUsableSizeForHighPriorityWrites(curDir.getUsableSpace() + 1024);
            dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), statsLogger);
            ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, Collections.singletonList(dirsManager));
            ledgerMonitor.init();
        }

        MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        assertFalse(mockLedgerDirsListener.readOnly);
        assertTrue(mockLedgerDirsListener.highPriorityWritesAllowed);

        mockDiskChecker.setUsage(threshold + 0.05f);
        executorController.advance(Duration.ofMillis(diskCheckInterval));

        assertTrue(mockLedgerDirsListener.readOnly);
        assertEquals(highPriorityWritesAllowed, mockLedgerDirsListener.highPriorityWritesAllowed);

        mockDiskChecker.setUsage(threshold - 0.05f);
        executorController.advance(Duration.ofMillis(diskCheckInterval));

        assertFalse(mockLedgerDirsListener.readOnly);
        assertTrue(mockLedgerDirsListener.highPriorityWritesAllowed);
    }

    @Test
    public void testIsReadOnlyModeOnAnyDiskFullEnabled() throws Exception {
        testAnyLedgerFullTransitToReadOnly(true);
        testAnyLedgerFullTransitToReadOnly(false);
    }

    @Test
    public void testTriggerLedgerDirListener() throws Exception {
        ledgerMonitor.shutdown();

        final float nospace = 0.90f;
        final float lwm = 0.80f;
        HashMap<File, Float> usageMap;

        File tmpDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = BookieImpl.getCurrentDirectory(tmpDir1);
        BookieImpl.checkDirectoryStructure(curDir1);

        File tmpDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = BookieImpl.getCurrentDirectory(tmpDir2);
        BookieImpl.checkDirectoryStructure(curDir2);

        conf.setDiskUsageThreshold(nospace);
        conf.setDiskLowWaterMarkUsageThreshold(lwm);
        conf.setDiskUsageWarnThreshold(nospace);
        conf.setReadOnlyModeOnAnyDiskFullEnabled(false);
        conf.setLedgerDirNames(new String[] { tmpDir1.toString(), tmpDir2.toString() });

        mockDiskChecker = new MockDiskChecker(nospace, warnThreshold);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), statsLogger);
        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, Collections.singletonList(dirsManager));
        usageMap = new HashMap<>();
        usageMap.put(curDir1, 0.1f);
        usageMap.put(curDir2, 0.1f);
        mockDiskChecker.setUsageMap(usageMap);
        ledgerMonitor.init();
        final MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        final CountDownLatch diskAlmostFull = new CountDownLatch(1);
        final CountDownLatch diskFull = new CountDownLatch(1);

        dirsManager.addLedgerDirsListener(new LedgerDirsListener() {
            @Override
            public void diskAlmostFull(File disk) {
                if (disk.equals(curDir1)) {
                    diskAlmostFull.countDown();
                }

            }

            @Override
            public void diskFull(File disk) {
                if (disk.equals(curDir1)) {
                    diskFull.countDown();
                }


            }
        });

        Thread.sleep((diskCheckInterval * 2) + 100);
        assertFalse(mockLedgerDirsListener.readOnly);

        // diskAlmostFull
        setUsageAndThenVerify(curDir1, nospace - 0.6f, curDir2, nospace - 0.20f, mockDiskChecker,
                mockLedgerDirsListener, false);
        assertEquals(1, diskAlmostFull.getCount());
        setUsageAndThenVerify(curDir1, nospace - 0.2f, curDir2, nospace - 0.60f, mockDiskChecker,
                mockLedgerDirsListener, false);
        assertEquals(0, diskAlmostFull.getCount());

        // diskFull
        setUsageAndThenVerify(curDir1, nospace - 0.6f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, false);
        assertEquals(1, diskFull.getCount());
        setUsageAndThenVerify(curDir1, nospace + 0.05f, curDir2, nospace - 0.20f, mockDiskChecker,
                mockLedgerDirsListener, true);
        assertEquals(0, diskFull.getCount());
    }

    public void testAnyLedgerFullTransitToReadOnly(boolean isReadOnlyModeOnAnyDiskFullEnabled) throws Exception {
        ledgerMonitor.shutdown();

        final float nospace = 0.90f;
        final float lwm = 0.80f;
        HashMap<File, Float> usageMap;

        File tmpDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = BookieImpl.getCurrentDirectory(tmpDir1);
        BookieImpl.checkDirectoryStructure(curDir1);

        File tmpDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = BookieImpl.getCurrentDirectory(tmpDir2);
        BookieImpl.checkDirectoryStructure(curDir2);

        conf.setDiskUsageThreshold(nospace);
        conf.setDiskLowWaterMarkUsageThreshold(lwm);
        conf.setDiskUsageWarnThreshold(nospace);
        conf.setReadOnlyModeOnAnyDiskFullEnabled(isReadOnlyModeOnAnyDiskFullEnabled);
        conf.setLedgerDirNames(new String[] { tmpDir1.toString(), tmpDir2.toString() });

        mockDiskChecker = new MockDiskChecker(nospace, warnThreshold);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), statsLogger);
        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, Collections.singletonList(dirsManager));
        usageMap = new HashMap<>();
        usageMap.put(curDir1, 0.1f);
        usageMap.put(curDir2, 0.1f);
        mockDiskChecker.setUsageMap(usageMap);
        ledgerMonitor.init();
        final MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        Thread.sleep((diskCheckInterval * 2) + 100);
        assertFalse(mockLedgerDirsListener.readOnly);

        if (isReadOnlyModeOnAnyDiskFullEnabled) {
            setUsageAndThenVerify(curDir1, 0.1f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, true);
            setUsageAndThenVerify(curDir1, nospace + 0.05f, curDir2, 0.1f, mockDiskChecker,
                mockLedgerDirsListener, true);
            setUsageAndThenVerify(curDir1, nospace + 0.05f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, true);
            setUsageAndThenVerify(curDir1, nospace - 0.30f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, true);
            setUsageAndThenVerify(curDir1, nospace - 0.20f, curDir2, nospace - 0.20f, mockDiskChecker,
                mockLedgerDirsListener, false);
        } else {
            setUsageAndThenVerify(curDir1, 0.1f, curDir2, 0.1f, mockDiskChecker,
                mockLedgerDirsListener, false);
            setUsageAndThenVerify(curDir1, 0.1f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, false);
            setUsageAndThenVerify(curDir1, nospace + 0.05f, curDir2, 0.1f, mockDiskChecker,
                mockLedgerDirsListener, false);
            setUsageAndThenVerify(curDir1, nospace + 0.05f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, true);
            setUsageAndThenVerify(curDir1, nospace - 0.30f, curDir2, nospace + 0.05f, mockDiskChecker,
                mockLedgerDirsListener, false);
            setUsageAndThenVerify(curDir1, nospace - 0.20f, curDir2, nospace - 0.20f, mockDiskChecker,
                mockLedgerDirsListener, false);
        }
    }

    @Test
    public void testLedgerDirsMonitorHandlingLowWaterMark() throws Exception {
        ledgerMonitor.shutdown();

        final float warn = 0.90f;
        final float nospace = 0.98f;
        final float lwm = (warn + nospace) / 2;
        final float lwm2warn = (warn + lwm) / 2;
        final float lwm2nospace = (lwm + nospace) / 2;
        final float nospaceExceeded = nospace + 0.005f;

        conf.setDiskUsageThreshold(nospace);
        conf.setDiskLowWaterMarkUsageThreshold(lwm);
        conf.setDiskUsageWarnThreshold(warn);

        mockDiskChecker = new MockDiskChecker(nospace, warnThreshold);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, Collections.singletonList(dirsManager));
        ledgerMonitor.init();
        final MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertFalse(mockLedgerDirsListener.readOnly);

        // go above LWM but below threshold
        // should still be writable
        mockDiskChecker.setUsage(lwm2nospace);
        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertFalse(mockLedgerDirsListener.readOnly);

        // exceed the threshold, should go to readonly
        mockDiskChecker.setUsage(nospaceExceeded);
        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertTrue(mockLedgerDirsListener.readOnly);

        // drop below threshold but above LWM
        // should stay read-only
        mockDiskChecker.setUsage(lwm2nospace);
        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertTrue(mockLedgerDirsListener.readOnly);

        // drop below LWM
        // should become writable
        mockDiskChecker.setUsage(lwm2warn);
        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertFalse(mockLedgerDirsListener.readOnly);

        // go above LWM but below threshold
        // should still be writable
        mockDiskChecker.setUsage(lwm2nospace);
        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertFalse(mockLedgerDirsListener.readOnly);
    }

    @Test
    public void testLedgerDirsMonitorHandlingWithMultipleLedgerDirectories() throws Exception {
        ledgerMonitor.shutdown();

        final float nospace = 0.90f;
        final float lwm = 0.80f;
        HashMap<File, Float> usageMap;

        File tmpDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = BookieImpl.getCurrentDirectory(tmpDir1);
        BookieImpl.checkDirectoryStructure(curDir1);

        File tmpDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = BookieImpl.getCurrentDirectory(tmpDir2);
        BookieImpl.checkDirectoryStructure(curDir2);

        conf.setDiskUsageThreshold(nospace);
        conf.setDiskLowWaterMarkUsageThreshold(lwm);
        conf.setDiskUsageWarnThreshold(nospace);
        conf.setLedgerDirNames(new String[] { tmpDir1.toString(), tmpDir2.toString() });

        mockDiskChecker = new MockDiskChecker(nospace, warnThreshold);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()),
                statsLogger);
        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, Collections.singletonList(dirsManager));
        usageMap = new HashMap<File, Float>();
        usageMap.put(curDir1, 0.1f);
        usageMap.put(curDir2, 0.1f);
        mockDiskChecker.setUsageMap(usageMap);
        ledgerMonitor.init();
        final MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        Thread.sleep((diskCheckInterval * 2) + 100);
        assertFalse(mockLedgerDirsListener.readOnly);

        // go above LWM but below threshold
        // should still be writable
        setUsageAndThenVerify(curDir1, lwm + 0.05f, curDir2, lwm + 0.05f, mockDiskChecker, mockLedgerDirsListener,
                false);

        // one dir usagespace above storagethreshold, another dir below storagethreshold
        // should still be writable
        setUsageAndThenVerify(curDir1, nospace + 0.02f, curDir2, nospace - 0.05f, mockDiskChecker,
                mockLedgerDirsListener, true);

        // should remain readonly
        setUsageAndThenVerify(curDir1, nospace + 0.05f, curDir2, nospace + 0.02f, mockDiskChecker,
                mockLedgerDirsListener, true);

        // bring the disk usages to less than the threshold,
        // but more than the LWM.
        // should still be readonly
        setUsageAndThenVerify(curDir1, nospace - 0.05f, curDir2, nospace - 0.05f, mockDiskChecker,
                mockLedgerDirsListener, true);

        // bring one dir diskusage to less than lwm,
        // the other dir to be more than lwm, but the
        // overall diskusage to be more than lwm
        // should still be readonly
        setUsageAndThenVerify(curDir1, lwm - 0.03f, curDir2, lwm + 0.07f, mockDiskChecker, mockLedgerDirsListener,
                true);

        // bring one dir diskusage to much less than lwm,
        // the other dir to be more than storage threahold, but the
        // overall diskusage is less than lwm
        // should goto readwrite
        setUsageAndThenVerify(curDir1, lwm - 0.17f, curDir2, nospace + 0.03f, mockDiskChecker, mockLedgerDirsListener,
                true);
        assertEquals("Only one LedgerDir should be writable", 1, dirsManager.getWritableLedgerDirs().size());

        // bring both the dirs below lwm
        // should still be readwrite
        setUsageAndThenVerify(curDir1, lwm - 0.03f, curDir2, lwm - 0.02f, mockDiskChecker, mockLedgerDirsListener,
                false);
        assertEquals("Both the LedgerDirs should be writable", 2, dirsManager.getWritableLedgerDirs().size());

        // bring both the dirs above lwm but < threshold
        // should still be readwrite
        setUsageAndThenVerify(curDir1, lwm + 0.02f, curDir2, lwm + 0.08f, mockDiskChecker, mockLedgerDirsListener,
                false);
    }

    @Test
    public void testLedgerDirsMonitorStartReadOnly() throws Exception {
        ledgerMonitor.shutdown();

        final float nospace = 0.90f;
        final float lwm = 0.80f;

        File tmpDir1 = createTempDir("bkTest", ".dir");
        File curDir1 = BookieImpl.getCurrentDirectory(tmpDir1);
        BookieImpl.checkDirectoryStructure(curDir1);

        File tmpDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = BookieImpl.getCurrentDirectory(tmpDir2);
        BookieImpl.checkDirectoryStructure(curDir2);

        conf.setDiskUsageThreshold(nospace);
        conf.setDiskLowWaterMarkUsageThreshold(lwm);
        conf.setDiskUsageWarnThreshold(nospace);
        conf.setLedgerDirNames(new String[] { tmpDir1.toString(), tmpDir2.toString() });

        // Both disks are out of space at the start.
        HashMap<File, Float> usageMap = new HashMap<>();
        usageMap.put(curDir1, nospace + 0.05f);
        usageMap.put(curDir2, nospace + 0.05f);

        mockDiskChecker = new MockDiskChecker(nospace, warnThreshold);
        mockDiskChecker.setUsageMap(usageMap);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()),
                statsLogger);

        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, Collections.singletonList(dirsManager));
        try {
            ledgerMonitor.init();
            fail("NoWritableLedgerDirException expected");
        } catch (NoWritableLedgerDirException exception) {
            // ok
        }
        final MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        Thread.sleep((diskCheckInterval * 2) + 100);
        verifyUsage(curDir1, nospace + 0.05f, curDir2, nospace + 0.05f, mockLedgerDirsListener, true);
    }

    @Test
    public void testValidateLwmThreshold() {
        final ServerConfiguration configuration = TestBKConfiguration.newServerConfiguration();
        // check failed because diskSpaceThreshold < diskSpaceLwmThreshold
        configuration.setDiskUsageThreshold(0.65f);
        configuration.setDiskLowWaterMarkUsageThreshold(0.90f);
        try {
            new LedgerDirsMonitor(configuration, mockDiskChecker, Collections.singletonList(dirsManager));
            fail("diskSpaceThreshold < diskSpaceLwmThreshold, should be failed.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("diskSpaceThreshold >= diskSpaceLwmThreshold"));
        }

        // check failed because diskSpaceThreshold = 0 and diskUsageLwmThreshold = 1
        configuration.setDiskUsageThreshold(0f);
        configuration.setDiskLowWaterMarkUsageThreshold(1f);
        try {
            new LedgerDirsMonitor(configuration, mockDiskChecker, Collections.singletonList(dirsManager));
            fail("diskSpaceThreshold = 0 and diskUsageLwmThreshold = 1, should be failed.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Should be > 0 and < 1"));
        }

        // check succeeded
        configuration.setDiskUsageThreshold(0.95f);
        configuration.setDiskLowWaterMarkUsageThreshold(0.90f);
        new LedgerDirsMonitor(configuration, mockDiskChecker, Collections.singletonList(dirsManager));
    }

    private void setUsageAndThenVerify(File dir1, float dir1Usage, File dir2, float dir2Usage,
            MockDiskChecker mockDiskChecker, MockLedgerDirsListener mockLedgerDirsListener, boolean verifyReadOnly)
            throws InterruptedException {
        HashMap<File, Float> usageMap = new HashMap<File, Float>();
        usageMap.put(dir1, dir1Usage);
        usageMap.put(dir2, dir2Usage);
        mockDiskChecker.setUsageMap(usageMap);
        verifyUsage(dir1, dir1Usage, dir2, dir2Usage, mockLedgerDirsListener, verifyReadOnly);
    }

    private void verifyUsage(File dir1, float dir1Usage, File dir2, float dir2Usage,
                             MockLedgerDirsListener mockLedgerDirsListener, boolean verifyReadOnly) {
        executorController.advance(Duration.ofMillis(diskCheckInterval));

        float sample1 = getGauge(dir1.getParent()).getSample().floatValue();
        float sample2 = getGauge(dir2.getParent()).getSample().floatValue();

        assertEquals(mockLedgerDirsListener.readOnly, verifyReadOnly);
        assertThat(sample1, equalTo(dir1Usage * 100f));
        assertThat(sample2, equalTo(dir2Usage * 100f));
    }

    private Gauge<? extends Number> getGauge(String path) {
        String gaugeName = String.format("test.dir_%s_usage", path.replace('/', '_'));
        return statsProvider.getGauge(gaugeName);
    }

    private class MockDiskChecker extends DiskChecker {

        private volatile float used;
        private volatile Map<File, Float> usageMap = null;

        public MockDiskChecker(float threshold, float warnThreshold) {
            super(threshold, warnThreshold);
            used = 0f;
        }

        @Override
        public float checkDir(File dir) throws DiskErrorException, DiskOutOfSpaceException, DiskWarnThresholdException {
            float dirUsage = getDirUsage(dir);

            if (dirUsage > getDiskUsageThreshold()) {
                throw new DiskOutOfSpaceException("", dirUsage);
            }
            if (dirUsage > getDiskUsageWarnThreshold()) {
                throw new DiskWarnThresholdException("", dirUsage);
            }
            return dirUsage;
        }

        @Override
        public float getTotalDiskUsage(List<File> dirs) {
            float accumulatedDiskUsage = 0f;
            for (File dir : dirs) {
                accumulatedDiskUsage += getDirUsage(dir);
            }
            return (accumulatedDiskUsage / dirs.size());
        }

        public float getDirUsage(File dir) {
            float dirUsage;
            if ((usageMap == null) || (!usageMap.containsKey(dir))) {
                dirUsage = used;
            } else {
                dirUsage = usageMap.get(dir);
            }
            return dirUsage;
        }

        public void setUsage(float usage) {
            this.used = usage;
        }

        public void setUsageMap(Map<File, Float> usageMap) {
            this.usageMap = usageMap;
        }
    }

    private class MockLedgerDirsListener implements LedgerDirsListener {

        public volatile boolean highPriorityWritesAllowed;
        public volatile boolean readOnly;

        public MockLedgerDirsListener() {
            reset();
        }

        @Override
        public void diskWritable(File disk) {
            if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
                return;
            }
            readOnly = false;
            highPriorityWritesAllowed = true;
        }

        @Override
        public void diskJustWritable(File disk) {
            if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
                return;
            }
            readOnly = false;
            highPriorityWritesAllowed = true;
        }

        @Override
        public void allDisksFull(boolean highPriorityWritesAllowed) {
            this.readOnly = true;
            this.highPriorityWritesAllowed = highPriorityWritesAllowed;
        }

        @Override
        public void anyDiskFull(boolean highPriorityWritesAllowed) {
            if (conf.isReadOnlyModeOnAnyDiskFullEnabled()) {
                this.readOnly = true;
                this.highPriorityWritesAllowed = highPriorityWritesAllowed;
            }
        }

        @Override
        public void allDisksWritable() {
            this.readOnly = false;
            this.highPriorityWritesAllowed = true;
        }

        public void reset() {
            readOnly = false;
            highPriorityWritesAllowed = true;
        }

    }
}
