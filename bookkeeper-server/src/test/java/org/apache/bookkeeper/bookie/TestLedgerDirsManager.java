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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test LedgerDirsManager.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(LedgerDirsMonitor.class)
public class TestLedgerDirsManager {

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

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(Executors.class);

        File tmpDir = createTempDir("bkTest", ".dir");
        curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setDiskLowWaterMarkUsageThreshold(conf.getDiskUsageThreshold());
        conf.setDiskCheckInterval(diskCheckInterval);
        conf.setIsForceGCAllowWhenNoSpace(true);

        executor = PowerMockito.mock(ScheduledExecutorService.class);
        executorController = new MockExecutorController()
            .controlScheduleAtFixedRate(executor, 10);
        PowerMockito.when(Executors.newSingleThreadScheduledExecutor(any()))
            .thenReturn(executor);

        mockDiskChecker = new MockDiskChecker(threshold, warnThreshold);
        statsProvider = new TestStatsProvider();
        statsLogger = statsProvider.getStatsLogger("test");
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()), statsLogger);
        ledgerMonitor = new LedgerDirsMonitor(conf,
                mockDiskChecker, dirsManager);
        ledgerMonitor.init();
    }

    @After
    public void tearDown() throws Exception {
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
            writeDirs = dirsManager.getWritableLedgerDirs();
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
    public void testLedgerDirsMonitorDuringTransition() throws Exception {
        MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        ledgerMonitor.start();

        assertFalse(mockLedgerDirsListener.readOnly);
        mockDiskChecker.setUsage(threshold + 0.05f);

        executorController.advance(Duration.ofMillis(diskCheckInterval));
        assertTrue(mockLedgerDirsListener.readOnly);

        mockDiskChecker.setUsage(threshold - 0.05f);
        executorController.advance(Duration.ofMillis(diskCheckInterval));

        assertFalse(mockLedgerDirsListener.readOnly);
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
        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, dirsManager);
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
        File curDir1 = Bookie.getCurrentDirectory(tmpDir1);
        Bookie.checkDirectoryStructure(curDir1);

        File tmpDir2 = createTempDir("bkTest", ".dir");
        File curDir2 = Bookie.getCurrentDirectory(tmpDir2);
        Bookie.checkDirectoryStructure(curDir2);

        conf.setDiskUsageThreshold(nospace);
        conf.setDiskLowWaterMarkUsageThreshold(lwm);
        conf.setDiskUsageWarnThreshold(nospace);
        conf.setLedgerDirNames(new String[] { tmpDir1.toString(), tmpDir2.toString() });

        mockDiskChecker = new MockDiskChecker(nospace, warnThreshold);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()),
                statsLogger);
        ledgerMonitor = new LedgerDirsMonitor(conf, mockDiskChecker, dirsManager);
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
                mockLedgerDirsListener, false);

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
                false);
        assertTrue("Only one LedgerDir should be writable", dirsManager.getWritableLedgerDirs().size() == 1);

        // bring both the dirs below lwm
        // should still be readwrite
        setUsageAndThenVerify(curDir1, lwm - 0.03f, curDir2, lwm - 0.02f, mockDiskChecker, mockLedgerDirsListener,
                false);
        assertTrue("Both the LedgerDirs should be writable", dirsManager.getWritableLedgerDirs().size() == 2);

        // bring both the dirs above lwm but < threshold
        // should still be readwrite
        setUsageAndThenVerify(curDir1, lwm + 0.02f, curDir2, lwm + 0.08f, mockDiskChecker, mockLedgerDirsListener,
                false);
    }

    private void setUsageAndThenVerify(File dir1, float dir1Usage, File dir2, float dir2Usage,
            MockDiskChecker mockDiskChecker, MockLedgerDirsListener mockLedgerDirsListener, boolean verifyReadOnly)
            throws InterruptedException {
        HashMap<File, Float> usageMap = new HashMap<File, Float>();
        usageMap.put(dir1, dir1Usage);
        usageMap.put(dir2, dir2Usage);
        mockDiskChecker.setUsageMap(usageMap);
        executorController.advance(Duration.ofMillis(diskCheckInterval));

        float sample1 = getGauge(dir1.getParent()).getSample().floatValue();
        float sample2 = getGauge(dir2.getParent()).getSample().floatValue();

        if (verifyReadOnly) {
            assertTrue(mockLedgerDirsListener.readOnly);

            // LedgerDirsMonitor stops updating diskUsages when the bookie is in the readonly mode,
            // so the stats will reflect an older value at the time when the bookie became readonly
            assertThat(sample1, greaterThan(90f));
            assertThat(sample1, lessThan(100f));
            assertThat(sample2, greaterThan(90f));
            assertThat(sample2, lessThan(100f));
        } else {
            assertFalse(mockLedgerDirsListener.readOnly);

            assertThat(sample1, equalTo(dir1Usage * 100f));
            assertThat(sample2, equalTo(dir2Usage * 100f));
        }
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

        public volatile boolean readOnly;

        public MockLedgerDirsListener() {
            reset();
        }

        @Override
        public void diskFailed(File disk) {
        }

        @Override
        public void diskAlmostFull(File disk) {
        }

        @Override
        public void diskFull(File disk) {
        }

        @Override
        public void diskWritable(File disk) {
            readOnly = false;
        }

        @Override
        public void diskJustWritable(File disk) {
            readOnly = false;
        }

        @Override
        public void allDisksFull() {
            readOnly = true;
        }

        @Override
        public void fatalError() {
        }

        public void reset() {
            readOnly = false;
        }

    }
}
