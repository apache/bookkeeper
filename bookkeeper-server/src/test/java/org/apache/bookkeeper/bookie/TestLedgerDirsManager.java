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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLedgerDirsManager {
    private final static Logger LOG = LoggerFactory.getLogger(TestLedgerDirsManager.class);

    ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
    File curDir;
    LedgerDirsManager dirsManager;
    MockDiskChecker mockDiskChecker;
    int diskCheckInterval = 1000;
    float threshold = 0.5f;
    float warnThreshold = 0.5f;

    final List<File> tempDirs = new ArrayList<File>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @Before
    public void setUp() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setDiskCheckInterval(diskCheckInterval);

        mockDiskChecker = new MockDiskChecker(threshold, warnThreshold);
        dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), NullStatsLogger.INSTANCE, mockDiskChecker);
        dirsManager.init();
    }

    @After
    public void tearDown() throws Exception {
        dirsManager.shutdown();
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    @Test(timeout=60000)
    public void testPickWritableDirExclusive() throws Exception {
        try {
            dirsManager.pickRandomWritableDir(curDir);
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            assertTrue(true);
        }
    }

    @Test(timeout=60000)
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

    @Test(timeout=60000)
    public void testLedgerDirsMonitorDuringTransition() throws Exception {

        MockLedgerDirsListener mockLedgerDirsListener = new MockLedgerDirsListener();
        dirsManager.addLedgerDirsListener(mockLedgerDirsListener);
        dirsManager.start();

        assertFalse(mockLedgerDirsListener.readOnly);
        mockDiskChecker.setUsage(threshold + 0.05f);

        Thread.sleep((diskCheckInterval * 2) + 100);

        assertTrue(mockLedgerDirsListener.readOnly);
        mockDiskChecker.setUsage(threshold - 0.05f);

        Thread.sleep(diskCheckInterval + 100);

        assertFalse(mockLedgerDirsListener.readOnly);
    }

    private class MockDiskChecker extends DiskChecker {

        private float used;

        public MockDiskChecker(float threshold, float warnThreshold) {
            super(threshold, warnThreshold);
            used = 0f;
        }

        @Override
        public float checkDir(File dir) throws DiskErrorException, DiskOutOfSpaceException, DiskWarnThresholdException {
            if (used > getDiskUsageThreshold()) {
                throw new DiskOutOfSpaceException("", used);
            }
            if (used > getDiskUsageWarnThreshold()) {
                throw new DiskWarnThresholdException("", used);
            }
            return used;
        }

        public void setUsage(float usage) {
            this.used = usage;
        }
    }

    private class MockLedgerDirsListener implements LedgerDirsListener {

        public boolean readOnly;

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
