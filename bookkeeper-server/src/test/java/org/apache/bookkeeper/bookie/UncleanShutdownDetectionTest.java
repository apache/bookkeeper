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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test the unclean shutdown implementation.
 */
public class UncleanShutdownDetectionTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testRegisterStartWithoutRegisterShutdownEqualsUncleanShutdown() throws IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();

        assertTrue(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    public void testRegisterStartWithRegisterShutdownEqualsCleanShutdown() throws IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
        uncleanShutdownDetection.registerCleanShutdown();

        assertFalse(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    public void testRegisterStartWithoutRegisterShutdownEqualsUncleanShutdownMultipleDirs() throws IOException {
        File ledgerDir1 = tempDir.newFolder("l1");
        File ledgerDir2 = tempDir.newFolder("l2");
        File ledgerDir3 = tempDir.newFolder("l3");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerDirNames(new String[] {ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                        ledgerDir3.getAbsolutePath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();

        assertTrue(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    public void testRegisterStartWithRegisterShutdownEqualsCleanShutdownMultipleDirs() throws IOException {
        File ledgerDir1 = tempDir.newFolder("l1");
        File ledgerDir2 = tempDir.newFolder("l2");
        File ledgerDir3 = tempDir.newFolder("l3");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerDirNames(new String[] {ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                        ledgerDir3.getAbsolutePath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
        uncleanShutdownDetection.registerCleanShutdown();

        assertFalse(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test
    public void testRegisterStartWithPartialRegisterShutdownEqualsUncleanShutdownMultipleDirs() throws IOException {
        File ledgerDir1 = tempDir.newFolder("l1");
        File ledgerDir2 = tempDir.newFolder("l2");
        File ledgerDir3 = tempDir.newFolder("l3");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setLedgerDirNames(new String[] {ledgerDir1.getAbsolutePath(), ledgerDir2.getAbsolutePath(),
                        ledgerDir3.getAbsolutePath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
        uncleanShutdownDetection.registerCleanShutdown();
        File dirtyFile = new File(ledgerDirsManager.getAllLedgerDirs().get(0),
                UncleanShutdownDetectionImpl.DIRTY_FILENAME);
        dirtyFile.createNewFile();

        assertTrue(uncleanShutdownDetection.lastShutdownWasUnclean());
    }

    @Test(expected = IOException.class)
    public void testRegisterStartFailsToCreateDirtyFilesAndThrowsIOException() throws IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new MockLedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker);

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        uncleanShutdownDetection.registerStartUp();
    }

    private class MockLedgerDirsManager extends LedgerDirsManager {
        public MockLedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker)
                throws IOException {
            super(conf, dirs, diskChecker);
        }

        @Override
        public List<File> getAllLedgerDirs() {
            List<File> dirs = new ArrayList<>();
            dirs.add(new File("does_not_exist"));
            return dirs;
        }
    }
}
