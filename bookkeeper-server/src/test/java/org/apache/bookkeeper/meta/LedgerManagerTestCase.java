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

package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.SnapshotMap;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case to run over serveral ledger managers
 */
@RunWith(Parameterized.class)
public abstract class LedgerManagerTestCase extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(LedgerManagerTestCase.class);

    protected LedgerManagerFactory ledgerManagerFactory;
    protected LedgerManager ledgerManager = null;
    protected LedgerIdGenerator ledgerIdGenerator = null;
    protected SnapshotMap<Long, Boolean> activeLedgers = null;

    public LedgerManagerTestCase(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        this(lmFactoryCls, 0);
    }

    public LedgerManagerTestCase(Class<? extends LedgerManagerFactory> lmFactoryCls, int numBookies) {
        super(numBookies);
        activeLedgers = new SnapshotMap<Long, Boolean>();
        baseConf.setLedgerManagerFactoryClass(lmFactoryCls);
    }

    public LedgerManager getLedgerManager() {
        if (null == ledgerManager) {
            ledgerManager = ledgerManagerFactory.newLedgerManager();
        }
        return ledgerManager;
    }

    public LedgerIdGenerator getLedgerIdGenerator() throws IOException {
        if (null == ledgerIdGenerator) {
            ledgerIdGenerator = ledgerManagerFactory.newLedgerIdGenerator();
        }
        return ledgerIdGenerator;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { FlatLedgerManagerFactory.class },
            { HierarchicalLedgerManagerFactory.class },
            { LongHierarchicalLedgerManagerFactory.class },
            { MSLedgerManagerFactory.class },
        });
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(baseConf, zkc);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != ledgerManager) {
            ledgerManager.close();
        }
        ledgerManagerFactory.uninitialize();
        super.tearDown();
    }

    public class MockLedgerStorage implements CompactableLedgerStorage {

        @Override
        public void initialize(ServerConfiguration conf,
                               GarbageCollectorThread.LedgerManagerProvider ledgerManagerProvider,
                               LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
                               CheckpointSource checkpointSource, StatsLogger statsLogger) throws IOException {
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() throws InterruptedException {
        }

        @Override
        public boolean ledgerExists(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public boolean setFenced(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public boolean isFenced(long ledgerId) throws IOException {
            return false;
        }

        @Override
        public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        }

        @Override
        public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
            return null;
        }

        @Override
        public long addEntry(ByteBuffer entry) throws IOException {
            return 0;
        }

        @Override
        public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
            return null;
        }

        @Override
        public long getLastAddConfirmed(long ledgerId) throws IOException {
            return 0;
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public Checkpoint checkpoint(Checkpoint checkpoint) throws IOException {
            return null;
        }

        @Override
        public void deleteLedger(long ledgerId) throws IOException {
            activeLedgers.remove(ledgerId);
        }

        @Override
        public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) {
            NavigableMap<Long, Boolean> bkActiveLedgersSnapshot = activeLedgers.snapshot();
            Map<Long, Boolean> subBkActiveLedgers = bkActiveLedgersSnapshot
                    .subMap(firstLedgerId, true, lastLedgerId, false);

            return subBkActiveLedgers.keySet();
        }

        @Override
        public BKMBeanInfo getJMXBean() {
            return null;
        }

        @Override
        public EntryLogger getEntryLogger() {
            return null;
        }

        @Override
        public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        }

        @Override
        public void flushEntriesLocationsIndex() throws IOException {
        }

        @Override
        public void setExplicitlac(long ledgerId, ByteBuffer lac) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public ByteBuffer getExplicitLac(long ledgerId) {
            // TODO Auto-generated method stub
            return null;
        }
    }
}
