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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for class {@link LocationsIndexRebuildOp}.
 */
public class LocationsIndexRebuildTest {

    CheckpointSource checkpointSource = new CheckpointSource() {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        }
    };

    Checkpointer checkpointer = new Checkpointer() {
        @Override
        public void startCheckpoint(Checkpoint checkpoint) {
            // No-op
        }

        @Override
        public void start() {
            // no-op
        }
    };

    protected final TmpDirs tmpDirs = new TmpDirs();
    private String newDirectory() throws Exception {
        File d = tmpDirs.createNew("bkTest", ".dir");
        d.delete();
        d.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(d);
        BookieImpl.checkDirectoryStructure(curDir);
        return d.getPath();
    }

    @Test
    public void test() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        System.out.println(tmpDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        DbLedgerStorage ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                                 NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        ledgerStorage.setCheckpointer(checkpointer);
        ledgerStorage.setCheckpointSource(checkpointSource);

        // Insert some ledger & entries in the storage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            ledgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            ledgerStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 100; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ledgerStorage.addEntry(entry);
            }
        }

        ledgerStorage.flush();
        ledgerStorage.shutdown();

        // Rebuild index through the tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "rebuild-db-ledger-locations-index" });

        Assert.assertEquals(0, res);

        // Verify that db index has the same entries
        ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                                 NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        ledgerStorage.setCheckpointSource(checkpointSource);
        ledgerStorage.setCheckpointer(checkpointer);

        Set<Long> ledgers = Sets.newTreeSet(ledgerStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0L, 1L, 2L, 3L, 4L)), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, ledgerStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(ledgerStorage.readMasterKey(ledgerId)));

            ByteBuf lastEntry = ledgerStorage.getLastEntry(ledgerId);
            assertEquals(ledgerId, lastEntry.readLong());
            long lastEntryId = lastEntry.readLong();
            assertEquals(99, lastEntryId);

            for (long entryId = 0; entryId < 100; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = ledgerStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
            }
        }

        ledgerStorage.shutdown();
        FileUtils.forceDelete(tmpDir);
    }

    @Test
    public void testMultiLedgerIndexDiffDirs() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { newDirectory(), newDirectory() });
        conf.setIndexDirName(new String[] { newDirectory(), newDirectory() });
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker);
        LedgerDirsManager indexDirsManager = new LedgerDirsManager(conf, conf.getIndexDirs(), diskChecker);

        DbLedgerStorage ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, indexDirsManager,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        ledgerStorage.setCheckpointer(checkpointer);
        ledgerStorage.setCheckpointSource(checkpointSource);

        // Insert some ledger & entries in the storage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            ledgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            ledgerStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 100; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ledgerStorage.addEntry(entry);
            }
        }

        ledgerStorage.flush();
        ledgerStorage.shutdown();

        // Rebuild index through the tool
        new LocationsIndexRebuildOp(conf).initiate();

        // Verify that db index has the same entries
        ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, indexDirsManager,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        ledgerStorage.setCheckpointSource(checkpointSource);
        ledgerStorage.setCheckpointer(checkpointer);

        Set<Long> ledgers = Sets.newTreeSet(ledgerStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0L, 1L, 2L, 3L, 4L)), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, ledgerStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(ledgerStorage.readMasterKey(ledgerId)));

            ByteBuf lastEntry = ledgerStorage.getLastEntry(ledgerId);
            assertEquals(ledgerId, lastEntry.readLong());
            long lastEntryId = lastEntry.readLong();
            assertEquals(99, lastEntryId);

            for (long entryId = 0; entryId < 100; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = ledgerStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
            }
        }

        ledgerStorage.shutdown();
        List<String> toDeleted = Lists.newArrayList(conf.getLedgerDirNames());
        toDeleted.addAll(Lists.newArrayList(conf.getIndexDirNames()));
        toDeleted.forEach(d -> {
            try {
                FileUtils.forceDelete(new File(d));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
