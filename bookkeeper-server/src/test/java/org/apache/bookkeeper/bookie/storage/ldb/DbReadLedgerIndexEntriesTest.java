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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.bookkeeper.bookie.BookieImpl;
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
 * Test for class {@link DbLedgerStorage#readLedgerIndexEntries}.
 */
public class DbReadLedgerIndexEntriesTest {
    private static final int TEST_LEDGER_MIN_ID = 0;
    private static final int TEST_LEDGER_MAX_ID = 5;
    private static final int TEST_ENTRY_MIN_ID = 0;
    private static final int TEST_ENTRY_MAX_ID = 10;

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
    public void testReadLedgerIndexEntries() throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[]{newDirectory(), newDirectory()});
        conf.setIndexDirName(new String[]{newDirectory(), newDirectory()});
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
        for (long ledgerId = TEST_LEDGER_MIN_ID; ledgerId <= TEST_LEDGER_MAX_ID; ledgerId++) {
            ledgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            ledgerStorage.setFenced(ledgerId);

            for (long entryId = TEST_ENTRY_MIN_ID; entryId <= TEST_ENTRY_MAX_ID; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ledgerStorage.addEntry(entry);
            }
        }

        ledgerStorage.flush();
        ledgerStorage.shutdown();

        // read ledger index entries
        long ledgerId = TEST_LEDGER_MIN_ID;
        try {
            for (ledgerId = TEST_LEDGER_MIN_ID; ledgerId <= TEST_LEDGER_MAX_ID; ledgerId++) {
                BlockingQueue<Long> entrys = new ArrayBlockingQueue<>(TEST_ENTRY_MAX_ID + 1);
                DbLedgerStorage.readLedgerIndexEntries(ledgerId, conf, (eId, entryLodId, pos) -> {
                    System.out.println("entry " + eId + "\t:\t(log: " + entryLodId + ", pos: " + pos + ")");
                    entrys.add(eId);
                });
                for (long entryId = TEST_ENTRY_MIN_ID; entryId <= TEST_ENTRY_MAX_ID; entryId++) {
                    Assert.assertTrue(entrys.contains(entryId));
                }
            }
        } catch (Exception e) {
            System.err.printf("ERROR: initializing dbLedgerStorage %s", e.getMessage());
            Assert.fail("fail to read this ledger(" + ledgerId + ") index entries");
        }

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
