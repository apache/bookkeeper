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
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for BookieShell convert-to-db-storage command.
 */
public class ConversionTest {

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
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                                      NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        interleavedStorage.setCheckpointSource(checkpointSource);
        interleavedStorage.setCheckpointer(checkpointer);

        // Insert some ledger & entries in the interleaved storage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            interleavedStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            interleavedStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                interleavedStorage.addEntry(entry);
            }
        }

        interleavedStorage.flush();
        interleavedStorage.shutdown();

        // Run conversion tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "convert-to-db-storage" });

        Assert.assertEquals(0, res);

        // Verify that db index has the same entries
        DbLedgerStorage dbStorage = new DbLedgerStorage();
        dbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager,
                             NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
        dbStorage.setCheckpointer(checkpointer);
        dbStorage.setCheckpointSource(checkpointSource);

        interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager,
                ledgerDirsManager, NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT);
        interleavedStorage.setCheckpointSource(checkpointSource);
        interleavedStorage.setCheckpointer(checkpointer);

        Set<Long> ledgers = Sets.newTreeSet(dbStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0L, 1L, 2L, 3L, 4L)), ledgers);

        ledgers = Sets.newTreeSet(interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, dbStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(dbStorage.readMasterKey(ledgerId)));

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = dbStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
                result.release();

                try {
                    interleavedStorage.getEntry(ledgerId, entryId);
                    Assert.fail("entry should not exist");
                } catch (NoLedgerException e) {
                    // Ok
                }
            }
        }

        interleavedStorage.shutdown();
        dbStorage.shutdown();
        FileUtils.forceDelete(tmpDir);
    }
}
