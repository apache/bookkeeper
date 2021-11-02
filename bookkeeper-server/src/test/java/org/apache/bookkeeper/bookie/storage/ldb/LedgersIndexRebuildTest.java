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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test for class {@link LedgersIndexRebuildOp}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ LedgersIndexRebuildTest.class, MetadataDrivers.class })
public class LedgersIndexRebuildTest {

    private final BookieId bookieAddress = BookieId.parse(UUID.randomUUID().toString());
    private ServerConfiguration conf;
    private File tmpDir;

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

    @Before
    public void setUp() throws IOException {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = BookieImpl.getCurrentDirectory(tmpDir);
        BookieImpl.checkDirectoryStructure(curDir);

        System.out.println(tmpDir);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.forceDelete(tmpDir);
    }

    @Test
    public void testRebuildIncludesAllLedgersAndSetToFenced() throws Exception {
        byte[] masterKey = "12345".getBytes();
        long ledgerCount = 100;

        // no attempts to get ledger metadata fail
        DbLedgerStorage ledgerStorage = setupLedgerStorage();

        // Insert some ledger & entries in the storage
        for (long ledgerId = 0; ledgerId < ledgerCount; ledgerId++) {
            ledgerStorage.setMasterKey(ledgerId, masterKey);

            for (long entryId = 0; entryId < 2; entryId++) {
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
        int res = shell.run(new String[] { "rebuild-db-ledgers-index", "-v" });

        Assert.assertEquals(0, res);

        // Verify that the ledgers index has the ledgers and that they are fenced
        ledgerStorage = new DbLedgerStorage();
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));
        ledgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, null, checkpointSource, checkpointer,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

       for (long ledgerId = 0; ledgerId < ledgerCount; ledgerId++) {
            assertTrue(ledgerStorage.ledgerExists(ledgerId));
            assertTrue(ledgerStorage.isFenced(ledgerId));
        }

        ledgerStorage.shutdown();
    }

    private DbLedgerStorage setupLedgerStorage() throws Exception {
        conf = TestBKConfiguration.newServerConfiguration();
        conf.setBookieId(bookieAddress.getId());
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(),
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()));

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);

        PowerMockito.whenNew(BookieId.class).withParameterTypes(String.class).withArguments(anyString())
                .thenReturn(bookieAddress);

        DbLedgerStorage ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, null, checkpointSource, checkpointer,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        return ledgerStorage;
    }
}
