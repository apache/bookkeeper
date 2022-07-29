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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.complete;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test the bookie journal.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BookieImpl.class})
@PowerMockIgnore({"jdk.internal.loader.*", "javax.naming.*"})
@Slf4j
public class BookieWriteToJournalTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    class NoOpJournalReplayBookie extends TestBookieImpl {

        public NoOpJournalReplayBookie(ServerConfiguration conf)
                throws Exception {
            super(conf);
        }

        @Override
        void readJournal() throws IOException, BookieException {
            // Should be no-op since journal objects are mocked
        }
    }

    /**
     * test that Bookie calls correctly Journal.logAddEntry about "ackBeforeSync" parameter.
     */

    @Test
    public void testJournalLogAddEntryCalledCorrectly() throws Exception {

        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[]{ledgerDir.getPath()})
                .setMetadataServiceUri(null);

        BookieId bookieAddress = BookieImpl.getBookieId(conf);
        CountDownLatch journalJoinLatch = new CountDownLatch(1);
        Journal journal = mock(Journal.class);
        MutableBoolean effectiveAckBeforeSync = new MutableBoolean(false);
        doAnswer((Answer) (InvocationOnMock iom) -> {
            ByteBuf entry = iom.getArgument(0);
            long ledgerId = entry.getLong(entry.readerIndex() + 0);
            long entryId = entry.getLong(entry.readerIndex() + 8);
            boolean ackBeforeSync = iom.getArgument(1);
            WriteCallback callback = iom.getArgument(2);
            Object ctx = iom.getArgument(3);

            effectiveAckBeforeSync.setValue(ackBeforeSync);
            callback.writeComplete(BKException.Code.OK, ledgerId, entryId, bookieAddress, ctx);
            return null;
        }).when(journal).logAddEntry(any(ByteBuf.class), anyBoolean(), any(WriteCallback.class), any());

        // bookie will continue to work as soon as the journal thread is alive
        doAnswer((Answer) (InvocationOnMock iom) -> {
            journalJoinLatch.await();
            return null;
        }).when(journal).joinThread();

        whenNew(Journal.class).withAnyArguments().thenReturn(journal);

        Bookie b = new NoOpJournalReplayBookie(conf);
        b.start();

        long ledgerId = 1;
        long entryId = 0;
        Object expectedCtx = "foo";
        byte[] masterKey = new byte[64];
        for (boolean ackBeforeSync : new boolean[]{true, false}) {
            CountDownLatch latch = new CountDownLatch(1);
            final ByteBuf data = buildEntry(ledgerId, entryId, -1);
            final long expectedEntryId = entryId;
            b.addEntry(data, ackBeforeSync, (int rc, long ledgerId1, long entryId1,
                    BookieId addr, Object ctx) -> {
                assertSame(expectedCtx, ctx);
                assertEquals(ledgerId, ledgerId1);
                assertEquals(expectedEntryId, entryId1);
                latch.countDown();
            }, expectedCtx, masterKey);
            latch.await(30, TimeUnit.SECONDS);
            assertEquals(ackBeforeSync, effectiveAckBeforeSync.booleanValue());
            entryId++;
        }
        // let bookie exit main thread
        journalJoinLatch.countDown();
        b.shutdown();
    }

    /**
     * test that Bookie calls correctly Journal.forceLedger and is able to return the correct LastAddPersisted entry id.
     */
    @Test
    public void testForceLedger() throws Exception {

        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
                .setLedgerDirNames(new String[]{ledgerDir.getPath()});

        Bookie b = new TestBookieImpl(conf);
        b.start();

        long ledgerId = 1;
        long entryId = 0;
        Object expectedCtx = "foo";
        byte[] masterKey = new byte[64];

        CompletableFuture<Long> latchForceLedger1 = new CompletableFuture<>();
        CompletableFuture<Long> latchForceLedger2 = new CompletableFuture<>();
        CompletableFuture<Long> latchAddEntry = new CompletableFuture<>();
        final ByteBuf data = buildEntry(ledgerId, entryId, -1);
        final long expectedEntryId = entryId;
        b.forceLedger(ledgerId, (int rc, long ledgerId1, long entryId1,
                BookieId addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger1.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            complete(latchForceLedger1, null);
        }, expectedCtx);
        result(latchForceLedger1);

        b.addEntry(data, true /* ackBeforesync */, (int rc, long ledgerId1, long entryId1,
                        BookieId addr, Object ctx) -> {
                    if (rc != BKException.Code.OK) {
                        latchAddEntry.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                        return;
                    }
                    latchAddEntry.complete(entryId);
                }, expectedCtx, masterKey);
        assertEquals(expectedEntryId, result(latchAddEntry).longValue());

        // issue a new "forceLedger"
        b.forceLedger(ledgerId, (int rc, long ledgerId1, long entryId1,
                BookieId addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger2.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            complete(latchForceLedger2, null);
        }, expectedCtx);
        result(latchForceLedger2);

        b.shutdown();
    }

    @Test
    public void testSmallJournalQueueWithHighFlushFrequency() throws IOException, InterruptedException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalQueueSize(1);
        conf.setJournalFlushWhenQueueEmpty(true);
        conf.setJournalBufferedWritesThreshold(1);

        conf.setJournalDirName(tempDir.newFolder().getPath());
        conf.setLedgerDirNames(new String[]{tempDir.newFolder().getPath()});
        DiskChecker diskChecker = new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold());
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), diskChecker);
        Journal journal = new Journal(0, conf.getJournalDirs()[0], conf, ledgerDirsManager);
        journal.start();

        final int entries = 1000;
        CountDownLatch entriesLatch = new CountDownLatch(entries);
        for (int j = 1; j <= entries; j++) {
            ByteBuf entry = buildEntry(1, j, -1);
            journal.logAddEntry(entry, false, (int rc, long ledgerId, long entryId, BookieId addr, Object ctx) -> {
                entriesLatch.countDown();
            }, null);
        }
        entriesLatch.await();

        journal.shutdown();
    }

    private static ByteBuf buildEntry(long ledgerId, long entryId, long lastAddConfirmed) {
        final ByteBuf data = Unpooled.buffer();
        data.writeLong(ledgerId);
        data.writeLong(entryId);
        data.writeLong(lastAddConfirmed);
        return data;
    }
}
