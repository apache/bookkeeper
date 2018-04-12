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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Test the bookie journal.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Bookie.class})
@Slf4j
public class BookieWriteToJournalTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    /**
     * test that Bookie calls correctly Journal.logAddEntry about "ackBeforeSync" parameter.
     */
    @Test
    public void testJournalLogAddEntryCalledCorrectly() throws Exception {

        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(ledgerDir));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[]{ledgerDir.getPath()})
            .setMetadataServiceUri(null);
        BookieSocketAddress bookieAddress = Bookie.getBookieAddress(conf);
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

        Bookie b = new Bookie(conf);
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
                                             BookieSocketAddress addr, Object ctx) -> {
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
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(ledgerDir));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[]{ledgerDir.getPath()})
            .setZkServers(null);

        Bookie b = new Bookie(conf);
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
        b.forceLedger(ledgerId, expectedEntryId, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger1.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger1.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(BookieProtocol.INVALID_ENTRY_ID, result(latchForceLedger1).longValue());

        b.addEntry(data, true /* ackBeforesync */, (int rc, long ledgerId1, long entryId1,
                                         BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchAddEntry.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchAddEntry.complete(entryId);
        }, expectedCtx, masterKey);
        assertEquals(expectedEntryId, result(latchAddEntry).longValue());

        // issue a new "forceLedger", it should return the last persisted entry id
        b.forceLedger(ledgerId, expectedEntryId, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger2.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger2.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(entryId, result(latchForceLedger2).longValue());

        b.shutdown();
    }

    /**
     * test that Bookie is able to return the recover LastAddPersisted entry id from LedgerStorage after a restart.
     */
    @Test
    public void testForceLedgerRecoverFromLedgerStorage() throws Exception {

        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(ledgerDir));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[]{ledgerDir.getPath()})
            .setZkServers(null);

        Bookie b = new Bookie(conf);
        b.start();

        long ledgerId = 1;
        long entryId = 0;
        Object expectedCtx = "foo";
        byte[] masterKey = new byte[64];

        CompletableFuture<Long> latchForceLedger = new CompletableFuture<>();
        CompletableFuture<Long> latchAddEntry = new CompletableFuture<>();
        final ByteBuf data = buildEntry(ledgerId, entryId, -1);
        final long expectedEntryId = entryId;

        b.addEntry(data, true /* ackBeforesync */, (int rc, long ledgerId1, long entryId1,
                                         BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchAddEntry.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchAddEntry.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(expectedEntryId, result(latchAddEntry).longValue());

        // issue a new "forceLedger", it should return the last persisted entry id
        b.forceLedger(ledgerId, expectedEntryId, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(entryId, result(latchForceLedger).longValue());

        b.shutdown();

        // re-start the bookie
        Bookie b2 = new Bookie(conf);
        b2.start();

        AtomicInteger recoverReadsCount = new AtomicInteger();
        LedgerStorage ledgerStorage = b2.getLedgerStorage();
        LedgerStorage ledgerStorageSpy = spy(ledgerStorage);
        Whitebox.setInternalState(b2, "ledgerStorage", ledgerStorageSpy);
        doAnswer((InvocationOnMock iom) -> {
                    long lId = (Long) iom.getArgument(0);
                    long eId = (Long) iom.getArgument(1);
                    recoverReadsCount.incrementAndGet();
                    return ledgerStorage.getEntry(lId, eId);
        }).when(ledgerStorageSpy).getEntry(eq(ledgerId), anyLong());

        // issue a new "forceLedger", lastAddPersisted will be recovered
        // by reading the LastAddConfirmed entry from LedgerStorage
        CompletableFuture<Long> latchForceLedger2 = new CompletableFuture<>();
        b2.forceLedger(ledgerId, expectedEntryId, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger2.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger2.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(entryId, result(latchForceLedger2).longValue());

//        assertEquals(1, getLastAddConfirmedEntryCount.get());
        assertEquals(1, recoverReadsCount.get());

        // issue a new "forceLedger", there is no need to recover again
        // from LedgerStorage
        CompletableFuture<Long> latchForceLedger3 = new CompletableFuture<>();
        b2.forceLedger(ledgerId, expectedEntryId, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger3.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger3.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(entryId, result(latchForceLedger3).longValue());

        assertEquals(1, recoverReadsCount.get());
        b2.shutdown();

    }

    /**
     * test that Bookie is able to return the recover LastAddPersisted entry id from LedgerStorage after a restart
     * the sequence of entries contains gaps as it has been written by a {@link WriteAdvHandle}.
     */
    @Test
    public void testForceLedgerRecoverFromLedgerStorageWithGaps() throws Exception {

        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));
        File ledgerDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(ledgerDir));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[]{ledgerDir.getPath()})
            .setZkServers(null);

        long ledgerId = 1;
        Object expectedCtx = "foo";
        byte[] masterKey = new byte[64];
        final long lastEntryBeforeGap = 2;
        final long lastEntryInLedger = 5;
        List<ByteBuf> entries = new ArrayList<>();
        entries.add(buildEntry(ledgerId, 0, -1));
        entries.add(buildEntry(ledgerId, 1, -1));
        entries.add(buildEntry(ledgerId, lastEntryBeforeGap, -1));
        ByteBuf gapEntry = buildEntry(ledgerId, 3, -1);
        entries.add(buildEntry(ledgerId, 4, -1));
        entries.add(buildEntry(ledgerId, lastEntryInLedger, -1));


        Bookie b = new Bookie(conf);
        b.start();

        for (ByteBuf data : entries) {
            final long expectedEntryId = data.getLong(8);
            CompletableFuture<Long> latchAddEntry = new CompletableFuture<>();
            b.addEntry(data, true /* ackBeforesync */, (int rc, long ledgerId1, long entryId1,
                            BookieSocketAddress addr, Object ctx) -> {
                if (rc != BKException.Code.OK) {
                    latchAddEntry.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                    return;
                }
                latchAddEntry.complete(entryId1);
            }, expectedCtx, masterKey);
            assertEquals(expectedEntryId, result(latchAddEntry).longValue());
        }

        // issue a new "forceLedger", it should return the last persisted entry id
        CompletableFuture<Long> latchForceLedger = new CompletableFuture<>();
        b.forceLedger(ledgerId, lastEntryInLedger, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(lastEntryBeforeGap, result(latchForceLedger).longValue());

        b.shutdown();


        // re-start the bookie
        Bookie b2 = new Bookie(conf);
        b2.start();

        AtomicInteger recoverReadsCount = new AtomicInteger();
        LedgerStorage ledgerStorage = b2.getLedgerStorage();
        LedgerStorage ledgerStorageSpy = spy(ledgerStorage);
        Whitebox.setInternalState(b2, "ledgerStorage", ledgerStorageSpy);
        doAnswer((InvocationOnMock iom) -> {
                    long lId = (Long) iom.getArgument(0);
                    long eId = (Long) iom.getArgument(1);
                    recoverReadsCount.incrementAndGet();
                    return ledgerStorage.getEntry(lId, eId);
        }).when(ledgerStorageSpy).getEntry(eq(ledgerId), anyLong());

        // issue a new "forceLedger", lastAddPersisted will be recovered
        // by reading the LastAddConfirmed entry from LedgerStorage
        CompletableFuture<Long> latchForceLedger2 = new CompletableFuture<>();
        b2.forceLedger(ledgerId, lastEntryInLedger, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger2.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger2.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(lastEntryBeforeGap, result(latchForceLedger2).longValue());

        // we had to scan from 0 to 5 (lastEntryInLedger)
        assertEquals(6, recoverReadsCount.get());
        recoverReadsCount.set(0);

        // issue a new "forceLedger", there is no need to recover again
        // from LedgerStorage
        CompletableFuture<Long> latchForceLedger3 = new CompletableFuture<>();
        b2.forceLedger(ledgerId, lastEntryInLedger, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger3.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger3.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(lastEntryBeforeGap, result(latchForceLedger3).longValue());

        assertEquals(0, recoverReadsCount.get());

        // fill the gap
        final long expectedEntryId = gapEntry.getLong(8);
        CompletableFuture<Long> latchAddEntry = new CompletableFuture<>();
        b2.addEntry(gapEntry, true /* ackBeforesync */, (int rc, long ledgerId1, long entryId1,
                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchAddEntry.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchAddEntry.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(expectedEntryId, result(latchAddEntry).longValue());

        // issue a new "forceLedger", there is no need to recover again
        // from LedgerStorage and now the gap is filled, so we will see lastAddForced  = lastEntryInLedger
        CompletableFuture<Long> latchForceLedger4 = new CompletableFuture<>();
        b2.forceLedger(ledgerId, lastEntryInLedger, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger4.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger4.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(lastEntryInLedger, result(latchForceLedger4).longValue());

        assertEquals(0, recoverReadsCount.get());

        b2.shutdown();


        // re-start the bookie
        Bookie b3 = new Bookie(conf);
        b3.start();

        AtomicInteger recoverReadsCount3 = new AtomicInteger();
        LedgerStorage ledgerStorage3 = b3.getLedgerStorage();
        LedgerStorage ledgerStorageSpy3 = spy(ledgerStorage3);
        Whitebox.setInternalState(b3, "ledgerStorage", ledgerStorageSpy3);
        doAnswer((InvocationOnMock iom) -> {
                    long lId = (Long) iom.getArgument(0);
                    long eId = (Long) iom.getArgument(1);
                    recoverReadsCount3.incrementAndGet();
                    return ledgerStorage3.getEntry(lId, eId);
        }).when(ledgerStorageSpy3).getEntry(eq(ledgerId), anyLong());

        // issue a new "forceLedger", lastAddPersisted will be recovered
        // by reading the LastAddConfirmed entry from LedgerStorage
        CompletableFuture<Long> latchForceLedger5 = new CompletableFuture<>();
        b3.forceLedger(ledgerId, lastEntryInLedger, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger5.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger5.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(lastEntryInLedger, result(latchForceLedger5).longValue());

        // we had to scan from 0 to 5 (lastEntryInLedger)
        assertEquals(6, recoverReadsCount3.get());
        recoverReadsCount3.set(0);

        // issue a new "forceLedger", there is no need to recover again
        // from LedgerStorage
        CompletableFuture<Long> latchForceLedger6 = new CompletableFuture<>();
        b3.forceLedger(ledgerId, lastEntryInLedger, (int rc, long ledgerId1, long entryId1,
                                        BookieSocketAddress addr, Object ctx) -> {
            if (rc != BKException.Code.OK) {
                latchForceLedger6.completeExceptionally(org.apache.bookkeeper.client.BKException.create(rc));
                return;
            }
            latchForceLedger6.complete(entryId1);
        }, expectedCtx, masterKey);
        assertEquals(lastEntryInLedger, result(latchForceLedger6).longValue());

        assertEquals(0, recoverReadsCount3.get());

        b3.shutdown();

    }

    private static ByteBuf buildEntry(long ledgerId, long entryId, long lastAddConfirmed) {
        final ByteBuf data = Unpooled.buffer();
        data.writeLong(ledgerId);
        data.writeLong(entryId);
        data.writeLong(lastAddConfirmed);
        return data;
    }
}
