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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Journal.ForceWriteRequest;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;

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
@PrepareForTest({JournalChannel.class, Journal.class})
@Slf4j
public class BookieJournalForceTest {

    private static final ByteBuf DATA = Unpooled.wrappedBuffer(new byte[]{});

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void testAckAfterSync() throws Exception {
        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setZkServers(null);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(journalDir, conf, ledgerDirsManager);

        CountDownLatch forceWriteThreadSuspendedIsWaiting = new CountDownLatch(1);
        LinkedBlockingQueue<ForceWriteRequest> supportQueue = new LinkedBlockingQueue<>();
        BlockingQueue<ForceWriteRequest> forceWriteRequests = mock(BlockingQueue.class);
        AtomicBoolean forceWriteThreadSuspended = new AtomicBoolean(true);

        doAnswer((Answer) (InvocationOnMock iom) -> {
            supportQueue.put(iom.getArgument(0));
            return null;
        }).when(forceWriteRequests).put(any(ForceWriteRequest.class));

        when(forceWriteRequests.take()).thenAnswer((InvocationOnMock iom) -> {
            forceWriteThreadSuspendedIsWaiting.countDown();
            while (forceWriteThreadSuspended.get()) {
                Thread.sleep(100);
            }
            ForceWriteRequest res = supportQueue.take();
            while (forceWriteThreadSuspended.get()) {
                Thread.sleep(100);
            }
            return res;
        });
        Whitebox.setInternalState(journal, "forceWriteRequests", forceWriteRequests);
        journal.start();
        assertTrue(forceWriteThreadSuspendedIsWaiting.await(10, TimeUnit.SECONDS));

        LastLogMark lastLogMarkBeforeWrite = journal.getLastLogMark();

        CountDownLatch latch = new CountDownLatch(1);
        long ledgerId = 1;
        long entryId = 0;
        journal.logAddEntry(ledgerId, entryId, DATA, false /* ackBeforeSync */, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                latch.countDown();
            }
        }, null);
        // logAddEntry should not complete even if ForceWriteThread is suspended
        while (supportQueue.isEmpty()) {
            Thread.sleep(100);
        }
        assertEquals(1, latch.getCount());
        assertEquals(1, supportQueue.size());

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked but PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        // let ForceWriteThread work
        forceWriteThreadSuspended.set(false);

        // callback should complete
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        verify(jc, atLeast(1)).forceWrite(false);

        // verify that log marker advanced
        LastLogMark lastLogMarkAfterForceWrite = journal.getLastLogMark();
        assertTrue(lastLogMarkAfterForceWrite.getCurMark().compare(lastLogMarkBeforeWrite.getCurMark()) > 0);

        journal.shutdown();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAckBeforeSync() throws Exception {
        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setZkServers(null);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(journalDir, conf, ledgerDirsManager);

        CountDownLatch forceWriteThreadSuspendedLatch = new CountDownLatch(1);
        LinkedBlockingQueue<ForceWriteRequest> supportQueue = new LinkedBlockingQueue<>();
        BlockingQueue<ForceWriteRequest> forceWriteRequests = mock(BlockingQueue.class);
        doAnswer((Answer) (InvocationOnMock iom) -> {
            supportQueue.put(iom.getArgument(0));
            return null;
        }).when(forceWriteRequests).put(any(ForceWriteRequest.class));
        when(forceWriteRequests.take()).thenAnswer(i -> {
            // suspend the force write thread
            forceWriteThreadSuspendedLatch.await();
            return supportQueue.take();
        });

        Whitebox.setInternalState(journal, "forceWriteRequests", forceWriteRequests);
        journal.start();

        LastLogMark lastLogMarkBeforeWrite = journal.getLastLogMark();
        CountDownLatch latch = new CountDownLatch(1);
        long ledgerId = 1;
        long entryId = 0;
        journal.logAddEntry(ledgerId, entryId, DATA, true /* ackBeforeSync */, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                latch.countDown();
            }
        }, null);
        // logAddEntry should complete even if ForceWriteThread is suspended
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked but PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        // anyway we are never calling forceWrite
        verify(jc, times(0)).forceWrite(false);

        // verify that log marker did not advance
        LastLogMark lastLogMarkAfterForceWrite = journal.getLastLogMark();
        assertEquals(0, lastLogMarkAfterForceWrite.getCurMark().compare(lastLogMarkBeforeWrite.getCurMark()));

        // let the forceWriteThread exit
        forceWriteThreadSuspendedLatch.countDown();

        journal.shutdown();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAckBeforeSyncWithJournalBufferedEntriesThreshold() throws Exception {
        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        final int journalBufferedEntriesThreshold = 10;
        // sending a burst of entries, more than journalBufferedEntriesThreshold
        final int numEntries = journalBufferedEntriesThreshold + 50;

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setJournalBufferedEntriesThreshold(journalBufferedEntriesThreshold)
            .setZkServers(null);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(journalDir, conf, ledgerDirsManager);

        CountDownLatch forceWriteThreadSuspendedLatch = new CountDownLatch(1);
        LinkedBlockingQueue<ForceWriteRequest> supportQueue = new LinkedBlockingQueue<>();
        BlockingQueue<ForceWriteRequest> forceWriteRequests = mock(BlockingQueue.class);
        doAnswer((Answer) (InvocationOnMock iom) -> {
            supportQueue.put(iom.getArgument(0));
            return null;
        }).when(forceWriteRequests).put(any(ForceWriteRequest.class));
        when(forceWriteRequests.take()).thenAnswer(i -> {
            // suspend the force write thread
            forceWriteThreadSuspendedLatch.await();
            return supportQueue.take();
        });

        Whitebox.setInternalState(journal, "forceWriteRequests", forceWriteRequests);

        Counter flushMaxOutstandingBytesCounter = new CounterImpl();
        Whitebox.setInternalState(journal, "flushMaxOutstandingBytesCounter", flushMaxOutstandingBytesCounter);

        journal.start();

        LastLogMark lastLogMarkBeforeWrite = journal.getLastLogMark();
        CountDownLatch latch = new CountDownLatch(numEntries);
        long ledgerId = 1;
        for (long entryId = 0; entryId < numEntries; entryId++) {
            journal.logAddEntry(ledgerId, entryId, DATA, true /* ackBeforeSync */, new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    latch.countDown();
                }
            }, null);
        }

        // logAddEntry should complete even if ForceWriteThread is suspended
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked but PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        // anyway we are never calling forceWrite
        verify(jc, times(0)).forceWrite(false);

        // verify that log marker did not advance
        LastLogMark lastLogMarkAfterForceWrite = journal.getLastLogMark();
        assertEquals(0, lastLogMarkAfterForceWrite.getCurMark().compare(lastLogMarkBeforeWrite.getCurMark()));

        // let the forceWriteThread exit
        forceWriteThreadSuspendedLatch.countDown();

        assertTrue(flushMaxOutstandingBytesCounter.get() > 1);
        journal.shutdown();
    }

    @Test
    public void testInterleavedRequests() throws Exception {
        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setZkServers(null);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(journalDir, conf, ledgerDirsManager);
        journal.start();

        final int numEntries = 100;
        CountDownLatch latchAckBeforeSynch = new CountDownLatch(numEntries);
        CountDownLatch latchAckAfterSynch = new CountDownLatch(numEntries);

        long ledgerIdAckBeforeSync = 1;
        long ledgerIdAckAfterSync = 2;
        for (long entryId = 0; entryId < numEntries; entryId++) {
            journal.logAddEntry(ledgerIdAckBeforeSync, entryId, DATA, true, new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    latchAckBeforeSynch.countDown();
                }
            }, null);
            journal.logAddEntry(ledgerIdAckAfterSync, entryId, DATA, false, new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    latchAckAfterSynch.countDown();
                }
            }, null);
        }
        assertTrue(latchAckBeforeSynch.await(20, TimeUnit.SECONDS));
        assertTrue(latchAckAfterSynch.await(20, TimeUnit.SECONDS));

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked but PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        verify(jc, atLeast(1)).forceWrite(false);

        journal.shutdown();
    }

    private static class CounterImpl implements Counter {

        AtomicLong counter = new AtomicLong();

        @Override
        public void clear() {
            counter.set(0);
        }

        @Override
        public void inc() {
            counter.incrementAndGet();
        }

        @Override
        public void dec() {
            counter.decrementAndGet();
        }

        @Override
        public void add(long delta) {
            counter.addAndGet(delta);
        }

        @Override
        public Long get() {
            return counter.get();
        }
    }

}
