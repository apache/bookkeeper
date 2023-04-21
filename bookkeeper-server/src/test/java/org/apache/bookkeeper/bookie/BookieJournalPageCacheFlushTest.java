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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Journal.ForceWriteRequest;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Test the bookie journal PageCache flush interval.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.xml.*", "org.xml.*", "org.w3c.*", "com.sun.org.apache.xerces.*"})
@PrepareForTest({JournalChannel.class, Journal.class})
@Slf4j
public class BookieJournalPageCacheFlushTest {

    private static final ByteBuf DATA = Unpooled.wrappedBuffer(new byte[]{});

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @SuppressWarnings("unchecked")
    private BatchedArrayBlockingQueue<ForceWriteRequest> enableForceWriteThreadSuspension(
            CountDownLatch forceWriteThreadSuspendedLatch,
            Journal journal) throws InterruptedException {
        BatchedArrayBlockingQueue<ForceWriteRequest> supportQueue = new BatchedArrayBlockingQueue<>(10000);
        BatchedArrayBlockingQueue<ForceWriteRequest> forceWriteRequests = mock(BatchedArrayBlockingQueue.class);
        doAnswer((Answer) (InvocationOnMock iom) -> {
            supportQueue.put(iom.getArgument(0));
            return null;
        }).when(forceWriteRequests).put(any(ForceWriteRequest.class));
        doAnswer((Answer) (InvocationOnMock iom) -> {
            forceWriteThreadSuspendedLatch.await();
            ForceWriteRequest[] array = iom.getArgument(0);
            return supportQueue.takeAll(array);
        }).when(forceWriteRequests).takeAll(any());
        Whitebox.setInternalState(journal, "forceWriteRequests", forceWriteRequests);
        return supportQueue;
    }

    @Test
    public void testAckAfterSyncPageCacheFlush() throws Exception {
        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(journalDir.getPath())
                .setMetadataServiceUri(null)
                .setJournalAdaptiveGroupWrites(false)
                .setJournalSyncData(true)
                .setJournalPageCacheFlushIntervalMSec(5000);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(0, journalDir, conf, ledgerDirsManager);

        CountDownLatch forceWriteThreadSuspendedLatch = new CountDownLatch(1);
        BatchedArrayBlockingQueue<ForceWriteRequest> supportQueue =
                enableForceWriteThreadSuspension(forceWriteThreadSuspendedLatch, journal);
        journal.start();

        LogMark lastLogMarkBeforeWrite = journal.getLastLogMark().markLog().getCurMark();
        CountDownLatch latch = new CountDownLatch(1);
        long ledgerId = 1;
        long entryId = 0;
        long startTime = System.currentTimeMillis();
        journal.logAddEntry(ledgerId, entryId, DATA, false /* ackBeforeSync */, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                latch.countDown();
            }
        }, null);

        while (supportQueue.isEmpty()) {
            Thread.sleep(100);
        }

        // forceWriteRequest insert into forceWriteRequestQueue not effected by journalPageCacheFlushInterval
        assertTrue(System.currentTimeMillis() - startTime < 5000);

        assertEquals(1, latch.getCount());
        assertEquals(1, supportQueue.size());

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked by PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        // should not call forceWrite
        verify(jc, times(0)).forceWrite(false);

        // let ForceWriteThread work
        forceWriteThreadSuspendedLatch.countDown();
        // callback should complete now
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        verify(jc, times(1)).forceWrite(false);
        assertEquals(0, supportQueue.size());

        // verify that log marker advanced
        LastLogMark lastLogMarkAfterForceWrite = journal.getLastLogMark();
        assertTrue(lastLogMarkAfterForceWrite.getCurMark().compare(lastLogMarkBeforeWrite) > 0);

        journal.shutdown();
    }

    @Test
    public void testAckBeforeSyncPageCacheFlush() throws Exception {
        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(journalDir.getPath())
                .setMetadataServiceUri(null)
                .setJournalAdaptiveGroupWrites(false)
                .setJournalSyncData(true)
                .setJournalPageCacheFlushIntervalMSec(5000);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(0, journalDir, conf, ledgerDirsManager);

        CountDownLatch forceWriteThreadSuspendedLatch = new CountDownLatch(1);
        BatchedArrayBlockingQueue<ForceWriteRequest> supportQueue =
                enableForceWriteThreadSuspension(forceWriteThreadSuspendedLatch, journal);
        journal.start();

        LogMark lastLogMarkBeforeWrite = journal.getLastLogMark().markLog().getCurMark();
        CountDownLatch latch = new CountDownLatch(1);
        long ledgerId = 1;
        long entryId = 0;
        long startTime = System.currentTimeMillis();
        journal.logAddEntry(ledgerId, entryId, DATA, true /* ackBeforeSync */, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                latch.countDown();
            }
        }, null);

        while (supportQueue.isEmpty()) {
            Thread.sleep(100);
        }

        // forceWriteRequest insert into forceWriteRequestQueue not effected by journalPageCacheFlushInterval
        assertTrue(System.currentTimeMillis() - startTime < 5000);
        assertEquals(1, supportQueue.size());

        // callback should completed now
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked by PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        // we are never calling foreWrite
        verify(jc, times(0)).forceWrite(false);

        // verify that log marker did not advance
        LastLogMark lastLogMarkAfterForceWrite = journal.getLastLogMark();
        assertEquals(0, lastLogMarkAfterForceWrite.getCurMark().compare(lastLogMarkBeforeWrite));

        // let the forceWriteThread exit
        forceWriteThreadSuspendedLatch.countDown();

        journal.shutdown();
    }

    @Test
    public void testAckBeforeUnSyncPageCacheFlush() throws Exception {
        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(journalDir.getPath())
                .setMetadataServiceUri(null)
                .setJournalAdaptiveGroupWrites(false)
                .setJournalSyncData(false)
                .setJournalPageCacheFlushIntervalMSec(5000);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(0, journalDir, conf, ledgerDirsManager);

        CountDownLatch forceWriteThreadSuspendedLatch = new CountDownLatch(1);
        BatchedArrayBlockingQueue<ForceWriteRequest> supportQueue =
                enableForceWriteThreadSuspension(forceWriteThreadSuspendedLatch, journal);
        journal.start();

        CountDownLatch latch = new CountDownLatch(2);
        long ledgerId = 1;
        long entryId = 0;
        LogMark lastLogMarkBeforeWrite = journal.getLastLogMark().markLog().getCurMark();
        journal.logAddEntry(ledgerId, entryId, DATA, true, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                latch.countDown();
            }
        }, null);

        // the forceWriteRequest should not generated because of journalPageCacheFlushIntervalMSec control
        assertEquals(0, supportQueue.size());

        // wait journalPageCacheFlushIntervalMsec timeout
        Thread.sleep(10000);

        // add an entry to journal, wake up journal main thread which is blocked on queue.take()
        journal.logAddEntry(ledgerId, entryId + 1, DATA, true, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                latch.countDown();
            }
        }, null);

        // wait forceWriteRequest generated
        while (supportQueue.isEmpty()) {
            Thread.sleep(100);
        }

        // only one forceWriteRequest inserted into forceWriteRequestQueue
        assertEquals(1, supportQueue.size());

        // callback should completed now
        assertTrue(latch.await(20, TimeUnit.SECONDS));

        // in constructor of JournalChannel we are calling forceWrite(true) but it is not tracked by PowerMock
        // because the 'spy' is applied only on return from the constructor
        verify(jc, times(0)).forceWrite(true);

        // we are never calling foreWrite
        verify(jc, times(0)).forceWrite(false);

        // verify that log marker did not advance
        LastLogMark lastLogMarkAfterForceWrite = journal.getLastLogMark();
        assertEquals(0, lastLogMarkAfterForceWrite.getCurMark().compare(lastLogMarkBeforeWrite));

        // let the forceWriteThread exit
        forceWriteThreadSuspendedLatch.countDown();

        journal.shutdown();
    }
}
