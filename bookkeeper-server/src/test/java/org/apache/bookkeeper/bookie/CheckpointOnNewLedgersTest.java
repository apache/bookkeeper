/*
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
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test the checkpoint logic of {@link DbLedgerStorage}.
 */
@Slf4j
public class CheckpointOnNewLedgersTest {

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private ServerConfiguration conf;
    private BookieImpl bookie;
    private CountDownLatch getLedgerDescCalledLatch;
    private CountDownLatch getLedgerDescWaitLatch;

    @Before
    public void setup() throws Exception {
        File bkDir = testDir.newFolder("dbLedgerStorageCheckpointTest");
        File curDir = BookieImpl.getCurrentDirectory(bkDir);
        BookieImpl.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        conf.setJournalDirsName(new String[] { bkDir.toString() });
        conf.setLedgerDirNames(new String[] { bkDir.toString() });
        conf.setEntryLogSizeLimit(10 * 1024);

        bookie = spy(new TestBookieImpl(conf));
        bookie.start();

        getLedgerDescCalledLatch = new CountDownLatch(1);
        getLedgerDescWaitLatch = new CountDownLatch(1);

        // spy `getLedgerForEntry`
        doAnswer(invocationOnMock -> {
            ByteBuf entry = invocationOnMock.getArgument(0);
            long ledgerId = entry.getLong(entry.readerIndex());

            LedgerDescriptor ld = (LedgerDescriptor) invocationOnMock.callRealMethod();

            if (ledgerId % 2 == 1) {
                getLedgerDescCalledLatch.countDown();
                getLedgerDescWaitLatch.await();
            }

            return ld;
        }).when(bookie).getLedgerForEntry(
            any(ByteBuf.class),
            any(byte[].class));
    }

    @After
    public void teardown() throws Exception {
        if (null != bookie) {
            bookie.shutdown();
        }
    }

    private static ByteBuf createByteBuf(long ledgerId, long entryId, int entrySize) {
        byte[] data = new byte[entrySize];
        ThreadLocalRandom.current().nextBytes(data);
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        buffer.writerIndex(0);
        buffer.writeLong(ledgerId);
        buffer.writeLong(entryId);
        buffer.writeLong(entryId - 1); // lac
        buffer.writerIndex(entrySize);
        return buffer;
    }

    @Test
    public void testCheckpoint() throws Exception {
        int entrySize = 1024;
        long l1 = 1L;
        long l2 = 2L;

        final CountDownLatch writeL1Latch = new CountDownLatch(1);

        Thread t1 = new Thread(() -> {

            ByteBuf entry = createByteBuf(l1, 0L, entrySize);
            try {
                bookie.addEntry(
                    entry,
                    false,
                    (rc, ledgerId, entryId, addr, ctx) -> writeL1Latch.countDown(),
                    null,
                    new byte[0]
                );
            } catch (Exception e) {
                log.info("Failed to write entry to l1", e);
            }

        }, "ledger-1-writer");

        t1.start();

        // wait until the ledger desc is opened
        getLedgerDescCalledLatch.await();

        LastLogMark logMark = bookie.journals.get(0).getLastLogMark().markLog();

        // keep write entries to l2 to trigger entry log rolling to checkpoint
        int numEntries = 10;
        final CountDownLatch writeL2Latch = new CountDownLatch(numEntries);
        for (int i = 0; i < numEntries; i++) {
            ByteBuf entry = createByteBuf(l2, i, entrySize);
            bookie.addEntry(
                entry,
                false,
                (rc, ledgerId, entryId, addr, ctx) -> writeL2Latch.countDown(),
                null,
                new byte[0]);
        }
        writeL2Latch.await();

        // wait until checkpoint to complete and journal marker is rolled.
        bookie.syncThread.getExecutor().submit(() -> {}).get();

        log.info("Wait until checkpoint is completed");

        // the journal mark is rolled.
        LastLogMark newLogMark = bookie.journals.get(0).getLastLogMark().markLog();
        assertTrue(newLogMark.getCurMark().compare(logMark.getCurMark()) > 0);

        // resume l1-writer to continue writing the entries
        getLedgerDescWaitLatch.countDown();

        // wait until the l1 entry is written
        writeL1Latch.await();
        t1.join();

        // construct a new bookie to simulate "bookie restart from crash"
        Bookie newBookie = new TestBookieImpl(conf);
        newBookie.start();

        for (int i = 0; i < numEntries; i++) {
            ByteBuf entry = newBookie.readEntry(l2, i);
            assertNotNull(entry);
            assertEquals(l2, entry.readLong());
            assertEquals((long) i, entry.readLong());
            ReferenceCountUtil.release(entry);
        }

        ByteBuf entry = newBookie.readEntry(l1, 0L);
        assertNotNull(entry);
        assertEquals(l1, entry.readLong());
        assertEquals(0L, entry.readLong());
        ReferenceCountUtil.release(entry);
        newBookie.shutdown();
    }

}
