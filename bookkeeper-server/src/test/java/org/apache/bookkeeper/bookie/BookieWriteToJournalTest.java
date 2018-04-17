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
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
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
            final ByteBuf data = Unpooled.buffer();
            data.writeLong(ledgerId);
            data.writeLong(entryId);
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
}
