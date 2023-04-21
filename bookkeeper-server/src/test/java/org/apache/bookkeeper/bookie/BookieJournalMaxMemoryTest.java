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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MemoryLimitController;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Test the bookie journal max memory controller.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.xml.*", "org.xml.*", "org.w3c.*", "com.sun.org.apache.xerces.*"})
@PrepareForTest({JournalChannel.class, Journal.class})
@Slf4j
public class BookieJournalMaxMemoryTest {

    private static final ByteBuf DATA = Unpooled.wrappedBuffer(new byte[1024 * 1024]);

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testAckAfterSyncPageCacheFlush() throws Exception {
        File journalDir = tempDir.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(journalDir.getPath())
                .setJournalMaxMemorySizeMb(1);

        JournalChannel jc = spy(new JournalChannel(journalDir, 1));
        whenNew(JournalChannel.class).withAnyArguments().thenReturn(jc);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = spy(new Journal(0, journalDir, conf, ledgerDirsManager));
        Whitebox.setInternalState(journal, "memoryLimitController",
                spy(new MemoryLimitController(1)));
        MemoryLimitController mlc = Whitebox.getInternalState(journal, "memoryLimitController");

        journal.start();

        CountDownLatch latch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            long ledgerId = 1;
            long entryId = i;

            journal.logAddEntry(ledgerId, entryId, DATA, false,
                    (rc, ledgerId1, entryId1, addr, ctx) -> latch.countDown(),
                    null);
        }

        latch.await();

        verify(mlc, times(10)).reserveMemory(DATA.readableBytes());
        verify(mlc, times(10)).releaseMemory(DATA.readableBytes());

        journal.shutdown();
    }
}
