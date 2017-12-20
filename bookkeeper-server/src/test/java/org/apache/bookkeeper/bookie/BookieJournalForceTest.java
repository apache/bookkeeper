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
import static org.mockito.Mockito.mock;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.replace;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test the bookie journal.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JournalChannel.class})
@Slf4j
public class BookieJournalForceTest {

    private static final ByteBuf DATA = Unpooled.wrappedBuffer(new byte[]{});

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testAckBeforeSync() throws Exception {
        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setZkServers(null);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(journalDir, conf, ledgerDirsManager);
        journal.start();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger countForceWriteCallsBeforeAck = new AtomicInteger();
        replace(method(JournalChannel.class, "forceWrite")).with(new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                log.info("{} called with arguments {} ", method.getName(), Arrays.toString(args));
                if (latch.getCount() == 1) {
                    countForceWriteCallsBeforeAck.incrementAndGet();
                }
                try {
                    method.invoke(proxy, args);
                } catch (InvocationTargetException err) {
                    throw err.getCause();
                }
                return null;
            }
        });

        // wait for first file to be created
        while (countForceWriteCallsBeforeAck.get() == 0) {
            Thread.sleep(100);
        }

        long ledgerId = 1;
        long entryId = 0;
        journal.logAddEntry(ledgerId, entryId, DATA, true /* ackBeforeSync */, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                latch.countDown();
            }
        }, null);
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertEquals(1, countForceWriteCallsBeforeAck.get());

        journal.shutdown();
    }

    @Test
    public void testAckAfterSync() throws Exception {
        File journalDir = tempDir.newFolder();
        Bookie.checkDirectoryStructure(Bookie.getCurrentDirectory(journalDir));

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setZkServers(null);

        LedgerDirsManager ledgerDirsManager = mock(LedgerDirsManager.class);
        Journal journal = new Journal(journalDir, conf, ledgerDirsManager);
        journal.start();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger countForceWriteCallsBeforeAck = new AtomicInteger();
        replace(method(JournalChannel.class, "forceWrite")).with(new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                log.info("{} called with arguments {} ", method.getName(), Arrays.toString(args));
                if (latch.getCount() == 1) {
                    countForceWriteCallsBeforeAck.incrementAndGet();
                }
                try {
                    method.invoke(proxy, args);
                } catch (InvocationTargetException err) {
                    throw err.getCause();
                }
                return null;
            }
        });

        // wait for first file to be created
        while (countForceWriteCallsBeforeAck.get() == 0) {
            Thread.sleep(100);
        }

        long ledgerId = 1;
        long entryId = 0;
        journal.logAddEntry(ledgerId, entryId, DATA, false /* ackBeforeSync */, new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                latch.countDown();
            }
        }, null);
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertEquals(2, countForceWriteCallsBeforeAck.get());

        journal.shutdown();
    }

}
