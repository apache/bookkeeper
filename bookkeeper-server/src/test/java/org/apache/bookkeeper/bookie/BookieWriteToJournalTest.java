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
@PrepareForTest({Journal.class})
@Slf4j
public class BookieWriteToJournalTest {

    private static final ByteBuf DATA = Unpooled.buffer();
    static {
        DATA.writeLong(1); // ledgerId
        DATA.writeLong(1); // entryId
    }

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
            .setZkServers(null);
        Bookie b = new Bookie(conf);
        b.start();
        Boolean[] effectiveAckBeforeSync = new Boolean[1];
        replace(method(Journal.class, "logAddEntry", ByteBuf.class, Boolean.TYPE, WriteCallback.class, Object.class))
                .with(new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                log.info("{} called with arguments {} ", method.getName(), Arrays.toString(args));
                effectiveAckBeforeSync[0] = (Boolean) args[1];
                try {
                    method.invoke(proxy, args);
                } catch (InvocationTargetException err) {
                    throw err.getCause();
                }
                return null;
            }
        });

        for (boolean ackBeforeSync : new boolean[]{true, false}) {
            CountDownLatch latch = new CountDownLatch(1);
            DATA.retain();
            b.addEntry(DATA, ackBeforeSync, new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    latch.countDown();
                }
            }, null, new byte[]{});
            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(ackBeforeSync, effectiveAckBeforeSync[0]);
        }
        b.shutdown();
    }
}
