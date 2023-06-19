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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.logIdFromLocation;
import static org.apache.bookkeeper.bookie.storage.EntryLogTestUtils.makeEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.MockEntryLogIds;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Test;

/**
 * TestDirectEntryLoggerForEntryLogPerLedger.
 */
public class TestDirectEntryLoggerForEntryLogPerLedger {

    private final Slogger slog = Slogger.CONSOLE;

    private final TmpDirs tmpDirs = new TmpDirs();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
    }


    @Test
    public void testAddEntry() throws Exception {
        File ledgerDir = tmpDirs.createNew("logRolling", "ledgers");
        File curDir = new File(ledgerDir, "current");
        curDir.mkdirs();

        long ledger1 = 1;
        long ledger2 = 2;
        long ledger3 = 3;

        ByteBuf e1 = makeEntry(ledger1, 1L, 4000);
        ByteBuf e2 = makeEntry(ledger2, 2L, 4000);
        ByteBuf e3 = makeEntry(ledger3, 3L, 4000);

        try (EntryLogger elog = new DirectEntryLoggerForEntryLogPerLedger(10,
                10,
                curDir, new MockEntryLogIds(),
                new NativeIOImpl(),
                ByteBufAllocator.DEFAULT,
                MoreExecutors.newDirectExecutorService(),
                MoreExecutors.newDirectExecutorService(),
                1 * 1024 * 1024 * 1024, // 1GB，max file size (header + size of one entry)
                10 * 1024 * 1024, // max sane entry size
                1024 * 1024, // total write buffer size
                1024 * 1024, // total read buffer size
                64 * 1024, // read buffer size
                1, // numReadThreads
                300, // max fd cache time in seconds
                slog, NullStatsLogger.INSTANCE)) {

            // Data from different ledgers is no longer written to the same log file
            long loc1 = elog.addEntry(ledger1, e1.slice());
            int logId1 = logIdFromLocation(loc1);
            assertThat(logId1, equalTo(1));

            long loc2 = elog.addEntry(ledger2, e2.slice());
            int logId2 = logIdFromLocation(loc2);
            assertThat(logId2, equalTo(2));

            long loc3 = elog.addEntry(ledger3, e3.slice());
            int logId3 = logIdFromLocation(loc3);
            assertThat(logId3, equalTo(3));
        }
    }
}
