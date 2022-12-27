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
package org.apache.bookkeeper.client;

import static junit.framework.TestCase.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.bookie.BookieAccessor;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test for bookieshell ledger command.
 */
public class LedgerCmdTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerCmdTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";

    public LedgerCmdTest() {
        super(1);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setGcWaitTime(60000);
        baseConf.setFlushInterval(1);
    }


    /**
     * list of entry logger files that contains given ledgerId.
     */
    @Test
    public void testLedgerDbStorageCmd() throws Exception {

        BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        LOG.info("Create ledger and add entries to it");
        LedgerHandle lh1 = createLedgerWithEntries(bk, 10);

        for (int i = 0; i < bookieCount(); i++) {
                BookieAccessor.forceFlush((BookieImpl) serverByIndex(i).getBookie());
        }

        String[] argv = { "ledger", Long.toString(lh1.getId()) };
        final ServerConfiguration conf = confByIndex(0);
        conf.setUseHostNameAsBookieID(true);

        BookieShell bkShell =
            new BookieShell(LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER);
        bkShell.setConf(conf);

        assertEquals("Failed to return exit code!", 0, bkShell.run(argv));

    }

    private LedgerHandle createLedgerWithEntries(BookKeeper bk, int numOfEntries) throws Exception {
        LedgerHandle lh = bk.createLedger(1, 1, digestType, PASSWORD.getBytes());
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch latch = new CountDownLatch(numOfEntries);

        final AddCallback cb = new AddCallback() {
            public void addComplete(int rccb, LedgerHandle lh, long entryId, Object ctx) {
                rc.compareAndSet(BKException.Code.OK, rccb);
                latch.countDown();
            }
        };
        for (int i = 0; i < numOfEntries; i++) {
            lh.asyncAddEntry(("foobar" + i).getBytes(), cb, null);
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        return lh;
    }
}
