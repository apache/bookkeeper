/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static junit.framework.TestCase.assertEquals;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of correct-ensemble-placement command.
 */
public class CorrectEnsemblePlacementCmdTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CorrectEnsemblePlacementCmdTest.class);
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";

    public CorrectEnsemblePlacementCmdTest() throws Exception {
        super(1);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setGcWaitTime(60000);
        baseConf.setFlushInterval(1);
    }

    /**
     * list of entry logger files that contains given ledgerId.
     */
    @Test
    public void testArgument() throws Exception {
        @Cleanup final BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        @Cleanup final LedgerHandle lh = createLedgerWithEntries(bk, 10, 1, 1);

        final String[] argv1 = {"correct-ensemble-placement", "--ledgerids", String.valueOf(lh.getId()),
                "--skipOpenLedgers", "--force"};
        final String[] argv2 = {"correct-ensemble-placement", "--ledgerids", String.valueOf(lh.getId()),
                "--skipOpenLedgers", "--force", "--dryrun"};
        final BookieShell bkShell =
                new BookieShell(LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER);
        bkShell.setConf(baseClientConf);

        assertEquals("Failed to return exit code!", 0, bkShell.run(argv1));
        assertEquals("Failed to return exit code!", 0, bkShell.run(argv2));
    }

    private LedgerHandle createLedgerWithEntries(BookKeeper bk, int numOfEntries,
                                                 int ensembleSize, int quorumSize) throws Exception {
        LedgerHandle lh = bk.createLedger(ensembleSize, quorumSize, digestType, PASSWORD.getBytes());
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch latch = new CountDownLatch(numOfEntries);

        final AsyncCallback.AddCallback cb = (rccb, lh1, entryId, ctx) -> {
            rc.compareAndSet(BKException.Code.OK, rccb);
            latch.countDown();
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
