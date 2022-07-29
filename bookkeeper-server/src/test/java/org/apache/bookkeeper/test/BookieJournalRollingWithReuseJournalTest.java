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
package org.apache.bookkeeper.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests that bookie rolling journals for reuse journal files.
 */
@Slf4j
public class BookieJournalRollingWithReuseJournalTest extends BookieJournalRollingTest {

    public BookieJournalRollingWithReuseJournalTest() {
        super();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseConf.setJournalReuseFiles(true);
        super.setUp();
    }

    @Test
    public void testJournalRolling() throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Testing Journal Rolling");
        }

        // Write enough ledger entries so that we roll over journals
        LedgerHandle[] lhs = writeLedgerEntries(10, 1024, 1024);
        long[] ledgerIds = new long[lhs.length];
        for (int i = 0; i < lhs.length; i++) {
            ledgerIds[i] = lhs[i].getId();
            lhs[i].close();
        }

        Awaitility.await().untilAsserted(() -> {
            // verify that we only keep at most journal files
            for (File journalDir : bookieJournalDirs()) {
                File[] journals = journalDir.listFiles();
                int numJournals = 0;
                for (File f : journals) {
                    if (!f.getName().endsWith(".txn")) {
                        continue;
                    }
                    ++numJournals;
                }
                assertTrue(numJournals <= 2);
            }
        });

        // restart bookies
        // ensure after restart we can read the entries since journals rolls
        restartBookies();
        validLedgerEntries(ledgerIds, 1024, 1024);
    }

}