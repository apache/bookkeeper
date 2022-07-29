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

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Test the bookie with multiple journals.
 */
public class BookieMultipleJournalsTest extends BookKeeperClusterTestCase {

    public BookieMultipleJournalsTest() {
        super(1);
    }

    protected ServerConfiguration newServerConfiguration(int port, File journalDir,
            File[] ledgerDirs) {
        ServerConfiguration conf = super.newServerConfiguration(port, journalDir, ledgerDirs);

        // Use 4 journals
        String[] journalDirs = new String[4];
        for (int i = 0; i < 4; i++) {
            journalDirs[i] = journalDir.getAbsolutePath() + "/journal-" + i;
        }
        conf.setJournalDirsName(journalDirs);

        return conf;
    }

    @Test
    public void testMultipleWritesAndBookieRestart() throws Exception {
        // Creates few ledgers so that writes are spread across all journals
        final int numLedgers = 16;
        final int numEntriesPerLedger = 30;
        List<LedgerHandle> writeHandles = new ArrayList<>();

        for (int i = 0; i < numLedgers; i++) {
            writeHandles.add(bkc.createLedger(1, 1, DigestType.CRC32, new byte[0]));
        }

        for (int i = 0; i < numEntriesPerLedger; i++) {
            for (int j = 0; j < numLedgers; j++) {
                writeHandles.get(j).addEntry(("entry-" + i).getBytes());
            }
        }

        restartBookies();

        // Write another set of entries
        for (int i = numEntriesPerLedger; i < 2 * numEntriesPerLedger; i++) {
            for (int j = 0; j < numLedgers; j++) {
                writeHandles.get(j).addEntry(("entry-" + i).getBytes());
            }
        }

        restartBookies();

        List<LedgerHandle> readHandles = new ArrayList<>();

        for (int i = 0; i < numLedgers; i++) {
            readHandles.add(bkc.openLedger(writeHandles.get(i).getId(), DigestType.CRC32, new byte[0]));
        }

        for (int i = 0; i < numLedgers; i++) {
            Enumeration<LedgerEntry> entries = readHandles.get(i).readEntries(0, numEntriesPerLedger - 1);

            for (int j = 0; j < numEntriesPerLedger; j++) {
                LedgerEntry entry = entries.nextElement();
                assertEquals("entry-" + j, new String(entry.getEntry()));
            }
        }
    }

}
