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
package org.apache.bookkeeper.tools.cli.helpers;

import java.io.File;
import org.junit.Before;

/**
 * A test base for testing bookie commands.
 */
public abstract class BookieCommandTestBase extends CommandTestBase {

    protected final int numJournalDirs;
    protected final int numLedgerDirs;

    protected BookieCommandTestBase(int numJournalDirs, int numLedgerDirs) {
        this.numJournalDirs = numJournalDirs;
        this.numLedgerDirs = numLedgerDirs;
    }

    @Before
    public void setup() throws Exception {
        String[] journalDirs = new String[numJournalDirs];
        for (int i = 0; i < numJournalDirs; i++) {
            File dir = testDir.newFile();
            dir.mkdirs();
            journalDirs[i] = dir.getAbsolutePath();
        }
        journalDirsName = journalDirs;
        String[] ledgerDirs = new String[numLedgerDirs];
        for (int i = 0; i < numLedgerDirs; i++) {
            File dir = testDir.newFile();
            dir.mkdirs();
            ledgerDirs[i] = dir.getAbsolutePath();
        }
        ledgerDirNames = ledgerDirs;

        String[] indexDirs = new String[numLedgerDirs];
        for (int i = 0; i < numLedgerDirs; i++) {
            File dir = testDir.newFile();
            dir.mkdirs();
            indexDirs[i] = dir.getAbsolutePath();
        }
        indexDirNames = indexDirs;
    }

}
