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

package org.apache.bookkeeper.bookie;

import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Test the checkpoint logic in bookies.
 */
@Slf4j
public abstract class LedgerStorageTestBase {

    @Rule
    public TestName testName = new TestName();

    protected ServerConfiguration conf;
    protected File journalDir, ledgerDir;
    protected LedgerDirsManager ledgerDirsManager;

    private File createTempDir(String suffix) throws Exception {
        File dir = File.createTempFile(testName.getMethodName(), suffix);
        dir.delete();
        dir.mkdirs();
        return dir;
    }

    protected LedgerStorageTestBase() {
        conf = TestBKConfiguration.newServerConfiguration();
    }

    @Before
    public void setUp() throws Exception {
        journalDir = createTempDir("journal");
        ledgerDir = createTempDir("ledger");

        // create current directories
        BookieImpl.getCurrentDirectory(journalDir).mkdir();
        BookieImpl.getCurrentDirectory(ledgerDir).mkdir();

        // build the configuration
        conf.setMetadataServiceUri(null);
        conf.setJournalDirName(journalDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });

        // build the ledger monitor
        DiskChecker checker = new DiskChecker(
            conf.getDiskUsageThreshold(),
            conf.getDiskUsageWarnThreshold());
        ledgerDirsManager = new LedgerDirsManager(
            conf,
            conf.getLedgerDirs(),
            checker);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(journalDir);
        FileUtils.deleteDirectory(ledgerDir);
    }


}
