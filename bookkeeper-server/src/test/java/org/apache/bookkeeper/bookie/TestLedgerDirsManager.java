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
package org.apache.bookkeeper.bookie;

import java.io.File;

import junit.framework.TestCase;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLedgerDirsManager extends TestCase {
    static Logger LOG = LoggerFactory.getLogger(TestLedgerDirsManager.class);

    ServerConfiguration conf = new ServerConfiguration();
    File curDir;
    LedgerDirsManager dirsManager;

    @Before
    public void setUp() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});

        dirsManager = new LedgerDirsManager(conf);
    }

    @Test(timeout=60000)
    public void testPickWritableDirExclusive() throws Exception {
        try {
            dirsManager.pickRandomWritableDir(curDir);
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            assertTrue(true);
        }
    }

    @Test(timeout=60000)
    public void testNoWritableDir() throws Exception {
        try {
            dirsManager.addToFilledDirs(curDir);
            dirsManager.pickRandomWritableDir();
            fail("Should not reach here due to there is no writable ledger dir.");
        } catch (NoWritableLedgerDirException nwlde) {
            // expected to fail with no writable ledger dir
            assertEquals("Should got NoWritableLedgerDirException w/ 'All ledger directories are non writable'.",
                         "All ledger directories are non writable", nwlde.getMessage());
        }
    }

}
