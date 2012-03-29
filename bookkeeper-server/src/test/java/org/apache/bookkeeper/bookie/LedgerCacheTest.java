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

import java.io.File;
import java.io.IOException;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

/**
 * LedgerCache related test cases
 */
public class LedgerCacheTest extends TestCase {
    static Logger LOG = LoggerFactory.getLogger(LedgerCacheTest.class);

    LedgerManager ledgerManager;
    LedgerCache ledgerCache;
    ServerConfiguration conf;
    File txnDir, ledgerDir;

    @Override
    @Before
    public void setUp() throws Exception {
        txnDir = File.createTempFile("ledgercache", "txn");
        txnDir.delete();
        txnDir.mkdir();
        ledgerDir = File.createTempFile("ledgercache", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();
        // create current dir
        new File(ledgerDir, Bookie.CURRENT_DIR).mkdir();

        conf = new ServerConfiguration();
        conf.setZkServers(null);
        conf.setJournalDirName(txnDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });

        ledgerManager = LedgerManagerFactory.newLedgerManager(conf, null);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        ledgerManager.close();
        FileUtils.deleteDirectory(txnDir);
        FileUtils.deleteDirectory(ledgerDir);
    }

    private void newLedgerCache() {
        ledgerCache = new LedgerCacheImpl(conf, ledgerManager);
    }

    @Test
    public void testAddEntryException() {
        // set page limitation
        conf.setPageLimit(10);
        // create a ledger cache
        newLedgerCache();
        /*
         * Populate ledger cache.
         */
        try {
            byte[] masterKey = "blah".getBytes();
            for( int i = 0; i < 100; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                ledgerCache.putEntryOffset(i, 0, i*8);
            }
        } catch (IOException e) {
            LOG.error("Got IOException.", e);
            fail("Failed to add entry.");
        }
    }

    @Test
    public void testLedgerEviction() throws Exception {
        int numEntries = 10;
        // limit open files & pages
        conf.setOpenFileLimit(1).setPageLimit(2)
            .setPageSize(8 * numEntries);
        // create ledger cache
        newLedgerCache();
        try {
            int numLedgers = 3;
            byte[] masterKey = "blah".getBytes();
            for (int i=1; i<=numLedgers; i++) {
                ledgerCache.setMasterKey((long)i, masterKey);
                for (int j=0; j<numEntries; j++) {
                    ledgerCache.putEntryOffset(i, j, i * numEntries + j);
                }
            }
        } catch (Exception e) {
            LOG.error("Got Exception.", e);
            fail("Failed to add entry.");
        }
    }

}
