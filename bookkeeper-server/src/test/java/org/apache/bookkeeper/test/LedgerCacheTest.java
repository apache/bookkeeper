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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.security.GeneralSecurityException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.client.MacDigestManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.log4j.Logger;

import junit.framework.TestCase;

/**
 * Tests writing to concurrent ledgers
 */
public class LedgerCacheTest extends TestCase {
    static Logger LOG = Logger.getLogger(LedgerCacheTest.class);
    
    Bookie bookie;
    File txnDir, ledgerDir;
    
    class TestWriteCallback implements WriteCallback {
        public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx){
            LOG.info("Added entry: " + rc + ", " + ledgerId + ", " + entryId + ", " + addr);
        }
    }
    
    
    @Override
    @Before
    public void setUp() throws IOException {
        String txnDirName = System.getProperty("txnDir");
        if (txnDirName != null) {
            txnDir = new File(txnDirName);
        }
        String ledgerDirName = System.getProperty("ledgerDir");
        if (ledgerDirName != null) {
            ledgerDir = new File(ledgerDirName);
        }
        File tmpFile = File.createTempFile("book", ".txn", txnDir);
        tmpFile.delete();
        txnDir = new File(tmpFile.getParent(), tmpFile.getName()+".dir");
        txnDir.mkdirs();
        tmpFile = File.createTempFile("book", ".ledger", ledgerDir);
        ledgerDir = new File(tmpFile.getParent(), tmpFile.getName()+".dir");
        ledgerDir.mkdirs();
        
        
        bookie = new Bookie(5000, null, txnDir, new File[] {ledgerDir});   
    }
    
    
    @Override
    @After
    public void tearDown() {
        try {
            bookie.shutdown();
            recursiveDelete(txnDir);
            recursiveDelete(ledgerDir);
        } catch (InterruptedException e) {
            LOG.error("Error tearing down", e);
        }
    }
    
    /**
     * Recursively deletes a directory. This is a duplication of BookieClientTest.
     * 
     * @param dir
     */
    private static void recursiveDelete(File dir) {
        File children[] = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                recursiveDelete(child);
            }
        }
        dir.delete();
    }
    
    @Test
    public void testAddEntryException() 
    throws GeneralSecurityException, BookieException {
        /*
         * Populate ledger cache
         */
        try{
            byte[] masterKey = "blah".getBytes();
            for( int i = 0; i < 30000; i++){
                MacDigestManager dm = new MacDigestManager(i, masterKey);
                byte[] data = "0123456789".getBytes();
                ByteBuffer entry = dm.computeDigestAndPackageForSending(0, 0, 10, data, 0, data.length).toByteBuffer();
                bookie.addEntry(entry, new TestWriteCallback(), null, masterKey);
            }
        } catch (IOException e) {
            LOG.error("Got IOException.", e);
            fail("Failed to add entry.");
        }
    }
    
}