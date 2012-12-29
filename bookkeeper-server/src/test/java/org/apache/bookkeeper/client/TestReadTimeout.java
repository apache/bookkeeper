package org.apache.bookkeeper.client;

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

import org.junit.*;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing;
 *
 */
public class TestReadTimeout extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestReadTimeout.class);

    DigestType digestType;

    public TestReadTimeout() {
        super(10);
        this.digestType = DigestType.CRC32;
    }

    @Test(timeout=60000)
    public void testReadTimeout() throws Exception {
        final AtomicBoolean completed = new AtomicBoolean(false);

        LedgerHandle writelh = bkc.createLedger(3,3,digestType, "testPasswd".getBytes());
        String tmp = "Foobar";
        
        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }
        
        Set<InetSocketAddress> beforeSet = new HashSet<InetSocketAddress>();
        for (InetSocketAddress addr : writelh.getLedgerMetadata().getEnsemble(numEntries)) {
            beforeSet.add(addr);
        }

        final InetSocketAddress bookieToSleep 
            = writelh.getLedgerMetadata().getEnsemble(numEntries).get(0);
        int sleeptime = baseClientConf.getReadTimeout()*3;
        CountDownLatch latch = sleepBookie(bookieToSleep, sleeptime);
        latch.await();

        writelh.asyncAddEntry(tmp.getBytes(), 
                new AddCallback() {
                    public void addComplete(int rc, LedgerHandle lh, 
                                            long entryId, Object ctx) {
                        completed.set(true);
                    }
                }, null);
        Thread.sleep((baseClientConf.getReadTimeout()*3)*1000);
        Assert.assertTrue("Write request did not finish", completed.get());

        Set<InetSocketAddress> afterSet = new HashSet<InetSocketAddress>();
        for (InetSocketAddress addr : writelh.getLedgerMetadata().getEnsemble(numEntries+1)) {
            afterSet.add(addr);
        }
        beforeSet.removeAll(afterSet);
        Assert.assertTrue("Bookie set should not match", beforeSet.size() != 0);
    }
}
