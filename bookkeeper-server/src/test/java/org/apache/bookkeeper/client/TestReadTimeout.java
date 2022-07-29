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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing.
 *
 */
public class TestReadTimeout extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(TestReadTimeout.class);

    DigestType digestType;

    public TestReadTimeout() {
        super(10);
        this.digestType = DigestType.CRC32;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testReadTimeout() throws Exception {
        final AtomicBoolean completed = new AtomicBoolean(false);

        LedgerHandle writelh = bkc.createLedger(3, 3, digestType, "testPasswd".getBytes());
        String tmp = "Foobar";

        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        Set<BookieId> beforeSet = new HashSet<BookieId>();
        beforeSet.addAll(writelh.getLedgerMetadata().getEnsembleAt(numEntries));

        final BookieId bookieToSleep = writelh.getLedgerMetadata().getEnsembleAt(numEntries).get(0);
        int sleeptime = baseClientConf.getReadTimeout() * 3;
        CountDownLatch latch = sleepBookie(bookieToSleep, sleeptime);
        latch.await();

        writelh.asyncAddEntry(tmp.getBytes(),
            new AddCallback() {
                public void addComplete(int rc, LedgerHandle lh,
                                        long entryId, Object ctx) {
                    completed.set(true);
                }
            }, null);
        Thread.sleep((baseClientConf.getReadTimeout() * 3) * 1000);
        Assert.assertTrue("Write request did not finish", completed.get());

        Set<BookieId> afterSet = new HashSet<BookieId>();
        afterSet.addAll(writelh.getLedgerMetadata().getEnsembleAt(numEntries + 1));
        beforeSet.removeAll(afterSet);
        Assert.assertTrue("Bookie set should not match", beforeSet.size() != 0);

    }
}
