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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * This unit test tests ensemble change.
 */
public class TestEnsembleChange extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestEnsembleChange.class);

    private final BookKeeper.DigestType digestType;

    public TestEnsembleChange() {
        super(3);
        this.digestType = BookKeeper.DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void TestFailedBookieIsNotInCurrentEnsembleWhenEnsembleChange() throws Exception {
        int ensembleSize = 3;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        int numEntries = 10;

        final LedgerHandleFaultInjector injector = new LedgerHandleFaultInjector() {
            @Override
            public void sleepWhenTest() {
                // make the run time of ensemble change long
                // to simulate add entry is faster than ensemble change relatively
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.info("catch InterruptedException when sleep", e);
                }
            }
        };
        LedgerHandleFaultInjector.instance = injector;

        LedgerHandle lh = bkc.createLedger(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, "".getBytes());

        // ensure available bookie to ensemble change
        ServerConfiguration conf = newServerConfiguration();
        startAndAddBookie(conf);

        // normal add entry
        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(tmp.getBytes());
        }

        // simulate slow bookie
        BookieId bookie = getBookie(0);
        sleepBookie(bookie, 30);

        lh.addEntry(tmp.getBytes());
        // make a 2-second interval between first timeout and following timeouts
        // so that first ensemble change can finish before following ensemble changes
        Thread.sleep(2000);

        // create following ensemble changes and the failed bookie is not in current ensemble
        for (int i = numEntries + 1; i < numEntries * 2; i++) {
            lh.addEntry(tmp.getBytes());
            Thread.sleep(1000);
        }

        assertEquals(2 * numEntries - 1, lh.getLastAddConfirmed());
    }
}
