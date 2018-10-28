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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test reading an entry from replicas in sequence way.
 */
public class TestSequenceRead extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestSequenceRead.class);

    final DigestType digestType;
    final byte[] passwd = "sequence-read".getBytes();

    public TestSequenceRead() {
        super(5);
        this.digestType = DigestType.CRC32;
    }

    private LedgerHandle createLedgerWithDuplicatedBookies() throws Exception {
        final LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, passwd);
        // introduce duplicated bookies in an ensemble.
        SortedMap<Long, ? extends List<BookieSocketAddress>> ensembles = lh.getLedgerMetadata().getAllEnsembles();
        TreeMap<Long, List<BookieSocketAddress>> newEnsembles = new TreeMap<>();
        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> entry : ensembles.entrySet()) {
            List<BookieSocketAddress> newList = new ArrayList<BookieSocketAddress>(entry.getValue().size());
            BookieSocketAddress firstBookie = entry.getValue().get(0);
            for (BookieSocketAddress ignored : entry.getValue()) {
                newList.add(firstBookie);
            }
            newEnsembles.put(entry.getKey(), newList);
        }
        lh.getLedgerMetadata().setEnsembles(newEnsembles);
        // update the ledger metadata with duplicated bookies
        final CountDownLatch latch = new CountDownLatch(1);
        bkc.getLedgerManager().writeLedgerMetadata(lh.getId(), lh.getLedgerMetadata(),
                new BookkeeperInternalCallbacks.GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                if (BKException.Code.OK == rc) {
                    latch.countDown();
                } else {
                    logger.error("Error on writing ledger metadata for ledger {} : ", lh.getId(),
                            BKException.getMessage(rc));
                }
            }
        });
        latch.await();
        logger.info("Update ledger metadata with duplicated bookies for ledger {}.", lh.getId());
        return lh;
    }

    @Test
    public void testSequenceReadOnDuplicatedBookies() throws Exception {
        final LedgerHandle lh = createLedgerWithDuplicatedBookies();

        // should be able to open the ledger even it has duplicated bookies
        final LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, passwd);
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
    }

}
