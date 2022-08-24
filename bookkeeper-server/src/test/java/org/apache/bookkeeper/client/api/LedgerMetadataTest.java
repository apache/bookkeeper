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
package org.apache.bookkeeper.client.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Iterator;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Bookkeeper Client API ledger metadata and ledgers listing test.
 */
public class LedgerMetadataTest extends BookKeeperClusterTestCase {

    public LedgerMetadataTest() {
        super(3);
    }

    @Test
    public void testGetLedgerMetadata()
            throws Exception {

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        try (BookKeeper bkc = BookKeeper.newBuilder(conf).build();) {
            long ledgerId;
            try (WriteHandle l = bkc
                    .newCreateLedgerOp()
                    .withDigestType(DigestType.CRC32)
                    .withPassword("testPasswd".getBytes())
                    .execute()
                    .get();) {
                ledgerId = l.getId();
            }

            LedgerMetadata metadata = FutureUtils.result(bkc.getLedgerMetadata(ledgerId));
            assertEquals(ledgerId, metadata.getLedgerId());
            assertEquals(3, metadata.getEnsembleSize());
            assertEquals(2, metadata.getAckQuorumSize());
            assertEquals(2, metadata.getWriteQuorumSize());
            assertArrayEquals("testPasswd".getBytes(), metadata.getPassword());
        }

    }

    @Test
    public void testListLedgers()
            throws Exception {
        int numOfLedgers = 10;

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        try (BookKeeper bkc = BookKeeper.newBuilder(conf).build();) {
            long[] ledgerIds = new long[numOfLedgers];
            for (int i = 0; i < numOfLedgers; i++) {

                try (WriteHandle l = bkc
                        .newCreateLedgerOp()
                        .withDigestType(DigestType.CRC32)
                        .withPassword("testPasswd".getBytes())
                        .execute()
                        .get();) {
                    ledgerIds[i] = l.getId();
                }
            }

            try (ListLedgersResult result = FutureUtils.result(bkc.newListLedgersOp().execute());) {
                int count = 0;

                for (long ledgerId : result.toIterable()) {
                    assertEquals(ledgerIds[count++], ledgerId);
                }

                assertEquals("Unexpected ledgers count", numOfLedgers, count);
                try {
                    result.iterator();
                    fail("Should thrown error");
                } catch (IllegalStateException e) {
                    // ok
                }
                try {
                    result.toIterable();
                    fail("Should thrown error");
                } catch (IllegalStateException e) {
                    // ok
                }
            }

            try (ListLedgersResult result = FutureUtils.result(bkc.newListLedgersOp().execute());) {
                int count = 0;

                for (LedgersIterator iterator = result.iterator(); iterator.hasNext();) {
                    long ledgerId = iterator.next();
                    assertEquals(ledgerIds[count++], ledgerId);

                }
                assertEquals("Unexpected ledgers count", numOfLedgers, count);
                try {
                    result.iterator();
                    fail("Should thrown error");
                } catch (IllegalStateException e) {
                    // ok
                }
                try {
                    result.toIterable();
                    fail("Should thrown error");
                } catch (IllegalStateException e) {
                    // ok
                }
            }
        }

        // check closed
        {
            ListLedgersResult result = FutureUtils.result(bkc.newListLedgersOp().execute());
            result.close();
            try {
                result.toIterable();
                fail("Should thrown error");
            } catch (IllegalStateException e) {
                // ok
            }

            try {
                result.iterator();
                fail("Should thrown error");
            } catch (IllegalStateException e) {
                // ok
            }
        }

        { // iterator
            ListLedgersResult result = FutureUtils.result(bkc.newListLedgersOp().execute());
            LedgersIterator it = result.iterator();
            result.close();
            try {
                it.hasNext();
                fail("Should thrown error");
            } catch (IllegalStateException e) {
                // ok
            }

            try {
                it.next();
                fail("Should thrown error");
            } catch (IllegalStateException e) {
                // ok
            }
        }

        { // iterable
            ListLedgersResult result = FutureUtils.result(bkc.newListLedgersOp().execute());
            Iterator<Long> it = result.toIterable().iterator();
            result.close();
            try {
                it.hasNext();
                fail("Should thrown error");
            } catch (IllegalStateException e) {
                // ok
            }

            try {
                it.next();
                fail("Should thrown error");
            } catch (IllegalStateException e) {
                // ok
            }
        }
    }

}
