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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Iterator;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.api.Test;

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

                assertEquals(numOfLedgers, count, "Unexpected ledgers count");
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
                assertEquals(numOfLedgers, count, "Unexpected ledgers count");
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

    /**
     * Unlike {@link #testListLedgers()} which only uses 10 ledgers within a single ZK range,
     * this test creates 10010 ledgers to verify cross-range iteration.
     *
     * <p>The HierarchicalLedgerManager stores ledgers in a ZK tree where each range (znode)
     * holds at most 10,000 leaf nodes. This limit is determined by the 4-digit last level of
     * the ledger ID path (0000-9999), as defined in:
     * <ul>
     *   <li>{@code LegacyHierarchicalLedgerManager} — 2-4-4 split,
     *       path: {root}/{level1}/{level2}/L{level3}</li>
     *   <li>{@code LongHierarchicalLedgerManager} — 3-4-4-4-4 split,
     *       path: {root}/{level0}/{level1}/{level2}/{level3}/L{level4}</li>
     * </ul>
     *
     * <p>By using numOfLedgers = 10010 (> 10000), ledgers will span across multiple ranges,
     * exercising the SyncLedgerIterator's ability to advance from one exhausted range to the
     * next via LedgerRangeIterator.
     */
    @Test
    public void testListLedgersLargeScale()
            throws Exception {
        // 10010 > 10000 (max ledgers per ZK range, determined by 4-digit last level 0000-9999),
        // ensuring cross-range iteration is covered
        int numOfLedgers = 10010;

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

                assertEquals(numOfLedgers, count, "Unexpected ledgers count");
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
                assertEquals(numOfLedgers, count, "Unexpected ledgers count");
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
