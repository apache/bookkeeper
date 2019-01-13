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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.beust.jcommander.internal.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * Tests of the main BookKeeper client.
 */
@Slf4j
public class BookieStickyReadsTest extends BookKeeperClusterTestCase {

    private static final int NUM_BOOKIES = 3;

    private static final String READ_ENTRY_REQUEST_METRIC = "bookkeeper_server.READ_ENTRY_REQUEST";

    public BookieStickyReadsTest() {
        super(NUM_BOOKIES);
    }

    @Test
    public void testNormalReads() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);

        // Default should already be set to false
        // conf.setStickyReadsEnabled(false);

        writeAndReadEntries(conf, 3, 3, 3);

        // All bookies should have received at least some read request
        getBookieReadRequestStats().values().forEach(readRequests -> assertTrue(readRequests > 0));
    }

    @Test
    public void testStickyFlagWithStriping() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        conf.setStickyReadsEnabled(true);

        writeAndReadEntries(conf, 3, 2, 2);

        // All bookies should have received at least some read request since we
        // don't enable sticky reads when striping is enabled
        getBookieReadRequestStats().values().forEach(readRequests -> assertTrue(readRequests > 0));
    }

    @Test
    public void stickyReadsWithNoFailures() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        conf.setStickyReadsEnabled(true);

        writeAndReadEntries(conf, 3, 3, 3);

        // All read requests should have been made to a single bookie
        Map<Integer, Long> stats = getBookieReadRequestStats();
        boolean foundBookieWithRequests = false;
        for (long readRequests : stats.values()) {
            if (readRequests > 0) {
                assertFalse("Another bookie already had received requests", foundBookieWithRequests);
                foundBookieWithRequests = true;
            }
        }
    }

    @Test
    public void stickyReadsWithFailures() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        conf.setStickyReadsEnabled(true);

        @Cleanup
        BookKeeper bkc = new BookKeeper(conf);

        final int n = 10;
        long ledgerId;

        try (WriteHandle wh = bkc.newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(3)
                .withPassword("".getBytes())
                .execute()
                .join()) {
            ledgerId = wh.getId();

            for (int i = 0; i < n; i++) {
                wh.append(("entry-" + i).getBytes());
            }
        }

        @Cleanup
        ReadHandle rh = bkc.newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword("".getBytes())
                .execute()
                .join();

        // Read 1 entry and detect which bookie was being used
        @Cleanup
        LedgerEntries entry0 = rh.read(0, 0);
        assertArrayEquals("entry-0".getBytes(), entry0.getEntry(0).getEntryBytes());

        // All read requests should have been made to a single bookie
        int bookieWithRequests = -1;
        for (int i = 0; i < NUM_BOOKIES; i++) {
            long requests = getStatsProvider(i).getOpStatsLogger(READ_ENTRY_REQUEST_METRIC)
                    .getSuccessCount();

            log.info("Bookie {} --- requests: {}", i, requests);

            if (requests > 0) {
                assertTrue("Another bookie already had received requests", bookieWithRequests == -1);
                bookieWithRequests = i;
            }
        }

        // Suspend the sticky bookie. Reads should now go to a different sticky
        // bookie
        bs.get(bookieWithRequests).suspendProcessing();

        for (int i = 0; i < n; i++) {
            @Cleanup
            LedgerEntries entries = rh.read(i, i);

            assertArrayEquals(("entry-" + i).getBytes(), entries.getEntry(i).getEntryBytes());
        }

        // At this point, we should have 1 bookie with 1 request (the initial
        // request), and a second bookie with 10 requests. The 3rd bookie should
        // have no requests
        List<Long> requestCounts = Lists.newArrayList(getBookieReadRequestStats().values());
        Collections.sort(requestCounts);

        assertEquals(0, requestCounts.get(0).longValue());
        assertEquals(1, requestCounts.get(1).longValue());
        assertEquals(10, requestCounts.get(2).longValue());
    }

    private Map<Integer, Long> getBookieReadRequestStats() throws Exception {
        Map<Integer, Long> stats = new TreeMap<>();
        for (int i = 0; i < NUM_BOOKIES; i++) {
            stats.put(i, getStatsProvider(i).getOpStatsLogger(READ_ENTRY_REQUEST_METRIC)
                    .getSuccessCount());
        }

        return stats;
    }

    private void writeAndReadEntries(ClientConfiguration conf, int ensembleSize, int writeQuorum, int ackQuorum)
            throws Exception {
        @Cleanup
        BookKeeper bkc = new BookKeeper(conf);

        @Cleanup
        WriteHandle wh = bkc.newCreateLedgerOp()
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorum)
                .withAckQuorumSize(ackQuorum)
                .withPassword("".getBytes())
                .execute()
                .join();

        final int n = 10;

        for (int i = 0; i < n; i++) {
            wh.append(("entry-" + i).getBytes());
        }

        for (int i = 0; i < n; i++) {
            @Cleanup
            LedgerEntries entries = wh.read(i, i);

            assertArrayEquals(("entry-" + i).getBytes(), entries.getEntry(i).getEntryBytes());
        }
    }
}
