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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Unit tests for journal index computation in BookieImpl.
 *
 * <p>Verifies that hash-based journal selection provides a near-even
 * distribution of ledgers across journals for various ID patterns.
 */
@RunWith(Parameterized.class)
public class JournalIndexComputationTest {

    private static final double TOLERANCE = 0.10;
    private static final int NUM_LEDGERS = 10000;

    private final int numJournals;
    private final int stride;

    public JournalIndexComputationTest(int numJournals, int stride, String description) {
        this.numJournals = numJournals;
        this.stride = stride;
    }

    @Parameterized.Parameters(name = "{2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            // Sequential IDs (stride=1) with various journal counts
            {1, 1, "1 journal"},
            {3, 1, "3 journals, sequential"},
            {4, 1, "4 journals, sequential"},
            {7, 1, "7 journals (prime), sequential"},
            {8, 1, "8 journals, sequential"},
            {15, 1, "15 journals, sequential"},

            // Strided IDs
            {4, 2, "4 journals, stride=2"},
            {4, 3, "4 journals, stride=3"},
            {4, 4, "4 journals, stride=4 (worst case without hash)"},
            {8, 8, "8 journals, stride=8 (worst case without hash)"},
            {4, 1024, "4 journals, stride=1024"},
            {8, 1024, "8 journals, stride=1024"},
        });
    }

    // Tests that ledger IDs with the given stride distribute evenly across journals
    // when hash-based selection is enabled.
    @Test
    public void testEvenDistributionWithHashing() {
        int[] counts = new int[numJournals];

        for (int i = 0; i < NUM_LEDGERS; i++) {
            long ledgerId = (long) i * stride;
            int journalIndex = BookieImpl.computeJournalIndex(ledgerId, numJournals, true);
            assertTrue("Index out of bounds", journalIndex >= 0 && journalIndex < numJournals);
            counts[journalIndex]++;

            // Verify determinism
            assertEquals("Hash not deterministic", journalIndex,
                BookieImpl.computeJournalIndex(ledgerId, numJournals, true));
        }

        assertNearEvenDistribution(counts);
    }

    // Tests that without hashing, journal index is computed via simple modulo
    @Test
    public void testSimpleModuloWithoutHashing() {
        for (int i = 0; i < NUM_LEDGERS; i++) {
            long ledgerId = (long) i * stride;
            int journalIndex = BookieImpl.computeJournalIndex(ledgerId, numJournals, false);
            assertTrue("Index out of bounds", journalIndex >= 0 && journalIndex < numJournals);

            // Verify determinism
            assertEquals("Result not deterministic", journalIndex,
                BookieImpl.computeJournalIndex(ledgerId, numJournals, false));

            // Verify it matches simple modulo for non-negative IDs
            int expectedIndex = (int) (ledgerId % numJournals);
            assertEquals("Should match simple modulo", expectedIndex, journalIndex);
        }
    }

    // Tests that negative and very large ledger IDs produce valid indices for both
    // hash and non-hash modes.
    @Test
    public void testEdgeCaseLedgerIds() {
        for (boolean useHash : new boolean[] {true, false}) {
            String mode = useHash ? "hash" : "modulo";

            for (long ledgerId = -100; ledgerId < 0; ledgerId++) {
                int idx = BookieImpl.computeJournalIndex(ledgerId, numJournals, useHash);
                assertTrue("Negative ID produced invalid index (" + mode + ")",
                        idx >= 0 && idx < numJournals);
            }

            for (int i = 0; i < 100; i++) {
                long ledgerId = Long.MAX_VALUE - i;
                int idx = BookieImpl.computeJournalIndex(ledgerId, numJournals, useHash);
                assertTrue("Large ID produced invalid index (" + mode + ")",
                        idx >= 0 && idx < numJournals);
            }
        }
    }

    private void assertNearEvenDistribution(int[] counts) {
        int total = Arrays.stream(counts).sum();
        int expected = total / counts.length;
        int maxDeviation = (int) (expected * TOLERANCE);

        for (int i = 0; i < counts.length; i++) {
            int deviation = Math.abs(counts[i] - expected);
            assertTrue(String.format("journal %d: %d (expected ~%d, max deviation %d)",
                            i, counts[i], expected, maxDeviation),
                    deviation <= maxDeviation);
        }
    }
}
