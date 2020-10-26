/**
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
package org.apache.bookkeeper.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;

import org.junit.Test;

/**
 * Testsuite for AvailabilityOfEntriesOfLedger.
 */
public class AvailabilityOfEntriesOfLedgerTest {
    @Test
    public void testWithItrConstructor() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 1, 2},
                { 1, 2, 3, 5, 6, 7, 8 },
                { 0, 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                {0},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            assertEquals("Expected total number of entries", tempArray.length,
                    availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(tempArray[j] + " is supposed to be available",
                        availabilityOfEntriesOfLedger.isEntryAvailable(tempArray[j]));
            }
        }
    }

    @Test
    public void testWithItrConstructorWithDuplicates() {
        long[][] arrays = {
                { 1, 2, 2, 3 },
                { 1, 2, 3, 5, 5, 6, 7, 8, 8 },
                { 1, 1, 5, 5 },
                { 3, 3 },
                { 1, 1, 2, 4, 5, 8, 9, 9, 9, 9, 9 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 17, 100, 1000, 1000, 1001, 10000, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            Set<Long> tempSet = new HashSet<Long>();
            for (int k = 0; k < tempArray.length; k++) {
                tempSet.add(tempArray[k]);
            }
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            assertEquals("Expected total number of entries", tempSet.size(),
                    availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(tempArray[j] + " is supposed to be available",
                        availabilityOfEntriesOfLedger.isEntryAvailable(tempArray[j]));
            }
        }
    }

    @Test
    public void testSerializeDeserialize() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 0, 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                { },
                { 0 },
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedgerUsingSer = new AvailabilityOfEntriesOfLedger(
                    serializedState);
            assertEquals("Expected total number of entries", tempArray.length,
                    availabilityOfEntriesOfLedgerUsingSer.getTotalNumOfAvailableEntries());
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(tempArray[j] + " is supposed to be available",
                        availabilityOfEntriesOfLedgerUsingSer.isEntryAvailable(tempArray[j]));
            }
        }
    }

    @Test
    public void testSerializeDeserializeWithItrConstructorWithDuplicates() {
        long[][] arrays = {
                { 1, 2, 2, 3 },
                { 1, 2, 3, 5, 5, 6, 7, 8, 8 },
                { 1, 1, 5, 5 },
                { 3, 3 },
                { 1, 1, 2, 4, 5, 8, 9, 9, 9, 9, 9 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 17, 100, 1000, 1000, 1001, 10000, 10000, 20000, 20001 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            Set<Long> tempSet = new HashSet<Long>();
            for (int k = 0; k < tempArray.length; k++) {
                tempSet.add(tempArray[k]);
            }
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);
            byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedgerUsingSer = new AvailabilityOfEntriesOfLedger(
                    serializedState);
            assertEquals("Expected total number of entries", tempSet.size(),
                    availabilityOfEntriesOfLedgerUsingSer.getTotalNumOfAvailableEntries());
            for (int j = 0; j < tempArray.length; j++) {
                assertTrue(tempArray[j] + " is supposed to be available",
                        availabilityOfEntriesOfLedgerUsingSer.isEntryAvailable(tempArray[j]));
            }
        }
    }

    @Test
    public void testNonExistingEntries() {
        long[][] arrays = {
                { 0, 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };
        /**
         * corresponding non-existing entries for 'arrays'
         */
        long[][] nonExistingEntries = {
                { 3 },
                { 0, 4, 9, 100, 101 },
                { 0, 2, 3, 6, 9 },
                { 0, 1, 2, 4, 5, 6 },
                { 0, 3, 9, 10, 11, 100, 1000 },
                { 0, 1, 2, 3, 4, 5 },
                { 4, 18, 1002, 19999, 20003 }
        };
        for (int i = 0; i < arrays.length; i++) {
            long[] tempArray = arrays[i];
            long[] nonExistingElementsTempArray = nonExistingEntries[i];
            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(tempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);

            for (int j = 0; j < nonExistingElementsTempArray.length; j++) {
                assertFalse(nonExistingElementsTempArray[j] + " is not supposed to be available",
                        availabilityOfEntriesOfLedger.isEntryAvailable(nonExistingElementsTempArray[j]));
            }
        }
    }

    @Test
    public void testGetUnavailableEntries() {
        /*
         * AvailabilityOfEntriesOfLedger is going to be created with this
         * entries. It is equivalent to considering that Bookie has these
         * entries.
         */
        long[][] availableEntries = {
                { 1, 2},
                { 0, 1, 2 },
                { 1, 2, 3, 5, 6, 7, 8 },
                { 1, 5 },
                { 3 },
                { 1, 2, 4, 5, 7, 8 },
                {},
                { 1, 2, 3, 5, 6, 11, 12, 13, 14, 15, 16, 17, 100, 1000, 1001, 10000, 20000, 20001 }
        };

        /*
         * getUnavailableEntries method is going to be called with these entries
         * as expected to contain.
         */
        long[][] expectedToContainEntries = {
                { 1, 2},
                { 0, 1, 2, 3, 5 },
                { 1, 2, 5, 7, 8 },
                { 2, 7 },
                { 3 },
                { 1, 5, 7, 8, 9, 10 },
                { 0, 1, 2, 3, 4, 5 },
                { 4, 18, 1002, 19999, 20003 }
        };

        /*
         * Considering what AvailabilityOfEntriesOfLedger contains
         * (availableEntries), what it is expected to contain
         * (expectedToContainEntries), following are the entries which are
         * supposed to be reported as unavailable (unavailableEntries).
         */
        long[][] unavailableEntries = {
                { },
                { 3, 5 },
                { },
                { 2, 7 },
                { },
                { 9, 10 },
                { 0, 1, 2, 3, 4, 5 },
                { 4, 18, 1002, 19999, 20003 }
        };

        for (int i = 0; i < availableEntries.length; i++) {
            long[] availableEntriesTempArray = availableEntries[i];
            long[] expectedToContainEntriesTempArray = expectedToContainEntries[i];
            long[] unavailableEntriesTempArray = unavailableEntries[i];
            List<Long> unavailableEntriesTempList = new ArrayList<Long>();
            for (int j = 0; j < unavailableEntriesTempArray.length; j++) {
                unavailableEntriesTempList.add(unavailableEntriesTempArray[j]);
            }

            PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(availableEntriesTempArray).iterator();
            AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(
                    primitiveIterator);

            long startEntryId;
            long lastEntryId;
            if (expectedToContainEntriesTempArray[0] == 0) {
                startEntryId = expectedToContainEntriesTempArray[0];
                lastEntryId = expectedToContainEntriesTempArray[expectedToContainEntriesTempArray.length - 1];
            } else {
                startEntryId = expectedToContainEntriesTempArray[0] - 1;
                lastEntryId = expectedToContainEntriesTempArray[expectedToContainEntriesTempArray.length - 1] + 1;
            }
            BitSet expectedToContainEntriesBitSet = new BitSet((int) (lastEntryId - startEntryId + 1));
            for (int ind = 0; ind < expectedToContainEntriesTempArray.length; ind++) {
                int entryId = (int) expectedToContainEntriesTempArray[ind];
                expectedToContainEntriesBitSet.set(entryId - (int) startEntryId);
            }

            List<Long> actualUnavailableEntries = availabilityOfEntriesOfLedger.getUnavailableEntries(startEntryId,
                    lastEntryId, expectedToContainEntriesBitSet);
            assertEquals("Unavailable Entries", unavailableEntriesTempList, actualUnavailableEntries);
        }
    }

    @Test
    public void testEmptyAvailabilityOfEntriesOfLedger() {
        AvailabilityOfEntriesOfLedger emptyOne = AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER;
        assertEquals("expected totalNumOfAvailableEntries", 0, emptyOne.getTotalNumOfAvailableEntries());
        assertFalse("empty one is not supposed to contain any entry", emptyOne.isEntryAvailable(100L));
        long startEntryId = 100;
        long lastEntryId = 105;
        BitSet bitSetOfAvailability = new BitSet((int) (lastEntryId - startEntryId + 1));
        for (int i = 0; i < bitSetOfAvailability.length(); i++) {
            if ((i % 2) == 0) {
                bitSetOfAvailability.set(i);
            }
        }
        List<Long> unavailableEntries = emptyOne.getUnavailableEntries(startEntryId, lastEntryId, bitSetOfAvailability);
        assertEquals("Num of unavailable entries", bitSetOfAvailability.cardinality(), unavailableEntries.size());
        for (int i = 0; i < bitSetOfAvailability.length(); i++) {
            long entryId = startEntryId + i;
            if (bitSetOfAvailability.get(i)) {
                assertTrue("Unavailable entry", unavailableEntries.contains(Long.valueOf(entryId)));
            }
        }
    }
}
