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

import java.util.Arrays;
import java.util.HashSet;
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
}
