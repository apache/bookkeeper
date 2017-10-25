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
package org.apache.bookkeeper.util.collections;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.bookkeeper.util.collections.ArrayUtils2.*;

import org.junit.Test;

public class ArrayUtils2Test {

    @Test
    public void testMoveAndShift() {
        int[] array = {1,2,3,4,5};
        moveAndShift(array, 3, 1);
        assertTrue(Arrays.equals(array, new int[]{1,4,2,3,5}));

        array = new int[]{1,2,3,4,5};
        moveAndShift(array, 1, 3);
        assertTrue(Arrays.equals(array, new int[]{1,3,4,2,5}));

        array = new int[]{1,2,3,4,5};
        moveAndShift(array, 0, 4);
        assertTrue(Arrays.equals(array, new int[]{2,3,4,5,1}));

        array = new int[]{1,2,3,4,5};
        moveAndShift(array, 0, 0);
        assertTrue(Arrays.equals(array, new int[]{1,2,3,4,5}));

        array = new int[]{1,2,3,4,5};
        moveAndShift(array, 4, 4);
        assertTrue(Arrays.equals(array, new int[]{1,2,3,4,5}));
    }

    @Test
    public void testShuffleWithMask() {
        int mask = 0xE1 << 16;
        int maskBits = 0xFF << 16;
        boolean shuffleOccurred = false;
        for (int i = 0; i < 100; i++) {
            int[] array = {1, 2, 3 & mask, 4 & mask, 5 & mask, 6};
            shuffleWithMask(array, mask, maskBits);
            assertEquals(array[0], 1);
            assertEquals(array[1], 2);
            assertEquals(array[5], 6);

            if (array[3] == (3 & mask)
                || array[4] == (3 & mask)) {
                shuffleOccurred = true;
            } else if (array[2] != (3 & mask)) {
                fail("3 not found");
            }

            if (array[2] == (4 & mask)
                || array[4] == (4 & mask)) {
                shuffleOccurred = true;
            } else if (array[3] != (4 & mask)) {
                fail("4 not found");
            }

            if (array[2] == (5 & mask)
                || array[3] == (5 & mask)) {
                shuffleOccurred = true;
            } else if (array[4] != (5 & mask)) {
                fail("5 not found");
            }
        }
        assertTrue(shuffleOccurred);

        // at start of array
        shuffleOccurred = false;
        for (int i = 0; i < 100; i++) {
            int[] array = {1 & mask, 2 & mask, 3 & mask, 4, 5, 6};
            shuffleWithMask(array, mask, maskBits);
            assertEquals(array[3], 4);
            assertEquals(array[4], 5);
            assertEquals(array[5], 6);

            if (array[1] == (1 & mask)
                || array[2] == (1 & mask)) {
                shuffleOccurred = true;
            } else if (array[0] != (1 & mask)) {
                fail("1 not found");
            }

            if (array[0] == (2 & mask)
                || array[2] == (2 & mask)) {
                shuffleOccurred = true;
            } else if (array[1] != (2 & mask)) {
                fail("2 not found");
            }

            if (array[0] == (3 & mask)
                || array[1] == (3 & mask)) {
                shuffleOccurred = true;
            } else if (array[2] != (3 & mask)) {
                fail("3 not found");
            }
        }
        assertTrue(shuffleOccurred);

        // at end of array
        shuffleOccurred = false;
        for (int i = 0; i < 100; i++) {
            int[] array = {1, 2, 3, 4 & mask, 5 & mask, 6 & mask};
            shuffleWithMask(array, mask, maskBits);
            assertEquals(array[0], 1);
            assertEquals(array[1], 2);
            assertEquals(array[2], 3);

            if (array[4] == (4 & mask)
                || array[5] == (4 & mask)) {
                shuffleOccurred = true;
            } else if (array[3] != (4 & mask)) {
                fail("4 not found");
            }

            if (array[3] == (5 & mask)
                || array[5] == (5 & mask)) {
                shuffleOccurred = true;
            } else if (array[4] != (5 & mask)) {
                fail("5 not found");
            }

            if (array[3] == (6 & mask)
                || array[4] == (6 & mask)) {
                shuffleOccurred = true;
            } else if (array[5] != (6 & mask)) {
                fail("6 not found");
            }
        }
        assertTrue(shuffleOccurred);
    }

}
