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
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ArraySortGroupTest {

    @Test
    public void simple() {
        long[] data = new long[] { //
                1, 2, 3, 4, //
                5, 6, 3, 1, //
                4, 8, 1, 2, //
                4, 5, 12, 10, //
                3, 3, 3, 3, //
                4, 3, 1, 2, //
                3, 3, 3, 3, //
        };

        long[] expectedSorted = new long[] { //
                1, 2, 3, 4, //
                3, 3, 3, 3, //
                3, 3, 3, 3, //
                4, 3, 1, 2, //
                4, 5, 12, 10, //
                4, 8, 1, 2, //
                5, 6, 3, 1, //
        };

        ArrayGroupSort sorter = new ArrayGroupSort(2, 4);
        sorter.sort(data);

        assertArrayEquals(expectedSorted, data);
    }

    @Test
    public void keySmallerThanTotalSize() {
        try {
            new ArrayGroupSort(3, 2);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void negativeKeySize() {
        try {
            new ArrayGroupSort(-1, 2);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void negativeTotalSize() {
        try {
            new ArrayGroupSort(1, -1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void arraySizeIsNotMultiple() {
        ArrayGroupSort sorter = new ArrayGroupSort(1, 3);

        try {
            sorter.sort(new long[] { 1, 2, 3, 4 });
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void arraySizeIsShorterThanRequired() {
        ArrayGroupSort sorter = new ArrayGroupSort(1, 3);

        try {
            sorter.sort(new long[] { 1, 2 });
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void emtpy() {
        long[] data = new long[] {};

        long[] expectedSorted = new long[] {};

        ArrayGroupSort sorter = new ArrayGroupSort(2, 4);
        sorter.sort(data);

        assertArrayEquals(expectedSorted, data);
    }

    @Test
    public void singleItem() {
        long[] data = new long[] { 1, 2, 3, 4 };
        long[] expectedSorted = new long[] { 1, 2, 3, 4 };

        ArrayGroupSort sorter = new ArrayGroupSort(2, 4);
        sorter.sort(data);

        assertArrayEquals(expectedSorted, data);
    }

    @Test
    public void twoItems() {
        long[] data = new long[] { 1, 2, 3, 4, 1, 1, 5, 5 };
        long[] expectedSorted = new long[] { 1, 1, 5, 5, 1, 2, 3, 4 };

        ArrayGroupSort sorter = new ArrayGroupSort(2, 4);
        sorter.sort(data);

        assertArrayEquals(expectedSorted, data);
    }

    @Test
    public void threeItems() {
        long[] data = new long[] { 1, 2, 3, 4, 1, 1, 5, 5, 1, 0, 2, 1 };
        long[] expectedSorted = new long[] { 1, 0, 2, 1, 1, 1, 5, 5, 1, 2, 3, 4 };

        ArrayGroupSort sorter = new ArrayGroupSort(2, 4);
        sorter.sort(data);

        assertArrayEquals(expectedSorted, data);
    }

}
