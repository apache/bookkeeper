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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Sort an array of longs, grouping the items in tuples.
 *
 * <p>Group size decides how many longs are included in the tuples and key size controls how many items to use for
 * comparison.
 */
public class ArrayGroupSort {

    private final int keySize;
    private final int groupSize;

    public ArrayGroupSort(int keySize, int groupSize) {
        checkArgument(keySize > 0);
        checkArgument(groupSize > 0);
        checkArgument(keySize <= groupSize, "keySize need to be less or equal the groupSize");
        this.keySize = keySize;
        this.groupSize = groupSize;
    }

    public void sort(long[] array) {
        sort(array, 0, array.length);
    }

    public void sort(long[] array, int offset, int length) {
        checkArgument(length % groupSize == 0, "Array length must be multiple of groupSize");
        quickSort(array, offset, (length + offset - groupSize));
    }

    ////// Private

    private void quickSort(long[] array, int low, int high) {
        if (low < high) {
            int pivotIdx = partition(array, low, high);
            quickSort(array, low, pivotIdx - groupSize);
            quickSort(array, pivotIdx + groupSize, high);
        }
    }

    private int partition(long[] array, int low, int high) {
        int pivotIdx = high;
        int i = low;

        for (int j = low; j < high; j += groupSize) {
            if (isLess(array, j, pivotIdx)) {
                swap(array, j, i);
                i += groupSize;
            }
        }

        swap(array, i, high);
        return i;
    }

    private void swap(long[] array, int a, int b) {
        long tmp;
        for (int k = 0; k < groupSize; k++) {
            tmp = array[a + k];
            array[a + k] = array[b + k];
            array[b + k] = tmp;
        }
    }

    private boolean isLess(long[] array, int idx1, int idx2) {
        for (int i = 0; i < keySize; i++) {
            long k1 = array[idx1 + i];
            long k2 = array[idx2 + i];
            if (k1 < k2) {
                return true;
            } else if (k1 > k2) {
                return false;
            }
        }

        return false;
    }
}
