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
package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import lombok.experimental.UtilityClass;

/**
 * Sort an array of longs, grouping the items in tuples.
 *
 * <p>Group size decides how many longs are included in the tuples and key size controls how many items to use for
 * comparison.
 */
@UtilityClass
public class ArrayGroupSort {

    private static final int INSERTION_SORT_THRESHOLD = 100;

    private static final int GROUP_SIZE = 4;

    public void sort(long[] array) {
        sort(array, 0, array.length);
    }

    public static void sort(long[] array, int offset, int length) {
        checkArgument(length % GROUP_SIZE == 0, "Array length must be multiple of 4");
        quickSort(array, offset, (length + offset - GROUP_SIZE));
    }

    ////// Private

    private static void quickSort(long[] array, int low, int high) {
        if (low >= high) {
            return;
        }

        if (high - low < INSERTION_SORT_THRESHOLD) {
            insertionSort(array, low, high);
            return;
        }

        int pivotIdx = partition(array, low, high);
        quickSort(array, low, pivotIdx - GROUP_SIZE);
        quickSort(array, pivotIdx + GROUP_SIZE, high);
    }

    private static int alignGroup(int count) {
        return count - (count % GROUP_SIZE);
    }

    private static int partition(long[] array, int low, int high) {
        int mid = low + alignGroup((high - low) / 2);
        swap(array, mid, high);

        int i = low;

        for (int j = low; j < high; j += GROUP_SIZE) {
            if (isLess(array, j, high)) {
                swap(array, j, i);
                i += GROUP_SIZE;
            }
        }

        swap(array, i, high);
        return i;
    }

    private static void swap(long[] array, int a, int b) {
        long tmp0 = array[a];
        long tmp1 = array[a + 1];
        long tmp2 = array[a + 2];
        long tmp3 = array[a + 3];

        array[a] = array[b];
        array[a + 1] = array[b + 1];
        array[a + 2] = array[b + 2];
        array[a + 3] = array[b + 3];

        array[b] = tmp0;
        array[b + 1] = tmp1;
        array[b + 2] = tmp2;
        array[b + 3] = tmp3;
    }

    private static boolean isLess(long[] array, int a, int b) {
        long a0 = array[a];
        long b0 = array[b];

        if (a0 < b0) {
            return true;
        } else if (a0 > b0) {
            return false;
        }

        return array[a + 1] < array[b + 1];
    }

    private static void insertionSort(long[] a, int low, int high) {
        for (int i = low + GROUP_SIZE; i <= high; i += GROUP_SIZE) {
            int j = i;
            while (j > 0 && isLess(a, j, j - GROUP_SIZE)) {
                swap(a, j, j - GROUP_SIZE);
                j -= GROUP_SIZE;
            }
        }
    }
}
