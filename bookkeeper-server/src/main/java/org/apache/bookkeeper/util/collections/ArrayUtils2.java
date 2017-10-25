/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.util.collections;

import java.util.Random;
import org.apache.commons.lang3.ArrayUtils;

public class ArrayUtils2 {
    static Random r = new Random();

    public static int[] addMissingIndices(int[] array, int maxIndex) {
        if (array.length < maxIndex) {
            int[] newArray = new int[maxIndex];
            System.arraycopy(array, 0,
                             newArray, 0, array.length);
            for (int i = 0, j = array.length;
                 i < maxIndex && j < maxIndex; i++) {
                if (!ArrayUtils.contains(array, i)) {
                    newArray[j] = i;
                    j++;
                }
            }
            return newArray;
        } else {
            return array;
        }
    }

    /**
     * Shuffle all the entries of an array that matches a mask.
     * It assumes all entries with the same mask are contiguous in the array.
     */
    public static void shuffleWithMask(int[] array, int mask, int bits) {
        int first = -1;
        int last = -1;
        for (int i = 0; i < array.length; i++) {
            if ((array[i] & bits) == mask) {
                if (first == -1) {
                    first = i;
                }
                last = i;
            }
        }
        if (first != -1) {
            for (int i = last + 1; i > first; i--) {
                int swapWith = r.nextInt(i);
                int tmp = array[i];
                array[i] = array[swapWith];
                array[swapWith] = tmp;
            }
        }
    }

    public static void moveAndShift(int[] array, int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            int tmp = array[fromIndex];
            for (int i = fromIndex; i > toIndex; i--) {
                array[i] = array[i-1];
            }
            array[toIndex] = tmp;
        } else if (fromIndex < toIndex) {
            int tmp = array[fromIndex];
            for (int i = fromIndex; i < toIndex; i++) {
                array[i] = array[i+1];
            }
            array[toIndex] = tmp;
        }
    }
}
