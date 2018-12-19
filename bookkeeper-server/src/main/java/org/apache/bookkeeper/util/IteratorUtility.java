/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.bookkeeper.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.PrimitiveIterator.OfLong;
import java.util.function.ToLongFunction;

/**
 * Utility class to merge iterators.
 */
public class IteratorUtility {

    private static final long INVALID_ELEMENT = -1;

    public static OfLong mergePrimitiveLongIterator(OfLong iter1, OfLong iter2) {
        return new PrimitiveIterator.OfLong() {
            private long curIter1Element = INVALID_ELEMENT;
            private long curIter2Element = INVALID_ELEMENT;
            private boolean hasToPreFetch = true;

            @Override
            public boolean hasNext() {
                if (hasToPreFetch) {
                    if (curIter1Element == INVALID_ELEMENT) {
                        curIter1Element = iter1.hasNext() ? iter1.nextLong() : INVALID_ELEMENT;
                    }
                    if (curIter2Element == INVALID_ELEMENT) {
                        curIter2Element = iter2.hasNext() ? iter2.nextLong() : INVALID_ELEMENT;
                    }
                }
                hasToPreFetch = false;
                return (curIter1Element != INVALID_ELEMENT || curIter2Element != INVALID_ELEMENT);
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                long returnEntryId = INVALID_ELEMENT;
                if (curIter1Element != INVALID_ELEMENT && curIter2Element != INVALID_ELEMENT) {
                    if (curIter1Element == curIter2Element) {
                        returnEntryId = curIter1Element;
                        curIter1Element = INVALID_ELEMENT;
                        curIter2Element = INVALID_ELEMENT;
                    } else if (curIter1Element < curIter2Element) {
                        returnEntryId = curIter1Element;
                        curIter1Element = INVALID_ELEMENT;
                    } else {
                        returnEntryId = curIter2Element;
                        curIter2Element = INVALID_ELEMENT;
                    }
                } else if (curIter1Element != INVALID_ELEMENT) {
                    returnEntryId = curIter1Element;
                    curIter1Element = INVALID_ELEMENT;
                } else {
                    returnEntryId = curIter2Element;
                    curIter2Element = INVALID_ELEMENT;
                }
                hasToPreFetch = true;
                return returnEntryId;
            }
        };
    }

    public static <T> OfLong mergeIteratorsForPrimitiveLongIterator(Iterator<T> iter1, Iterator<T> iter2,
            Comparator<T> comparator, ToLongFunction<T> function) {
        return new PrimitiveIterator.OfLong() {
            private T curIter1Entry = null;
            private T curIter2Entry = null;
            private boolean hasToPreFetch = true;

            @Override
            public boolean hasNext() {
                if (hasToPreFetch) {
                    if (curIter1Entry == null) {
                        curIter1Entry = iter1.hasNext() ? iter1.next() : null;
                    }
                    if (curIter2Entry == null) {
                        curIter2Entry = iter2.hasNext() ? iter2.next() : null;
                    }
                }
                hasToPreFetch = false;
                return (curIter1Entry != null || curIter2Entry != null);
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                T returnEntry = null;
                if (curIter1Entry != null && curIter2Entry != null) {
                    int compareValue = comparator.compare(curIter1Entry, curIter2Entry);
                    if (compareValue == 0) {
                        returnEntry = curIter1Entry;
                        curIter1Entry = null;
                        curIter2Entry = null;
                    } else if (compareValue < 0) {
                        returnEntry = curIter1Entry;
                        curIter1Entry = null;
                    } else {
                        returnEntry = curIter2Entry;
                        curIter2Entry = null;
                    }
                } else if (curIter1Entry != null) {
                    returnEntry = curIter1Entry;
                    curIter1Entry = null;
                } else {
                    returnEntry = curIter2Entry;
                    curIter2Entry = null;
                }
                hasToPreFetch = true;
                return function.applyAsLong(returnEntry);
            }
        };
    }
}
