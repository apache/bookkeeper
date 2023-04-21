/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java {@link java.util.stream.Stream} related operations.
 */
public class StreamUtil {

    public static <T1, T2, R> Stream<R> zip(Stream<? extends T1> a,
                                            Stream<? extends T2> b,
                                            BiFunction<? super T1, ? super T2, ? extends R> zipper) {
        Objects.requireNonNull(zipper);
        Spliterator<? extends T1> aSpliterator = Objects.requireNonNull(a).spliterator();
        Spliterator<? extends T2> bSpliterator = Objects.requireNonNull(b).spliterator();

        // Zipping looses DISTINCT and SORTED characteristics
        int characteristics = aSpliterator.characteristics() & bSpliterator.characteristics()
            & ~(Spliterator.DISTINCT | Spliterator.SORTED);

        long zipSize = ((characteristics & Spliterator.SIZED) != 0)
            ? Math.min(aSpliterator.getExactSizeIfKnown(), bSpliterator.getExactSizeIfKnown())
            : -1;

        Iterator<T1> aIterator = Spliterators.iterator(aSpliterator);
        Iterator<T2> bIterator = Spliterators.iterator(bSpliterator);
        Iterator<R> cIterator = new Iterator<R>() {
            @Override
            public boolean hasNext() {
                return aIterator.hasNext() && bIterator.hasNext();
            }

            @Override
            public R next() {
                return zipper.apply(aIterator.next(), bIterator.next());
            }
        };

        Spliterator<R> split = Spliterators.spliterator(cIterator, zipSize, characteristics);
        return (a.isParallel() || b.isParallel())
            ? StreamSupport.stream(split, true)
            : StreamSupport.stream(split, false);
    }


}
