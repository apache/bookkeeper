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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.ArrayGroupSort;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 3, time = 10)
public class GroupSortBenchmark {

    private static final int N = 10_000;

    @State(Scope.Benchmark)
    public static class TestState {

        private final long[] randomItems = new long[N * 4];
        private final long[] sortedItems;
        private final long[] reverseSortedItems = new long[N * 4];
        private final long[] groupSortedItems;
        private final long[] reverseGroupSortedItems = new long[N * 4];

        private long[] items;

        public TestState() {
            Random r = new Random();
            for (int i = 0; i < (N * 4); i++) {
                randomItems[i] = r.nextLong();
            }

            groupSortedItems = Arrays.copyOf(randomItems, randomItems.length);
            ArrayGroupSort.sort(groupSortedItems);
            for (int i = 0; i < (N * 4); i += 4) {
                reverseGroupSortedItems[i] = groupSortedItems[(N - 1) * 4 - i];
                reverseGroupSortedItems[i + 1] = groupSortedItems[(N - 1) * 4 - i + 1];
                reverseGroupSortedItems[i + 2] = groupSortedItems[(N - 1) * 4 - i + 2];
                reverseGroupSortedItems[i + 3] = groupSortedItems[(N - 1) * 4 - i + 3];
            }

            sortedItems = Arrays.copyOf(randomItems, randomItems.length);
            Arrays.sort(sortedItems);
            for (int i = 0; i < (N * 4); i++) {
                reverseSortedItems[i] = sortedItems[N * 4 - 1 - i];
            }
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            items = Arrays.copyOf(randomItems, randomItems.length);
        }
    }

    @Benchmark
    public void randomGroupSort(GroupSortBenchmark.TestState s) {
        ArrayGroupSort.sort(s.items);
    }


    @Benchmark
    public void randomArraySort(GroupSortBenchmark.TestState s) {
        Arrays.sort(s.items);
    }


    @Benchmark
    public void preSortedGroupSort(GroupSortBenchmark.TestState s) {
        ArrayGroupSort.sort(s.groupSortedItems);
    }


    @Benchmark
    public void preSortedArraySort(GroupSortBenchmark.TestState s) {
        Arrays.sort(s.sortedItems);
    }

    @Benchmark
    public void reverseSortedGroupSort(GroupSortBenchmark.TestState s) {
        ArrayGroupSort.sort(s.reverseGroupSortedItems);
    }


    @Benchmark
    public void reverseSortedArraySort(GroupSortBenchmark.TestState s) {
        Arrays.sort(s.reverseSortedItems);
    }
}
