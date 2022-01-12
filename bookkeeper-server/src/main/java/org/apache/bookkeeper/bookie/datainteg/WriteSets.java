/*
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
 */

package org.apache.bookkeeper.bookie.datainteg;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pregenerate the write sets. RoundRobinDistributionSchedule should really be doing this also.
 */
class WriteSets {
    private static final Logger log = LoggerFactory.getLogger(WriteSets.class);
    private final int ensembleSize;
    private final ImmutableList<ImmutableList<Integer>> sets;

    WriteSets(List<Integer> preferredOrder,
              int ensembleSize,
              int writeQuorumSize) {
        this.ensembleSize = ensembleSize;

        ImmutableList.Builder<ImmutableList<Integer>> builder =
            new ImmutableList.Builder<ImmutableList<Integer>>();
        for (int i = 0; i < ensembleSize; i++) {
            builder.add(generateWriteSet(preferredOrder, ensembleSize, writeQuorumSize, i));
        }
        sets = builder.build();
    }

    WriteSets(int ensembleSize, int writeQuorumSize) {
        this(IntStream.range(0, ensembleSize).boxed().collect(Collectors.toList()),
             ensembleSize, writeQuorumSize);
    }

    ImmutableList<Integer> getForEntry(long entryId) {
        return sets.get((int) (entryId % ensembleSize));
    }

    static ImmutableList<Integer> generateWriteSet(List<Integer> preferredOrder,
                                                   int ensembleSize,
                                                   int writeQuorumSize,
                                                   int offset) {
        ImmutableList.Builder<Integer> builder =
            new ImmutableList.Builder<Integer> ();
        int firstIndex = offset;
        int lastIndex = (offset + writeQuorumSize - 1) % ensembleSize;
        for (Integer i : preferredOrder) {
            if (firstIndex <= lastIndex
                && i >= firstIndex
                && i <= lastIndex) {
                builder.add(i);
            } else if (lastIndex < firstIndex
                       && (i <= lastIndex
                           || i >= firstIndex)) {
                builder.add(i);
            }
        }
        ImmutableList<Integer> writeSet = builder.build();

        // writeSet may be one smaller than the configured write
        // set size if we are excluding ourself
        checkState(writeSet.size() == writeQuorumSize
                                 || (writeSet.size() == writeQuorumSize - 1));
        return writeSet;
    }
}
