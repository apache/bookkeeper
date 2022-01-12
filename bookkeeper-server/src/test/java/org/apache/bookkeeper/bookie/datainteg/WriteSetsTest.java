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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isIn;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for WriteSets.
 */
public class WriteSetsTest {
    private static final Logger log = LoggerFactory.getLogger(WriteSetsTest.class);

    @Test
    public void testOrderPreserved() throws Exception {
        WriteSets writeSets = new WriteSets(ImmutableList.of(0, 3, 2, 4, 1),
                                            5 /* ensemble */, 2 /* writeQ */);
        assertThat(writeSets.getForEntry(0), contains(0, 1));
        assertThat(writeSets.getForEntry(1), contains(2, 1));
        assertThat(writeSets.getForEntry(2), contains(3, 2));
        assertThat(writeSets.getForEntry(3), contains(3, 4));
        assertThat(writeSets.getForEntry(4), contains(0, 4));
    }

    @Test
    public void testOrderPreservedWithGapForCurrentBookie() throws Exception {
        // my bookie id maps to 2, so it is missing from the preferred order
        WriteSets writeSets = new WriteSets(ImmutableList.of(0, 3, 4, 1),
                                            5 /* ensemble */, 2 /* writeQ */);
        assertThat(writeSets.getForEntry(0), contains(0, 1));
        assertThat(writeSets.getForEntry(1), contains(1));
        assertThat(writeSets.getForEntry(2), contains(3));
        assertThat(writeSets.getForEntry(3), contains(3, 4));
        assertThat(writeSets.getForEntry(4), contains(0, 4));
    }

    @Test
    public void testEmptyWriteSet() throws Exception {
        // As can happen if we are the only bookie for a entry
        WriteSets writeSets = new WriteSets(ImmutableList.of(0, 3, 4, 1),
                                            5 /* ensemble */, 1 /* writeQ */);
        assertThat(writeSets.getForEntry(0), contains(0));
        assertThat(writeSets.getForEntry(1), contains(1));
        assertThat(writeSets.getForEntry(2), empty());
        assertThat(writeSets.getForEntry(3), contains(3));
        assertThat(writeSets.getForEntry(4), contains(4));
    }

    @Test
    public void testE2W2() throws Exception {
        DistributionSchedule schedule = new RoundRobinDistributionSchedule(
                2 /* write */, 2 /* ack */, 2 /* ensemble */);
        WriteSets writeSets = new WriteSets(ImmutableList.of(0, 1),
                                            2 /* ensemble */, 2 /* writeQ */);
        for (int i = 0; i < 100; i++) {
            ImmutableList<Integer> writeSet = writeSets.getForEntry(i);
            DistributionSchedule.WriteSet distWriteSet = schedule.getWriteSet(i);
            assertContentsMatch(writeSet, distWriteSet);
        }

        WriteSets writeSets2 = new WriteSets(ImmutableList.of(1, 0),
                                             2 /* ensemble */, 2 /* writeQ */);
        for (int i = 0; i < 100; i++) {
            ImmutableList<Integer> writeSet = writeSets2.getForEntry(i);
            DistributionSchedule.WriteSet distWriteSet = schedule.getWriteSet(i);
            assertContentsMatch(writeSet, distWriteSet);
        }
    };

    @Test
    public void testE10W2() throws Exception {
        DistributionSchedule schedule = new RoundRobinDistributionSchedule(
                2 /* write */, 2 /* ack */, 10 /* ensemble */);
        WriteSets writeSets = new WriteSets(ImmutableList.of(0, 8, 1, 9, 6, 3, 7, 4, 2, 5),
                                            10 /* ensemble */,
                                            2 /* writeQ */);
        for (int i = 0; i < 100; i++) {
            ImmutableList<Integer> writeSet = writeSets.getForEntry(i);
            DistributionSchedule.WriteSet distWriteSet = schedule.getWriteSet(i);
            assertContentsMatch(writeSet, distWriteSet);
        }

        WriteSets writeSets2 = new WriteSets(ImmutableList.of(7, 5, 1, 6, 3, 0, 8, 9, 4, 2),
                                             10 /* ensemble */,
                                             2 /* writeQ */);
        for (int i = 0; i < 100; i++) {
            ImmutableList<Integer> writeSet = writeSets2.getForEntry(i);
            DistributionSchedule.WriteSet distWriteSet = schedule.getWriteSet(i);
            assertContentsMatch(writeSet, distWriteSet);
        }

        WriteSets writeSets3 = new WriteSets(ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                                             10 /* ensemble */,
                                             2 /* writeQ */);
        for (int i = 0; i < 100; i++) {
            ImmutableList<Integer> writeSet = writeSets3.getForEntry(i);
            DistributionSchedule.WriteSet distWriteSet = schedule.getWriteSet(i);
            assertContentsMatch(writeSet, distWriteSet);
        }
    };

    @Test
    public void testManyVariants() throws Exception {
        for (int w = 1; w <= 12; w++) {
            for (int e = w; e <= 12; e++) {
                DistributionSchedule schedule = new RoundRobinDistributionSchedule(
                        w /* write */, w /* ack */, e /* ensemble */);

                // Create shuffled set of indices
                List<Integer> indices = new ArrayList<>();
                for (int i = 0; i < e; i++) {
                    indices.add(i);
                }
                Collections.shuffle(indices);

                WriteSets writeSets = new WriteSets(ImmutableList.copyOf(indices),
                                                    e, w);
                for (int i = 0; i < 100; i++) {
                    ImmutableList<Integer> writeSet = writeSets.getForEntry(i);
                    DistributionSchedule.WriteSet distWriteSet = schedule.getWriteSet(i);
                    assertContentsMatch(writeSet, distWriteSet);
                }
            }
        }
    }

    private static void assertContentsMatch(ImmutableList<Integer> writeSet,
                                            DistributionSchedule.WriteSet distWriteSet)
            throws Exception {
        log.info("writeSet {} distWriteSet {}", writeSet, distWriteSet.size());
        assertThat(writeSet.size(), equalTo(distWriteSet.size()));
        for (Integer i : writeSet) {
            assertThat(distWriteSet.contains(i), equalTo(true));
        }

        for (int i = 0; i < distWriteSet.size(); i++) {
            assertThat(distWriteSet.get(i), isIn(writeSet));
        }
    }
}
