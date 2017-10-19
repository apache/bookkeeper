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
package org.apache.bookkeeper.client;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import com.google.common.collect.Sets;
import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinDistributionScheduleTest {

    private final static Logger LOG = LoggerFactory.getLogger(RoundRobinDistributionScheduleTest.class);

    @Test
    public void testDistributionSchedule() throws Exception {
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(3, 2, 5);

        List<Integer> wSet = schedule.getWriteSet(1);
        assertEquals("Write set is wrong size", wSet.size(), 3);

        DistributionSchedule.AckSet ackSet = schedule.getAckSet();
        assertFalse("Shouldn't ack yet", ackSet.completeBookieAndCheck(wSet.get(0), -1));
        assertFalse("Shouldn't ack yet", ackSet.completeBookieAndCheck(wSet.get(0), -1));
        assertTrue("Should ack after 2 unique", ackSet.completeBookieAndCheck(wSet.get(2), -1));
        assertTrue("Should still be acking", ackSet.completeBookieAndCheck(wSet.get(1), -1));
    }

    /**
     * Test that coverage sets only respond as covered when it has
     * heard from enough bookies that no ack quorum can exist without these bookies.
     */
    @Test
    public void testCoverageSets() {
        int errors = 0;
        for (int e = 6; e > 0; e--) {
            for (int w = e; w > 0; w--) {
                for (int a = w; a > 0; a--) {
                    errors += testCoverageForConfiguration(e, w, a);
                }
            }
        }
        assertEquals("Should be no errors", 0, errors);
    }

    /**
     * Build a boolean array of which nodes have not responded
     * and thus are available to build a quorum.
     */
    boolean[] buildAvailable(int ensemble, Set<Integer> responses) {
        boolean[] available = new boolean[ensemble];
        for (int i = 0; i < ensemble; i++) {
            if (responses.contains(i)) {
                available[i] = false;
            } else {
                available[i] = true;
            }
        }
        return available;
    }

    /**
     * Check whether it is possible for a write to reach
     * a quorum with a given set of nodes available
     */
    boolean canGetAckQuorum(int ensemble, int writeQuorum, int ackQuorum, boolean[] available) {
        for (int i = 0; i < ensemble; i++) {
            int count = 0;
            for (int j = 0; j < writeQuorum; j++) {
                if (available[(i+j)%ensemble]) {
                    count++;
                }
            }
            if (count >= ackQuorum) {
                return true;
            }
        }
        return false;
    }

    private int testCoverageForConfiguration(int ensemble, int writeQuorum, int ackQuorum) {
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(
            writeQuorum, ackQuorum, ensemble);
        Set<Integer> indexes = new HashSet<Integer>();
        for (int i = 0; i < ensemble; i++) {
            indexes.add(i);
        }
        Set<Set<Integer>> subsets = Sets.powerSet(indexes);

        int errors = 0;
        for (Set<Integer> subset : subsets) {
            DistributionSchedule.QuorumCoverageSet covSet = schedule.getCoverageSet();
            for (Integer i : subset) {
                covSet.addBookie(i, BKException.Code.OK);
            }
            boolean covSetSays = covSet.checkCovered();

            boolean[] nodesAvailable = buildAvailable(ensemble, subset);
            boolean canGetAck = canGetAckQuorum(ensemble, writeQuorum, ackQuorum, nodesAvailable);
            if (canGetAck == covSetSays) {
                LOG.error("e{}:w{}:a{} available {}    canGetAck {} covSetSays {}",
                          new Object[] { ensemble, writeQuorum, ackQuorum,
                                         nodesAvailable, canGetAck, covSetSays });
                errors++;
            }
        }
        return errors;
    }

    @Test
    public void testCalculateLastAddSynced() throws Exception {
        assertCalculateLastAddSynced(3, 1, 5, Arrays.asList(1L, 2L, 3L), 3);
        assertCalculateLastAddSynced(3, 2, 5, Arrays.asList(1L, 2L, 3L), 2);
        assertCalculateLastAddSynced(3, 3, 5, Arrays.asList(1L, 2L, 3L), 1);
    }

    @Test
    public void testCalculateLastAddSyncedNoEnoughData() throws Exception {
        assertCalculateLastAddSynced(3, 3, 3, Arrays.asList(), -1);
        assertCalculateLastAddSynced(3, 3, 3, Arrays.asList(1L), -1);
        assertCalculateLastAddSynced(3, 3, 3, Arrays.asList(1L, 2L), -1);
    }

    private void assertCalculateLastAddSynced(int writeQuorumSize, int ackQuorumSize, int ensembleSize, List<Long> lastAddSynced, long expectedLastAddSynced) {
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(writeQuorumSize, ackQuorumSize, ensembleSize);
        List<Integer> wSet = schedule.getWriteSet(1);
        assertEquals("Write set is wrong size", wSet.size(), 3);
        DistributionSchedule.AckSet ackSet = schedule.getAckSet();
        for (int i = 0; i < lastAddSynced.size(); i++) {
            ackSet.completeBookieAndCheck(wSet.get(i), lastAddSynced.get(i));
        }
        assertEquals(expectedLastAddSynced, ackSet.calculateCurrentLastAddSynced());
    }
}