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

import static org.apache.bookkeeper.client.RoundRobinDistributionSchedule.writeSetFromValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.client.DistributionSchedule.QuorumCoverageSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test a round-robin distribution schedule.
 */
public class RoundRobinDistributionScheduleTest {
    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinDistributionScheduleTest.class);

    @Test
    public void testDistributionSchedule() throws Exception {
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(3, 2, 5);

        DistributionSchedule.WriteSet wSet = schedule.getWriteSet(1);
        assertEquals("Write set is wrong size", wSet.size(), 3);
        DistributionSchedule.AckSet ackSet = schedule.getAckSet();
        assertFalse("Shouldn't ack yet",
                    ackSet.completeBookieAndCheck(wSet.get(0)));
        assertFalse("Shouldn't ack yet",
                    ackSet.completeBookieAndCheck(wSet.get(0)));
        assertTrue("Should ack after 2 unique",
                   ackSet.completeBookieAndCheck(wSet.get(2)));
        assertTrue("Should still be acking",
                   ackSet.completeBookieAndCheck(wSet.get(1)));
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
     * a quorum with a given set of nodes available.
     */
    boolean canGetAckQuorum(int ensemble, int writeQuorum, int ackQuorum, boolean[] available) {
        for (int i = 0; i < ensemble; i++) {
            int count = 0;
            for (int j = 0; j < writeQuorum; j++) {
                if (available[(i + j) % ensemble]) {
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
                        ensemble, writeQuorum, ackQuorum,
                        nodesAvailable, canGetAck, covSetSays);
                errors++;
            }
        }
        return errors;
    }

    @Test
    public void testMoveAndShift() {
        DistributionSchedule.WriteSet w = writeSetFromValues(1, 2, 3, 4, 5);
        w.moveAndShift(3, 1);
        assertEquals(w, writeSetFromValues(1, 4, 2, 3, 5));

        w = writeSetFromValues(1, 2, 3, 4, 5);
        w.moveAndShift(1, 3);
        assertEquals(w, writeSetFromValues(1, 3, 4, 2, 5));

        w = writeSetFromValues(1, 2, 3, 4, 5);
        w.moveAndShift(0, 4);
        assertEquals(w, writeSetFromValues(2, 3, 4, 5, 1));

        w = writeSetFromValues(1, 2, 3, 4, 5);
        w.moveAndShift(0, 0);
        assertEquals(w, writeSetFromValues(1, 2, 3, 4, 5));

        w = writeSetFromValues(1, 2, 3, 4, 5);
        w.moveAndShift(4, 4);
        assertEquals(w, writeSetFromValues(1, 2, 3, 4, 5));
    }

    @Test
    public void testRRQuorumCoverageSet() throws Exception {
        int ensembleSize = 9;
        int writeQuorumSize = 7;
        int ackQuorumSize = 5;
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(writeQuorumSize, ackQuorumSize,
                ensembleSize);
        QuorumCoverageSet rrQuorumCoverageSet = schedule.getCoverageSet();
        rrQuorumCoverageSet.addBookie(0, BKException.Code.DigestMatchException);
        rrQuorumCoverageSet.addBookie(1, BKException.Code.OK);
        rrQuorumCoverageSet.addBookie(2, BKException.Code.DigestMatchException);
        rrQuorumCoverageSet.addBookie(3, BKException.Code.OK);
        rrQuorumCoverageSet.addBookie(4, BKException.Code.DigestMatchException);
        rrQuorumCoverageSet.addBookie(5, BKException.Code.OK);
        rrQuorumCoverageSet.addBookie(6, BKException.Code.DigestMatchException);
        rrQuorumCoverageSet.addBookie(7, BKException.Code.OK);
        rrQuorumCoverageSet.addBookie(8, BKException.Code.DigestMatchException);
        /*
         * since not in any write quorum, there is |ackQuorum| number of OKs,
         * so checkcovered should be false
         */
        Assert.assertFalse("RRQuorumCoverageSet covered should be false", rrQuorumCoverageSet.checkCovered());
    }
}
