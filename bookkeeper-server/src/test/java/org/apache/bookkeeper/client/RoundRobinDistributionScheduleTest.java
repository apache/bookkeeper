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

import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinDistributionScheduleTest {
    private final static Logger LOG = LoggerFactory.getLogger(RoundRobinDistributionScheduleTest.class);

    @Test(timeout=60000)
    public void testDistributionSchedule() throws Exception {
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(3, 2, 5);

        List<Integer> wSet = schedule.getWriteSet(1);
        assertEquals("Write set is wrong size", wSet.size(), 3);

        DistributionSchedule.AckSet ackSet = schedule.getAckSet();
        assertFalse("Shouldn't ack yet", ackSet.completeBookieAndCheck(wSet.get(0)));
        assertFalse("Shouldn't ack yet", ackSet.completeBookieAndCheck(wSet.get(0)));
        assertTrue("Should ack after 2 unique", ackSet.completeBookieAndCheck(wSet.get(2)));
        assertTrue("Should still be acking", ackSet.completeBookieAndCheck(wSet.get(1)));
    }

    /**
     * Test that coverage sets only respond as covered when it has
     * heard from enough bookies that no ack quorum can exist without these bookies.
     */
    @Test(timeout=60000)
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
}
