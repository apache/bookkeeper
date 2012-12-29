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
import org.junit.Test;
import static org.junit.Assert.*;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinDistributionScheduleTest {
    static Logger LOG = LoggerFactory.getLogger(RoundRobinDistributionScheduleTest.class);

    @Test(timeout=60000)
    public void testDistributionSchedule() throws Exception {
        RoundRobinDistributionSchedule schedule = new RoundRobinDistributionSchedule(3, 2, 5);

        List<Integer> wSet = schedule.getWriteSet(1);
        assertEquals("Write set is wrong size", wSet.size(), 3);

        DistributionSchedule.AckSet ackSet = schedule.getAckSet();
        assertFalse("Shouldn't ack yet", ackSet.addBookieAndCheck(wSet.get(0)));
        assertFalse("Shouldn't ack yet", ackSet.addBookieAndCheck(wSet.get(0)));
        assertTrue("Should ack after 2 unique", ackSet.addBookieAndCheck(wSet.get(2)));
        assertTrue("Should still be acking", ackSet.addBookieAndCheck(wSet.get(1)));

        DistributionSchedule.QuorumCoverageSet covSet = schedule.getCoverageSet();
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(0));
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(2));
        assertTrue("Should cover now", covSet.addBookieAndCheckCovered(3));

        covSet = schedule.getCoverageSet();
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(0));
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(1));
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(2));
        assertTrue("Should cover now", covSet.addBookieAndCheckCovered(3));

        covSet = schedule.getCoverageSet();
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(4));
        assertFalse("Shouldn't cover yet", covSet.addBookieAndCheckCovered(0));
        assertTrue("Should cover now", covSet.addBookieAndCheckCovered(2));
    }
}