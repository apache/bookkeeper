/**
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
package org.apache.bookkeeper.zookeeper;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test the retry policy.
 */
public class TestRetryPolicy {

    private static void assertTimeRange(long waitTime, long minTime, long maxTime) {
        assertTrue(waitTime >= minTime);
        assertTrue(waitTime <= maxTime);
    }

    @Test
    public void testExponentialBackoffRetryPolicy() throws Exception {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(1000, Integer.MAX_VALUE);
        assertTimeRange(policy.nextRetryWaitTime(30, 2000), 1000L, (long) (1000 * Math.pow(2, 31)));
        assertTimeRange(policy.nextRetryWaitTime(31, 2000), 1000L, (long) (1000 * Math.pow(2, 32)));
        assertTimeRange(policy.nextRetryWaitTime(32, 2000), 1000L, (long) (1000 * Math.pow(2, 33)));
        assertTimeRange(policy.nextRetryWaitTime(127, 2000), 1000L, 1000L);
        assertTimeRange(policy.nextRetryWaitTime(128, 2000), 1000L, 2000L);
        assertTimeRange(policy.nextRetryWaitTime(129, 2000), 1000L, 4000L);
    }

    @Test
    public void testBoundExponentialBackoffRetryPolicy() throws Exception {
        RetryPolicy policy = new BoundExponentialBackoffRetryPolicy(1000, 2000, Integer.MAX_VALUE);
        assertTimeRange(policy.nextRetryWaitTime(30, 2000), 1000L, 2000L);
        assertTimeRange(policy.nextRetryWaitTime(31, 2000), 1000L, 2000L);
        assertTimeRange(policy.nextRetryWaitTime(32, 2000), 1000L, 2000L);
        assertTimeRange(policy.nextRetryWaitTime(127, 2000), 1000L, 1000L);
        assertTimeRange(policy.nextRetryWaitTime(128, 2000), 1000L, 2000L);
        assertTimeRange(policy.nextRetryWaitTime(129, 2000), 1000L, 2000L);
    }
}
