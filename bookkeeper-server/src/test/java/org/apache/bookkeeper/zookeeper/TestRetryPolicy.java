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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    @Test
    public void testExponentialBackoffWithDeadlineRetryPolicy() throws Exception {
        RetryPolicy policy = new ExponentialBackOffWithDeadlinePolicy(100, 55 * 1000, 20);

        // Retries are allowed as long as we don't exceed the limits of retry count and deadline
        assertTrue(policy.allowRetry(1, 5 * 1000));
        assertTrue(policy.allowRetry(4, 20 * 1000));
        assertTrue(policy.allowRetry(10, 50 * 1000));

        assertFalse(policy.allowRetry(0, 60 * 1000));
        assertFalse(policy.allowRetry(22, 20 * 1000));
        assertFalse(policy.allowRetry(22, 60 * 1000));

        // Verify that the wait times are in the range and with the excepted jitter, until deadline is exceeded
        assertTimeRange(policy.nextRetryWaitTime(0, 0), 0, 0);
        assertTimeRange(policy.nextRetryWaitTime(1, 0), 100, 110);
        assertTimeRange(policy.nextRetryWaitTime(1, 53 * 1000), 100, 110);
        assertTimeRange(policy.nextRetryWaitTime(2, 0), 200, 220);
        assertTimeRange(policy.nextRetryWaitTime(3, 0), 300, 330);
        assertTimeRange(policy.nextRetryWaitTime(3, 53 * 1000), 300, 330);
        assertTimeRange(policy.nextRetryWaitTime(4, 0), 500, 550);
        assertTimeRange(policy.nextRetryWaitTime(5, 0), 500, 550);

        // Verify that the final attempt is triggered at deadline.
        assertEquals(2000, policy.nextRetryWaitTime(10, 53 * 1000));
        assertEquals(4000, policy.nextRetryWaitTime(15, 51 * 1000));
    }
}
