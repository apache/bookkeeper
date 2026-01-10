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

package org.apache.bookkeeper.zookeeper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ExponentialBackoffRetryPolicy - LLM minimal high-ROI tests")
class ExponentialBackoffRetryPolicyLLMTest {

    private static void setRandomSeed(Object policy, Class<?> clazz, long seed) throws Exception {
        Field f = clazz.getDeclaredField("random");
        f.setAccessible(true);
        f.set(policy, new Random(seed));
    }

    @Test
    @DisplayName("allowRetry boundaries: within limit true, beyond limit false")
    void testAllowRetryBoundaries() {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(100L, 5);
        assertTrue(policy.allowRetry(0, 0L));
        assertTrue(policy.allowRetry(5, 0L));
        assertFalse(policy.allowRetry(6, 0L));
    }

    @Test
    @DisplayName("nextRetryWaitTime within expected exponential range")
    void testNextRetryWaitTimeRange() {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(100L, 10);
        long w0 = policy.nextRetryWaitTime(0, 0L); // max multiplier 2
        long w5 = policy.nextRetryWaitTime(5, 0L); // max multiplier 64
        assertThat(w0, greaterThanOrEqualTo(100L));
        assertThat(w0, lessThanOrEqualTo(200L));
        assertThat(w5, greaterThanOrEqualTo(100L));
        assertThat(w5, lessThanOrEqualTo(6400L));
    }

    @Test
    @DisplayName("Zero and negative maxRetries handling")
    void testMaxRetriesEdgeCases() {
        ExponentialBackoffRetryPolicy zero = new ExponentialBackoffRetryPolicy(100L, 0);
        assertTrue(zero.allowRetry(0, 0L));
        assertFalse(zero.allowRetry(1, 0L));

        ExponentialBackoffRetryPolicy negative = new ExponentialBackoffRetryPolicy(100L, -1);
        assertFalse(negative.allowRetry(0, 0L));
    }

    @Test
    @DisplayName("BoundExponentialBackoffRetryPolicy caps wait time")
    void testBoundedBackoffIsCapped() throws Exception {
        long base = 100L;
        long cap = 250L;
        BoundExponentialBackoffRetryPolicy policy = new BoundExponentialBackoffRetryPolicy(base, cap, 10);
        setRandomSeed(policy, ExponentialBackoffRetryPolicy.class, 1L);
        long wait = policy.nextRetryWaitTime(8, 0L); // large multiplier before cap
        assertThat(wait, greaterThanOrEqualTo(base));
        assertThat(wait, lessThanOrEqualTo(cap));
    }

    @Test
    @DisplayName("Deadline policy clamps to remaining time and obeys limits")
    void testDeadlinePolicyClampAndAllowRetry() throws Exception {
        long deadline = 100L;
        ExponentialBackOffWithDeadlinePolicy policy = new ExponentialBackOffWithDeadlinePolicy(10L, deadline, 2);
        setRandomSeed(policy, ExponentialBackOffWithDeadlinePolicy.class, 2L);

        assertTrue(policy.allowRetry(1, 20L));
        assertFalse(policy.allowRetry(3, 20L)); // over max retries
        assertFalse(policy.allowRetry(1, 150L)); // past deadline

        long wait = policy.nextRetryWaitTime(5, 80L); // would exceed deadline -> clamp
        assertEquals(deadline - 80L, wait);
    }
}
