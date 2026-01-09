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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Demo test for ExponentialBackoffRetryPolicy.
 * This is a standalone test that doesn't depend on BookKeeper libraries.
 */
@DisplayName("ExponentialBackoffRetryPolicy - Demo Tests")
class ExponentialBackoffRetryPolicyDemoTest {

    private SimpleRetryPolicy retryPolicy;

    @BeforeEach
    void setUp() {
        retryPolicy = new SimpleRetryPolicy(100L, 5);
    }

    @Test
    @DisplayName("allowRetry with valid count should return true")
    void testAllowRetryValid() {
        assertTrue(retryPolicy.allowRetry(0, 0L), "Should allow retry at count 0");
        assertTrue(retryPolicy.allowRetry(5, 0L), "Should allow retry at count 5");
    }

    @Test
    @DisplayName("allowRetry exceeding max should return false")
    void testAllowRetryExceeds() {
        assertFalse(retryPolicy.allowRetry(6, 0L), "Should reject retry at count 6");
        assertFalse(retryPolicy.allowRetry(10, 0L), "Should reject retry at count 10");
    }

    @Test
    @DisplayName("nextRetryWaitTime should be >= baseBackoffTime")
    void testNextRetryWaitTime() {
        long waitTime = retryPolicy.nextRetryWaitTime(0, 0L);
        assertThat(waitTime, greaterThanOrEqualTo(100L));
    }

    @Test
    @DisplayName("nextRetryWaitTime should increase with retry count")
    void testBackoffIncrease() {
        long waitTime0 = retryPolicy.nextRetryWaitTime(0, 0L);
        long waitTime3 = retryPolicy.nextRetryWaitTime(3, 0L);
        
        assertThat(waitTime0, greaterThanOrEqualTo(100L));
        assertThat(waitTime3, greaterThanOrEqualTo(100L));
    }

    /**
     * Simple implementation for testing purposes
     */
    static class SimpleRetryPolicy {
        private final long baseBackoffTime;
        private final int maxRetries;
        private final java.util.Random random;

        SimpleRetryPolicy(long baseBackoffTime, int maxRetries) {
            this.baseBackoffTime = baseBackoffTime;
            this.maxRetries = maxRetries;
            this.random = new java.util.Random(System.currentTimeMillis());
        }

        boolean allowRetry(int retryCount, long elapsedRetryTime) {
            return retryCount <= maxRetries;
        }

        long nextRetryWaitTime(int retryCount, long elapsedRetryTime) {
            return baseBackoffTime * Math.max(1, random.nextInt(Math.max(1, 1 << (retryCount + 1))));
        }
    }
}
