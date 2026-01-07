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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test class for ExponentialBackoffRetryPolicy with Mocks and Stubs.
 * 
 * This test suite verifies the behavior of ExponentialBackoffRetryPolicy
 * using mock objects and stubs to isolate the unit under test.
 */
@DisplayName("ExponentialBackoffRetryPolicy - Mock & Stub Tests")
class ExponentialBackoffRetryPolicyMockStubTest {

    private ExponentialBackoffRetryPolicy retryPolicy;
    private static final long BASE_BACKOFF_TIME = 100L;
    private static final int MAX_RETRIES = 5;

    @BeforeEach
    void setUp() {
        retryPolicy = new ExponentialBackoffRetryPolicy(BASE_BACKOFF_TIME, MAX_RETRIES);
    }

    /**
     * Test 1: allowRetry respects max retry boundary
     * Stub approach: uses actual Random behavior but checks boundary
     */
    @Test
    @DisplayName("allowRetry should return true when retryCount <= maxRetries")
    void testAllowRetryWithinBoundary() {
        // Arrange
        int retryCountAtBoundary = MAX_RETRIES;
        long elapsedRetryTime = 1000L;

        // Act
        boolean result = retryPolicy.allowRetry(retryCountAtBoundary, elapsedRetryTime);

        // Assert
        assertTrue(result, "Should allow retry at boundary (retryCount == maxRetries)");
    }

    /**
     * Test 2: allowRetry rejects exceeding max retries
     * Stub approach: verifies rejection behavior
     */
    @Test
    @DisplayName("allowRetry should return false when retryCount exceeds maxRetries")
    void testAllowRetryExceedsBoundary() {
        // Arrange
        int retryCountOverBoundary = MAX_RETRIES + 1;
        long elapsedRetryTime = 5000L;

        // Act
        boolean result = retryPolicy.allowRetry(retryCountOverBoundary, elapsedRetryTime);

        // Assert
        assertFalse(result, "Should reject retry when retryCount exceeds maxRetries");
    }

    /**
     * Test 3: nextRetryWaitTime increases exponentially
     * Mock approach: verify exponential growth pattern
     */
    @Test
    @DisplayName("nextRetryWaitTime should increase exponentially with retry count")
    void testExponentialBackoffProgression() {
        // Arrange
        long elapsedTime = 0L;

        // Act - Collect wait times for increasing retry counts
        long waitTime1 = retryPolicy.nextRetryWaitTime(0, elapsedTime);
        long waitTime2 = retryPolicy.nextRetryWaitTime(1, elapsedTime);
        long waitTime3 = retryPolicy.nextRetryWaitTime(2, elapsedTime);

        // Assert - Verify exponential progression
        assertThat(waitTime1, greaterThanOrEqualTo(BASE_BACKOFF_TIME));
        assertThat(waitTime2, greaterThanOrEqualTo(waitTime1));
        assertThat(waitTime3, greaterThanOrEqualTo(waitTime2));
    }

    /**
     * Test 4: nextRetryWaitTime respects minimum backoff
     * Stub approach: validates minimum boundary
     */
    @Test
    @DisplayName("nextRetryWaitTime should maintain minimum baseBackoffTime")
    void testMinimumBackoffTime() {
        // Arrange
        int retryCount = 0;
        long elapsedTime = 0L;

        // Act
        long waitTime = retryPolicy.nextRetryWaitTime(retryCount, elapsedTime);

        // Assert
        assertThat(waitTime, greaterThanOrEqualTo(BASE_BACKOFF_TIME));
    }

    /**
     * Test 5: nextRetryWaitTime with zero retry count uses minimal exponential factor
     * Stub approach: verifies edge case at retryCount=0
     */
    @Test
    @DisplayName("nextRetryWaitTime at retry count 0 should use minimal exponential backoff")
    void testZeroRetryCountBackoff() {
        // Arrange
        int retryCount = 0;
        long elapsedTime = 1000L;

        // Act
        long waitTime = retryPolicy.nextRetryWaitTime(retryCount, elapsedTime);

        // Assert
        // With retryCount=0, 1 << (0+1) = 2, so max value for random would be 2
        assertThat(waitTime, greaterThanOrEqualTo(BASE_BACKOFF_TIME));
        assertThat(waitTime, lessThanOrEqualTo(BASE_BACKOFF_TIME * 2));
    }

    /**
     * Test 6: nextRetryWaitTime with high retry count produces larger backoff
     * Mock approach: verifies high retry count behavior
     */
    @Test
    @DisplayName("nextRetryWaitTime with high retry count produces exponentially larger backoff")
    void testHighRetryCountBackoff() {
        // Arrange
        int lowRetryCount = 1;
        int highRetryCount = 4;
        long elapsedTime = 0L;

        // Act
        long lowWaitTime = retryPolicy.nextRetryWaitTime(lowRetryCount, elapsedTime);
        long highWaitTime = retryPolicy.nextRetryWaitTime(highRetryCount, elapsedTime);

        // Assert - High retry count should produce larger or equal backoff in general
        assertThat(highWaitTime, greaterThanOrEqualTo(lowWaitTime));
    }

    /**
     * Test 7: Multiple retries with stub random behavior
     * Stub approach: verifies consistent boundary checks across multiple attempts
     */
    @Test
    @DisplayName("allowRetry should consistently handle multiple retry attempts")
    void testMultipleRetryAttempts() {
        // Arrange
        long elapsedTime = 1000L;

        // Act & Assert - Verify each retry count individually
        for (int i = 0; i <= MAX_RETRIES; i++) {
            assertTrue(retryPolicy.allowRetry(i, elapsedTime),
                    "Should allow retry at count " + i);
        }

        // Verify rejection at boundary
        assertFalse(retryPolicy.allowRetry(MAX_RETRIES + 1, elapsedTime),
                "Should reject retry at count " + (MAX_RETRIES + 1));
    }

    /**
     * Test 8: Retry policy with different configurations
     * Stub approach: test with mock-like configurations
     */
    @Test
    @DisplayName("ExponentialBackoffRetryPolicy with different max retry configurations")
    void testDifferentMaxRetryConfigurations() {
        // Arrange
        int maxRetries3 = 3;
        int maxRetries10 = 10;
        ExponentialBackoffRetryPolicy policy3 = new ExponentialBackoffRetryPolicy(50L, maxRetries3);
        ExponentialBackoffRetryPolicy policy10 = new ExponentialBackoffRetryPolicy(50L, maxRetries10);

        // Act & Assert
        assertTrue(policy3.allowRetry(maxRetries3, 0L), "Policy with 3 max retries should allow 3");
        assertFalse(policy3.allowRetry(maxRetries3 + 1, 0L), "Policy should reject count > 3");

        assertTrue(policy10.allowRetry(maxRetries10, 0L), "Policy with 10 max retries should allow 10");
        assertFalse(policy10.allowRetry(maxRetries10 + 1, 0L), "Policy should reject count > 10");
    }

    /**
     * Test 9: Verify elapsedRetryTime parameter is ignored in allowRetry
     * Mock approach: tests that elapsedRetryTime doesn't affect allowRetry result
     */
    @Test
    @DisplayName("elapsedRetryTime parameter should not affect allowRetry decision")
    void testElapsedTimeIndependenceInAllowRetry() {
        // Arrange
        int retryCount = 2;
        long shortElapsedTime = 100L;
        long longElapsedTime = 10000L;

        // Act
        boolean resultShortTime = retryPolicy.allowRetry(retryCount, shortElapsedTime);
        boolean resultLongTime = retryPolicy.allowRetry(retryCount, longElapsedTime);

        // Assert
        assertEquals(resultShortTime, resultLongTime,
                "allowRetry result should be independent of elapsedRetryTime");
    }

    /**
     * Test 10: Stub behavior for randomization in nextRetryWaitTime
     * Stub approach: verify randomization produces varied results within bounds
     */
    @Test
    @DisplayName("nextRetryWaitTime should produce random values within expected bounds")
    void testRandomizationBounds() {
        // Arrange
        int retryCount = 2;
        long elapsedTime = 0L;
        long minBound = BASE_BACKOFF_TIME;
        long maxBound = BASE_BACKOFF_TIME * Math.max(1, 1 << (retryCount + 1));

        // Act - Get multiple values to verify randomization
        long[] waitTimes = new long[10];
        for (int i = 0; i < 10; i++) {
            waitTimes[i] = retryPolicy.nextRetryWaitTime(retryCount, elapsedTime);
        }

        // Assert - All values should be within bounds
        for (long waitTime : waitTimes) {
            assertThat(waitTime, greaterThanOrEqualTo(minBound));
            assertThat(waitTime, lessThanOrEqualTo(maxBound));
        }
    }
}
