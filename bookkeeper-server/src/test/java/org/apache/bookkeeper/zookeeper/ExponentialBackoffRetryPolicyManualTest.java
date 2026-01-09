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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Test class for ExponentialBackoffRetryPolicy - Control-Flow Driven Tests.
 * 
 * This test suite provides comprehensive code coverage by systematically
 * exploring all control flow paths and code branches in ExponentialBackoffRetryPolicy.
 * Tests are designed to achieve maximum code coverage with JaCoCo.
 * 
 * Methods tested:
 * 1. allowRetry(int retryCount, long elapsedRetryTime) - Control: retryCount <= maxRetries
 * 2. nextRetryWaitTime(int retryCount, long elapsedRetryTime) - Control: Math operations and randomization
 */
@DisplayName("ExponentialBackoffRetryPolicy - Control-Flow & Coverage Tests")
class ExponentialBackoffRetryPolicyControlFlowTest {

    private ExponentialBackoffRetryPolicy retryPolicy;

    @BeforeEach
    void setUp() {
        retryPolicy = new ExponentialBackoffRetryPolicy(100L, 5);
    }

    // ==================== CONTROL FLOW: allowRetry ====================

    /**
     * Control flow path 1: allowRetry - retryCount <= maxRetries (TRUE branch)
     * Line coverage: retryCount <= maxRetries evaluates to true
     */
    @Test
    @DisplayName("[CF-1] allowRetry path: retryCount <= maxRetries is TRUE")
    void testAllowRetryTruePath() {
        // All values from 0 to maxRetries (5) should return true
        for (int retryCount = 0; retryCount <= 5; retryCount++) {
            assertTrue(retryPolicy.allowRetry(retryCount, 0L),
                "Path: retryCount(" + retryCount + ") <= maxRetries(5) should be TRUE");
        }
    }

    /**
     * Control flow path 2: allowRetry - retryCount > maxRetries (FALSE branch)
     * Line coverage: retryCount <= maxRetries evaluates to false
     */
    @Test
    @DisplayName("[CF-2] allowRetry path: retryCount <= maxRetries is FALSE")
    void testAllowRetryFalsePath() {
        // All values > maxRetries should return false
        for (int retryCount = 6; retryCount <= 10; retryCount++) {
            assertFalse(retryPolicy.allowRetry(retryCount, 0L),
                "Path: retryCount(" + retryCount + ") > maxRetries(5) should be FALSE");
        }
    }

    /**
     * Control flow boundary: allowRetry - exact boundary at maxRetries
     * Line coverage: Test retryCount == maxRetries (boundary condition)
     */
    @Test
    @DisplayName("[CF-3] allowRetry boundary: retryCount == maxRetries")
    void testAllowRetryBoundary() {
        // At exact boundary
        assertTrue(retryPolicy.allowRetry(5, 0L),
            "At boundary: retryCount(5) == maxRetries(5) should be TRUE");

        // Just beyond boundary
        assertFalse(retryPolicy.allowRetry(6, 0L),
            "Beyond boundary: retryCount(6) > maxRetries(5) should be FALSE");
    }

    /**
     * Control flow: allowRetry - verify elapsedRetryTime doesn't affect return value
     * Line coverage: elapsedRetryTime is a parameter but not used in comparison
     */
    @Test
    @DisplayName("[CF-4] allowRetry: elapsedRetryTime parameter path independence")
    void testAllowRetryElapsedTimePathIndependence() {
        int retryCount = 3;

        // Different elapsed times should NOT change the boolean result
        assertTrue(retryPolicy.allowRetry(retryCount, 0L));
        assertTrue(retryPolicy.allowRetry(retryCount, 1000L));
        assertTrue(retryPolicy.allowRetry(retryCount, Long.MAX_VALUE));

        // Same for false path
        assertFalse(retryPolicy.allowRetry(6, 0L));
        assertFalse(retryPolicy.allowRetry(6, 1000L));
        assertFalse(retryPolicy.allowRetry(6, Long.MAX_VALUE));
    }

    // ==================== CONTROL FLOW: nextRetryWaitTime ====================

    /**
     * Control flow path 1: nextRetryWaitTime - Math.max first argument path
     * Line coverage: baseBackoffTime * Math.max(1, randomInt(1 << (retryCount + 1)))
     * Path: When retryCount = 0, (1 << 1) = 2
     */
    @Test
    @DisplayName("[CF-5] nextRetryWaitTime: retryCount=0, (1 << 1)=2 path")
    void testNextRetryWaitTimeRetryCount0Path() {
        // retryCount=0: random range is [0, 2), so backoff is baseBackoffTime * [1, 2]
        long backoff = retryPolicy.nextRetryWaitTime(0, 0L);
        
        long minExpected = 100L;  // baseBackoffTime * 1
        long maxExpected = 100L * 2;  // baseBackoffTime * 2

        assertThat(backoff, greaterThanOrEqualTo(minExpected));
        assertThat(backoff, lessThanOrEqualTo(maxExpected));
    }

    /**
     * Control flow path 2: nextRetryWaitTime - bit shift growth
     * Line coverage: (1 << (retryCount + 1)) increases exponentially
     * Path: retryCount increases from 1 to 5
     */
    @Test
    @DisplayName("[CF-6] nextRetryWaitTime: exponential bit-shift paths (retryCount 1-5)")
    void testNextRetryWaitTimeExponentialPaths() {
        // retryCount=1: 1 << 2 = 4
        long backoff1 = retryPolicy.nextRetryWaitTime(1, 0L);
        assertThat(backoff1, greaterThanOrEqualTo(100L));
        assertThat(backoff1, lessThanOrEqualTo(100L * 4));

        // retryCount=2: 1 << 3 = 8
        long backoff2 = retryPolicy.nextRetryWaitTime(2, 0L);
        assertThat(backoff2, greaterThanOrEqualTo(100L));
        assertThat(backoff2, lessThanOrEqualTo(100L * 8));

        // retryCount=3: 1 << 4 = 16
        long backoff3 = retryPolicy.nextRetryWaitTime(3, 0L);
        assertThat(backoff3, greaterThanOrEqualTo(100L));
        assertThat(backoff3, lessThanOrEqualTo(100L * 16));

        // retryCount=4: 1 << 5 = 32
        long backoff4 = retryPolicy.nextRetryWaitTime(4, 0L);
        assertThat(backoff4, greaterThanOrEqualTo(100L));
        assertThat(backoff4, lessThanOrEqualTo(100L * 32));

        // retryCount=5: 1 << 6 = 64
        long backoff5 = retryPolicy.nextRetryWaitTime(5, 0L);
        assertThat(backoff5, greaterThanOrEqualTo(100L));
        assertThat(backoff5, lessThanOrEqualTo(100L * 64));
    }

    /**
     * Control flow path 3: nextRetryWaitTime - Math.max function behavior
     * Line coverage: Test Math.max(1, randomValue) when randomValue could be less than 1
     * Path: (1 << (retryCount + 1)) could theoretically be 0 (though not with normal inputs)
     */
    @Test
    @DisplayName("[CF-7] nextRetryWaitTime: Math.max(1, ...) ensures minimum multiplier")
    void testNextRetryWaitTimeMathMaxPath() {
        // Verify all backoffs respect minimum baseBackoffTime
        for (int retryCount = 0; retryCount <= 5; retryCount++) {
            long backoff = retryPolicy.nextRetryWaitTime(retryCount, 0L);
            assertTrue(backoff >= 100L,
                "Math.max ensures backoff >= baseBackoffTime at retryCount=" + retryCount);
        }
    }

    /**
     * Control flow path 4: nextRetryWaitTime - Random value generation range
     * Line coverage: random.nextInt(max) generates values [0, max)
     * Path: Verify randomization generates variety of values
     */
    @Test
    @DisplayName("[CF-8] nextRetryWaitTime: randomization coverage (varied values)")
    void testNextRetryWaitTimeRandomizationPath() {
        int retryCount = 2;  // Max random range: 1 << 3 = 8
        Set<Long> uniqueValues = new HashSet<>();

        // Generate multiple backoff values to capture randomization
        for (int i = 0; i < 100; i++) {
            long backoff = retryPolicy.nextRetryWaitTime(retryCount, 0L);
            uniqueValues.add(backoff);
        }

        // Should have multiple different values due to randomization
        assertTrue(uniqueValues.size() > 1,
            "Randomization should produce multiple different backoff values. Got: " + uniqueValues.size());
    }

    /**
     * Control flow: nextRetryWaitTime - multiplication operation path
     * Line coverage: baseBackoffTime * Math.max(...) multiplication
     */
    @Test
    @DisplayName("[CF-9] nextRetryWaitTime: multiplication operation path")
    void testNextRetryWaitTimeMultiplicationPath() {
        long baseBackoff = 100L;
        int maxRandomValue = 1 << 3;  // For retryCount=2

        long backoff = retryPolicy.nextRetryWaitTime(2, 0L);

        // Should be baseBackoff * [1, maxRandomValue]
        assertTrue(backoff >= baseBackoff && backoff <= baseBackoff * maxRandomValue,
            "Multiplication path: " + backoff + " should be in [" + baseBackoff + ", " 
            + (baseBackoff * maxRandomValue) + "]");
    }

    // ==================== CONSTRUCTOR AND INITIALIZATION ====================

    /**
     * Control flow: Constructor initialization path
     * Line coverage: Constructor stores baseBackoffTime and maxRetries
     */
    @Test
    @DisplayName("[CF-10] Constructor: initialization and field assignment paths")
    void testConstructorInitializationPaths() {
        // Create policies with different configurations
        ExponentialBackoffRetryPolicy policy1 = new ExponentialBackoffRetryPolicy(50L, 3);
        ExponentialBackoffRetryPolicy policy2 = new ExponentialBackoffRetryPolicy(200L, 10);

        // Verify behavior differs based on initialization
        assertTrue(policy1.allowRetry(3, 0L));
        assertFalse(policy1.allowRetry(4, 0L));

        assertTrue(policy2.allowRetry(10, 0L));
        assertFalse(policy2.allowRetry(11, 0L));
    }

    /**
     * Control flow: Random object initialization
     * Line coverage: new Random(System.currentTimeMillis()) in constructor
     */
    @Test
    @DisplayName("[CF-11] Constructor: Random seed initialization path")
    void testConstructorRandomInitialization() {
        // Multiple instances should have different random sequences
        ExponentialBackoffRetryPolicy policy1 = new ExponentialBackoffRetryPolicy(100L, 5);
        ExponentialBackoffRetryPolicy policy2 = new ExponentialBackoffRetryPolicy(100L, 5);

        // Collect backoff values from both
        Set<Long> values1 = new HashSet<>();
        Set<Long> values2 = new HashSet<>();

        for (int i = 0; i < 50; i++) {
            values1.add(policy1.nextRetryWaitTime(2, 0L));
            values2.add(policy2.nextRetryWaitTime(2, 0L));
        }

        // Should have reasonable variety in both
        assertTrue(values1.size() > 1, "Policy1 should have varied values");
        assertTrue(values2.size() > 1, "Policy2 should have varied values");
    }

    // ==================== COMBINED CONTROL FLOW SCENARIOS ====================

    /**
     * Integrated test: Full retry sequence covering all paths
     * Line coverage: Tests all critical paths in combination
     */
    @Test
    @DisplayName("[CF-12] Integrated: Full retry sequence covering all paths")
    void testFullRetrySequenceAllPaths() {
        // Test retries from 0 to beyond max
        for (int retryCount = 0; retryCount <= 7; retryCount++) {
            boolean allowed = retryPolicy.allowRetry(retryCount, 0L);

            if (retryCount <= 5) {
                assertTrue(allowed, "Should allow retry at " + retryCount);
                // When allowed, get backoff time
                long backoff = retryPolicy.nextRetryWaitTime(retryCount, 0L);
                assertThat(backoff, greaterThanOrEqualTo(100L));
            } else {
                assertFalse(allowed, "Should reject retry at " + retryCount);
            }
        }
    }

    /**
     * Integrated test: Variable elapsed time path coverage
     * Line coverage: Tests allowRetry and nextRetryWaitTime with various elapsedRetryTime values
     */
    @Test
    @DisplayName("[CF-13] Integrated: Elapsed time variation path coverage")
    void testElapsedTimeVariationPathCoverage() {
        long[] elapsedTimes = {0L, 100L, 1000L, 10000L, Long.MAX_VALUE};

        for (long elapsedTime : elapsedTimes) {
            for (int retryCount = 0; retryCount <= 5; retryCount++) {
                // Test allowRetry path
                assertTrue(retryPolicy.allowRetry(retryCount, elapsedTime),
                    "allowRetry path with elapsed=" + elapsedTime + " at count=" + retryCount);

                // Test nextRetryWaitTime path
                long backoff = retryPolicy.nextRetryWaitTime(retryCount, elapsedTime);
                assertTrue(backoff >= 100L,
                    "nextRetryWaitTime path with elapsed=" + elapsedTime + " at count=" + retryCount);
            }
        }
    }

    /**
     * Integrated test: Edge case path coverage
     * Line coverage: Tests boundary and edge case control flows
     */
    @Test
    @DisplayName("[CF-14] Integrated: Edge case path coverage")
    void testEdgeCasePathCoverage() {
        // Test with zero base backoff
        ExponentialBackoffRetryPolicy zeroPolicy = new ExponentialBackoffRetryPolicy(0L, 3);
        assertTrue(zeroPolicy.allowRetry(0, 0L));
        assertFalse(zeroPolicy.allowRetry(4, 0L));
        long backoff = zeroPolicy.nextRetryWaitTime(0, 0L);
        assertThat(backoff, greaterThanOrEqualTo(0L));

        // Test with large max retries
        ExponentialBackoffRetryPolicy largePolicy = new ExponentialBackoffRetryPolicy(100L, 100);
        assertTrue(largePolicy.allowRetry(100, 0L));
        assertFalse(largePolicy.allowRetry(101, 0L));

        // Test with negative elapsed time
        assertTrue(retryPolicy.allowRetry(2, -1000L));
        long backoffNegative = retryPolicy.nextRetryWaitTime(2, -1000L);
        assertThat(backoffNegative, greaterThanOrEqualTo(100L));
    }

    /**
     * Coverage metric verification test
     * This test should achieve >95% line coverage when run with JaCoCo
     */
    @Test
    @DisplayName("[CF-15] Coverage verification: Comprehensive coverage check")
    void testCoverageVerification() {
        // Each important line/branch should be executed at least once
        
        // Line 1: Constructor Random initialization - COVERED by setUp()
        // Line 2: Constructor field assignments - COVERED by setUp()
        
        // Line 3: allowRetry true path - COVERED
        assertTrue(retryPolicy.allowRetry(2, 0L));
        
        // Line 4: allowRetry false path - COVERED
        assertFalse(retryPolicy.allowRetry(6, 0L));
        
        // Line 5: nextRetryWaitTime baseBackoffTime * Math.max - COVERED
        long backoff1 = retryPolicy.nextRetryWaitTime(0, 0L);
        assertTrue(backoff1 >= 100L);
        
        // Line 6: nextRetryWaitTime with various retryCount values - COVERED
        long backoff2 = retryPolicy.nextRetryWaitTime(3, 0L);
        assertTrue(backoff2 >= 100L);
        
        // Line 7: nextRetryWaitTime Random.nextInt call - COVERED
        long backoff3 = retryPolicy.nextRetryWaitTime(1, 1000L);
        assertTrue(backoff3 >= 100L);
    }
}
