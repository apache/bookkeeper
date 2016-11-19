package org.apache.bookkeeper.zookeeper;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestRetryPolicy {

    private static void assertTimeRange(long waitTime, long minTime, long maxTime) {
        assertTrue(waitTime >= minTime);
        assertTrue(waitTime <= maxTime);
    }

    @Test(timeout = 60000)
    public void testExponentialBackoffRetryPolicy() throws Exception {
        RetryPolicy policy = new ExponentialBackoffRetryPolicy(1000, Integer.MAX_VALUE);
        assertTimeRange(policy.nextRetryWaitTime(30, 2000), 1000L, (long) (1000 * Math.pow(2, 31)));
        assertTimeRange(policy.nextRetryWaitTime(31, 2000), 1000L, (long) (1000 * Math.pow(2, 32)));
        assertTimeRange(policy.nextRetryWaitTime(32, 2000), 1000L, (long) (1000 * Math.pow(2, 33)));
        assertTimeRange(policy.nextRetryWaitTime(127, 2000), 1000L, 1000L);
        assertTimeRange(policy.nextRetryWaitTime(128, 2000), 1000L, 2000L);
        assertTimeRange(policy.nextRetryWaitTime(129, 2000), 1000L, 4000L);
    }

    @Test(timeout = 60000)
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
