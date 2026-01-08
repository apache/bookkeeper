package org.apache.bookkeeper.zookeeper;

import java.util.Random;

/**
 * Simple retry policy for testing purposes.
 */
public class SimpleRetryPolicy {
    private final long baseBackoffTime;
    private final int maxRetries;
    private final Random random;

    public SimpleRetryPolicy(long baseBackoffTime, int maxRetries) {
        this.baseBackoffTime = baseBackoffTime;
        this.maxRetries = maxRetries;
        this.random = new Random(System.currentTimeMillis());
    }

    public boolean allowRetry(int retryCount, long elapsedRetryTime) {
        return retryCount <= maxRetries;
    }

    public long nextRetryWaitTime(int retryCount, long elapsedRetryTime) {
        int upperBound = Math.max(1, 1 << (retryCount + 1));
        return baseBackoffTime * Math.max(1, random.nextInt(upperBound));
    }
}
