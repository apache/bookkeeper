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

import java.util.Arrays;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

/**
 * Backoff time determined based as a multiple of baseBackoffTime.
 * The multiple value depends on retryCount.
 * If the retry schedule exceeds the deadline, we schedule a final attempt exactly at the deadline.
 */
@Slf4j
public class ExponentialBackOffWithDeadlinePolicy implements RetryPolicy {

    static final int [] RETRY_BACKOFF = {0, 1, 2, 3, 5, 5, 5, 10, 10, 10, 20, 40, 100};
    public static final int JITTER_PERCENT = 10;
    private final Random random;

    private final long baseBackoffTime;
    private final long deadline;
    private final int maxRetries;

    public ExponentialBackOffWithDeadlinePolicy(long baseBackoffTime, long deadline, int maxRetries) {
        this.baseBackoffTime = baseBackoffTime;
        this.deadline = deadline;
        this.maxRetries = maxRetries;
        this.random = new Random(System.currentTimeMillis());
    }

    @Override
    public boolean allowRetry(int retryCount, long elapsedRetryTime) {
        return retryCount <= maxRetries && elapsedRetryTime < deadline;
    }

    @Override
    public long nextRetryWaitTime(int retryCount, long elapsedRetryTime) {
        int idx = retryCount;
        if (idx >= RETRY_BACKOFF.length) {
            idx = RETRY_BACKOFF.length - 1;
        }

        long waitTime = (baseBackoffTime * RETRY_BACKOFF[idx]);
        long jitter = (random.nextInt(JITTER_PERCENT) * waitTime / 100);

        if (elapsedRetryTime + waitTime + jitter > deadline) {
            log.warn("Final retry attempt: {}, timeleft: {}, stacktrace: {}",
                    retryCount, (deadline - elapsedRetryTime), Arrays.toString(Thread.currentThread().getStackTrace()));
            return deadline - elapsedRetryTime;
        }

        return waitTime + jitter;
    }
}