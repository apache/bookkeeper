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

import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.zookeeper.KeeperException;

/**
 * Provide a mechanism to perform an operation on ZooKeeper that is safe on disconnections
 * and recoverable errors.
 */
@CustomLog
class ZooWorker {

    int attempts = 0;
    long startTimeNanos;
    long elapsedTimeMs = 0L;
    final RetryPolicy retryPolicy;
    final OpStatsLogger statsLogger;

    ZooWorker(RetryPolicy retryPolicy, OpStatsLogger statsLogger) {
        this.retryPolicy = retryPolicy;
        this.statsLogger = statsLogger;
        this.startTimeNanos = MathUtils.nowInNano();
    }

    public boolean allowRetry(int rc) {
        elapsedTimeMs = MathUtils.elapsedMSec(startTimeNanos);
        if (!ZooWorker.isRecoverableException(rc)) {
            if (KeeperException.Code.OK.intValue() == rc) {
                statsLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos), TimeUnit.MICROSECONDS);
            } else {
                statsLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos), TimeUnit.MICROSECONDS);
            }
            return false;
        }
        ++attempts;
        return retryPolicy.allowRetry(attempts, elapsedTimeMs);
    }

    public long nextRetryWaitTime() {
        return retryPolicy.nextRetryWaitTime(attempts, elapsedTimeMs);
    }

    /**
     * Check whether the given result code is recoverable by retry.
     *
     * @param rc result code
     * @return true if given result code is recoverable.
     */
    public static boolean isRecoverableException(int rc) {
        return KeeperException.Code.CONNECTIONLOSS.intValue() == rc
            || KeeperException.Code.OPERATIONTIMEOUT.intValue() == rc
            || KeeperException.Code.SESSIONMOVED.intValue() == rc
            || KeeperException.Code.SESSIONEXPIRED.intValue() == rc;
    }

    /**
     * Check whether the given exception is recoverable by retry.
     *
     * @param exception given exception
     * @return true if given exception is recoverable.
     */
    public static boolean isRecoverableException(KeeperException exception) {
        return isRecoverableException(exception.code().intValue());
    }

    interface ZooCallable<T> {
        /**
         * Be compatible with ZooKeeper interface.
         *
         * @return value
         * @throws InterruptedException
         * @throws KeeperException
         */
        T call() throws InterruptedException, KeeperException;
    }

    /**
     * Execute a sync zookeeper operation with a given retry policy.
     *
     * @param client
     *          ZooKeeper client.
     * @param proc
     *          Synchronous zookeeper operation wrapped in a {@link Callable}.
     * @param retryPolicy
     *          Retry policy to execute the synchronous operation.
     * @param rateLimiter
     *          Rate limiter for zookeeper calls
     * @param statsLogger
     *          Stats Logger for zookeeper client.
     * @return result of the zookeeper operation
     * @throws KeeperException any non-recoverable exception or recoverable exception exhausted all retires.
     * @throws InterruptedException the operation is interrupted.
     */
    public static<T> T syncCallWithRetries(ZooKeeperClient client,
                                           ZooCallable<T> proc,
                                           RetryPolicy retryPolicy,
                                           RateLimiter rateLimiter,
                                           OpStatsLogger statsLogger)
    throws KeeperException, InterruptedException {
        T result = null;
        boolean isDone = false;
        int attempts = 0;
        long startTimeNanos = MathUtils.nowInNano();
        while (!isDone) {
            try {
                if (null != client) {
                    client.waitForConnection();
                }
                log.debug()
                        .attr("proc", proc)
                        .attr("attempts", attempts)
                        .log("Execute at retry attempt");
                if (null != rateLimiter) {
                    rateLimiter.acquire();
                }
                result = proc.call();
                isDone = true;
                statsLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos), TimeUnit.MICROSECONDS);
            } catch (KeeperException e) {
                ++attempts;
                boolean rethrow = true;
                long elapsedTime = MathUtils.elapsedMSec(startTimeNanos);
                if (((null != client && isRecoverableException(e)) || null == client)
                    && retryPolicy.allowRetry(attempts, elapsedTime)) {
                    rethrow = false;
                }
                if (rethrow) {
                    statsLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos), TimeUnit.MICROSECONDS);
                    log.debug()
                            .attr("proc", proc)
                            .attr("attempts", attempts)
                            .log("Stopped executing after attempts");
                    throw e;
                }
                TimeUnit.MILLISECONDS.sleep(retryPolicy.nextRetryWaitTime(attempts, elapsedTime));
            }
        }
        return result;
    }

}
