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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a mechanism to perform an operation on ZooKeeper that is safe on disconnections
 * and recoverable errors.
 */
class ZooWorker {

    static final Logger logger = LoggerFactory.getLogger(ZooWorker.class);

    int attempts = 0;
    long startTimeMs;
    long elapsedTimeMs = 0L;
    final RetryPolicy retryPolicy;

    ZooWorker(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        this.startTimeMs = MathUtils.now();
    }

    public boolean allowRetry(int rc) {
        if (!ZooWorker.isRecoverableException(rc)) {
            return false;
        }
        ++attempts;
        elapsedTimeMs = MathUtils.now() - startTimeMs;
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
        return KeeperException.Code.CONNECTIONLOSS.intValue() == rc ||
                KeeperException.Code.OPERATIONTIMEOUT.intValue() == rc ||
                KeeperException.Code.SESSIONMOVED.intValue() == rc ||
                KeeperException.Code.SESSIONEXPIRED.intValue() == rc;
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
    
    static interface ZooCallable<T> {
        /**
         * Be compatible with ZooKeeper interface.
         *
         * @return value
         * @throws InterruptedException
         * @throws KeeperException
         */
        public T call() throws InterruptedException, KeeperException;
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
     * @return result of the zookeeper operation
     * @throws KeeperException any non-recoverable exception or recoverable exception exhausted all retires.
     * @throws InterruptedException the operation is interrupted.
     */
    public static<T> T syncCallWithRetries(
            ZooKeeperClient client, ZooCallable<T> proc, RetryPolicy retryPolicy)
    throws KeeperException, InterruptedException {
        T result = null;
        boolean isDone = false;
        int attempts = 0;
        long startTimeMs = MathUtils.now();
        while (!isDone) {
            try {
                if (null != client) {
                    client.waitForConnection();
                }
                logger.debug("Execute {} at {} retry attempt.", proc, attempts);
                result = proc.call();
                isDone = true;
            } catch (KeeperException e) {
                ++attempts;
                boolean rethrow = true;
                long elapsedTime = MathUtils.now() - startTimeMs;
                if (((null != client && isRecoverableException(e)) || null == client) &&
                        retryPolicy.allowRetry(attempts, elapsedTime)) {
                    rethrow = false;
                }
                if (rethrow) {
                    logger.debug("Stopped executing {} after {} attempts.", proc, attempts);
                    throw e;
                }
                TimeUnit.MILLISECONDS.sleep(retryPolicy.nextRetryWaitTime(attempts, elapsedTime));
            }
        }
        return result;
    }

    static<T> T syncCallWithRetries(
            ZooCallable<T> proc, RetryPolicy retryPolicy) throws KeeperException, InterruptedException {
        return syncCallWithRetries(null, proc, retryPolicy);
    }

}
