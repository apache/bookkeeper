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

/**
 * Retry policy that retries a set number of times with an increasing (up to a
 * maximum bound) backoff time between retries.
 */
public class BoundExponentialBackoffRetryPolicy extends ExponentialBackoffRetryPolicy {

    private final long maxBackoffTime;

    public BoundExponentialBackoffRetryPolicy(long baseBackoffTime, long maxBackoffTime, int maxRetries) {
        super(baseBackoffTime, maxRetries);
        this.maxBackoffTime = maxBackoffTime;
    }

    @Override
    public long nextRetryWaitTime(int retryCount, long elapsedRetryTime) {
        return Math.min(maxBackoffTime, super.nextRetryWaitTime(retryCount, elapsedRetryTime));
    }

}
