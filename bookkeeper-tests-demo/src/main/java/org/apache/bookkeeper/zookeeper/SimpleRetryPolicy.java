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
