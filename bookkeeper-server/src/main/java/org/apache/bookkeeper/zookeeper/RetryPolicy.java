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

/**
 * Interface of the policy to use when retrying operations.
 */
public interface RetryPolicy {

    /**
     * Called when retrying an operation failed for some reason. Return true if
     * another attempt is allowed to make.
     *
     * @param retryCount
     *            The number of times retried so far (1 for the first time).
     * @param elapsedRetryTime
     *            The elapsed time since the operation attempted. (in
     *            milliseconds)
     * @return true if anther attempt is allowed to make. otherwise, false.
     */
    boolean allowRetry(int retryCount, long elapsedRetryTime);

    /**
     * Called before making an attempt to retry a failed operation. Return 0 if
     * an attempt needs to be made immediately.
     *
     * @param retryCount
     *            The number of times retried so far (0 for the first time).
     * @param elapsedRetryTime
     *            The elapsed time since the operation attempted. (in
     *            milliseconds)
     * @return the elapsed time that the attempt needs to wait before retrying.
     *         (in milliseconds)
     */
    long nextRetryWaitTime(int retryCount, long elapsedRetryTime);

}
