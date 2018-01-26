/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.api.subscription;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.DLSN;

/**
 * Store to manage subscriptions.
 */
public interface SubscriptionsStore extends Closeable {

    /**
     * Get the last committed position stored for <i>subscriberId</i>.
     *
     * @param subscriberId
     *          subscriber id
     * @return future representing last committed position.
     */
    CompletableFuture<DLSN> getLastCommitPosition(String subscriberId);

    /**
     * Get the last committed positions for all subscribers.
     *
     * @return future representing last committed positions for all subscribers.
     */
    CompletableFuture<Map<String, DLSN>> getLastCommitPositions();

    /**
     * Advance the last committed position for <i>subscriberId</i>.
     *
     * @param subscriberId
     *          subscriber id.
     * @param newPosition
     *          new committed position.
     * @return future representing advancing result.
     */
    CompletableFuture<Void> advanceCommitPosition(String subscriberId, DLSN newPosition);

    /**
     * Delete the subscriber <i>subscriberId</i> permanently. Once the subscriber is deleted, all the
     * data stored under this subscriber will be lost.
     * @param subscriberId subscriber id
     * @return future represent success or failure.
     * return true only if there's such subscriber and we removed it successfully.
     * return false if there's no such subscriber, or we failed to remove.
     */
    CompletableFuture<Boolean> deleteSubscriber(String subscriberId);

}
