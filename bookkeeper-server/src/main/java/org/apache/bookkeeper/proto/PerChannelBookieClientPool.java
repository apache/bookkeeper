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
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

/**
 * An interface to manage channel pooling for bookie client.
 */
public interface PerChannelBookieClientPool {

    /**
     * intialize the pool. the implementation should not be blocked.
     */
    void intialize();

    /**
     * Obtain a channel from channel pool to execute operations.
     *
     * @param callback
     *          callback to return channel from channel pool.
     */
    void obtain(GenericCallback<PerChannelBookieClient> callback, long key);

    /**
     * Obtain a channel from channel pool by version to execute operations.
     *
     * @param callback
     *          callback to return channel from channel pool
     * @param forceUseV3
     *          whether or not use v3 protocol for connection
     */
    void obtain(GenericCallback<PerChannelBookieClient> callback, long key, boolean forceUseV3);

    /**
     * Returns status of a client.
     * It is suggested to delay/throttle requests to this channel if isWritable is false.
     *
     * @param key
     * @return
     */
    default boolean isWritable(long key) {
        return true;
    }

    /**
     * record any read/write error on {@link PerChannelBookieClientPool}.
     */
    void recordError();

    /**
     * Check if any ops on any channel needs to be timed out.
     * This is called on all channels, even if the channel is not yet connected.
     */
    void checkTimeoutOnPendingOperations();

    /**
     * Disconnect the connections in the pool.
     *
     * @param wait
     *          whether need to wait until pool disconnected.
     */
    void disconnect(boolean wait);

    /**
     * Close the pool.
     *
     * @param wait
     *          whether need to wait until pool closed.
     */
    void close(boolean wait);

    /**
     * Get the number of pending completion requests in the channel.
     */
    long getNumPendingCompletionRequests();
}
