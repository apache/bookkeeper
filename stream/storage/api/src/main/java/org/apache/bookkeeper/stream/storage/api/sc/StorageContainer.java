/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.api.sc;

import io.grpc.Channel;
import java.util.concurrent.CompletableFuture;

/**
 * A {@code StorageContainer} is a service container that can encapsulate metadata and data operations.
 *
 * <p>A {@link StorageContainer} is typically implemented by replicated state machine backed by a log.
 */
public interface StorageContainer extends AutoCloseable {

    /**
     * Get the storage container id.
     *
     * @return the storage container id.
     */
    long getId();

    /**
     * Get the grpc channel to interact with grpc services registered in this container.
     *
     * @return grpc channel.
     */
    Channel getChannel();

    /**
     * Start the storage container.
     *
     * @return a future represents the result of starting a storage container.
     */
    CompletableFuture<StorageContainer> start();

    /**
     * Stop the storage container.
     *
     * @return a future represents the result of stopping a storage container.
     */
    CompletableFuture<Void> stop();

    /**
     * Close a storage container.
     */
    @Override
    void close();

}
