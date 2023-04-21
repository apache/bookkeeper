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

package org.apache.bookkeeper.clients.impl.internal.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A client factory that manages various clients.
 *
 * <p>All connections/errors should be handled in this class and its related subclasses.
 */
public interface StorageServerClientManager extends AutoAsyncCloseable {

    /**
     * Get the channel connected to a storage container <i>scId</i>.
     *
     * @param scId storage container id
     * @return the channel connected to the storage container.
     */
    StorageContainerChannel getStorageContainerChannel(long scId);

    /**
     * Create a location client.
     *
     * @return a location client.
     */
    LocationClient getLocationClient();

    /**
     * Create a root range client.
     *
     * @return a root range client.
     */
    RootRangeClient getRootRangeClient();

    /**
     * Get the stream properties of a given stream {@code streamId}.
     *
     * @param streamId stream id
     * @return a future represents the get result.
     */
    CompletableFuture<StreamProperties> getStreamProperties(long streamId);

    /**
     * Create a meta range client.
     *
     * @return a meta range client.
     */
    MetaRangeClient openMetaRangeClient(StreamProperties streamProps);

    /**
     * Open a meta range client for a given stream {@code streamId}.
     *
     * @param streamId stream id
     * @return a future represents open future.
     */
    CompletableFuture<MetaRangeClient> openMetaRangeClient(long streamId);

}
