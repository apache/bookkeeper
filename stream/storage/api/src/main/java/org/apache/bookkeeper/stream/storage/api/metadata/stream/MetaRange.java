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

package org.apache.bookkeeper.stream.storage.api.metadata.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stream.proto.RangeMetadata;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamMetadata.ServingState;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A meta range for handling metadata operations for a stream.
 */
public interface MetaRange {

    /**
     * Get the stream name.
     *
     * @return stream name.
     */
    String getName();

    /**
     * Create the stream.
     *
     * @param streamProps stream properties
     * @return a future represents the creation result.
     */
    CompletableFuture<Boolean> create(StreamProperties streamProps);

    /**
     * Load the stream metadata.
     *
     * @param streamId stream id to load metadata.
     * @return a future represents an initialized metadata range instance.
     */
    CompletableFuture<MetaRange> load(long streamId);

    /**
     * Delete a stream.
     *
     * @return a future represents the deletion result.
     */
    CompletableFuture<Boolean> delete(long streamId);

    /**
     * Get the stream configuration.
     *
     * @return the stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration();

    /**
     * Get the stream state.
     *
     * @return a future represents the stream state.
     */
    CompletableFuture<ServingState> getServingState();

    /**
     * Update the stream state.
     *
     * @param state the stream state
     * @return a future represents the stream state.
     */
    CompletableFuture<ServingState> updateServingState(ServingState state);

    /**
     * Return the current active ranges.
     *
     * @return the current active ranges.
     */
    CompletableFuture<List<RangeMetadata>> getActiveRanges();

}
