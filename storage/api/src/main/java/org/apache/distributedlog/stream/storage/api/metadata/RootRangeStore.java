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

package org.apache.distributedlog.stream.storage.api.metadata;

import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamResponse;

/**
 * The metadata store that stores root range information.
 *
 * <p>These information includes:
 * <ul>
 *   <li>the list of collection</li>
 *   <li>the list of streams within each collection</li>
 *   <li>the list of transactions</li>
 *   <li>the list of read groups</li>
 * </ul>
 */
public interface RootRangeStore {

  //
  // Collection API
  //

  /**
   * Create a new collection with the provide <i>name</i> and configuration <i>colConf</i>.
   *
   * @param request create collection request
   * @return public collection properties on success or exception on failure.
   */
  CompletableFuture<CreateCollectionResponse> createCollection(CreateCollectionRequest request);

  /**
   * Delete a collection named <i>colName</i>.
   *
   * @param request delete collection request
   * @return null on success or exception on failure.
   */
  CompletableFuture<DeleteCollectionResponse> deleteCollection(DeleteCollectionRequest request);

  /**
   * Retrieve the configuration for collection <i>colName</i>.
   *
   * @param request get collection request
   * @return public collection properties on success or exception on failure.
   */
  CompletableFuture<GetCollectionResponse> getCollection(GetCollectionRequest request);

  //
  // Stream API
  //

  /**
   * Create a new stream with the provide <i>name</i> and configuration <i>streamConf</i>.
   *
   * @param request create stream request
   * @return public stream properties on success or exception on failure.
   */
  CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request);

  /**
   * Delete a stream named <i>streamName</i>.
   *
   * @param request delete stream request
   * @return null on success or exception on failure.
   */
  CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request);

  /**
   * Retrieve the configuration for stream <i>streamName</i>.
   *
   * @param request get stream request
   * @return public stream properties on success or exception on failure.
   */
  CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request);

}
