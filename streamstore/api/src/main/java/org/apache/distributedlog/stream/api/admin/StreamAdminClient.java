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
package org.apache.distributedlog.stream.api.admin;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.CollectionProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * StreamAdminClient is the admin client that application uses to create and delete streams, create and delete
 * coordination groups and modify their configurations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StreamAdminClient extends AutoCloseable, AutoAsyncCloseable {

  //
  // Collection API
  //

  /**
   * Create a collection.
   *
   * @param collection collection
   * @param colConf collection configuration
   * @return a future represents the result of creation.
   */
  CompletableFuture<CollectionProperties> createCollection(String collection,
                                                           CollectionConfiguration colConf);

  /**
   * Delete a collection.
   *
   * @param collection collection.
   * @return a future represents the result of deletion.
   */
  CompletableFuture<Boolean> deleteCollection(String collection);

  /**
   * Get the collection properties of the provided {@code collection}.
   *
   * @param collection collection name
   * @return a future represents collection properties.
   */
  CompletableFuture<CollectionProperties> getCollection(String collection);

  //
  // Stream API
  //

  /**
   * Create a stream in a collection.
   *
   * @param collection collection
   * @param streamName stream name.
   * @param streamConf stream configuration.
   * @return a future represents the result of creation.
   */
  CompletableFuture<StreamProperties> createStream(String collection,
                                                   String streamName,
                                                   StreamConfiguration streamConf);

  /**
   * Delete a stream in a collection.
   *
   * @param collection collection
   * @param streamName stream name.
   * @return a future represents the result of deletion.
   */
  CompletableFuture<Boolean> deleteStream(String collection, String streamName);

  /**
   * Get a stream in a collection.
   *
   * @param collection collection
   * @param streamName stream name.
   * @return a future represents the result of get.
   */
  CompletableFuture<StreamProperties> getStream(String collection, String streamName);

  //
  // Closeable API
  //

  /**
   * Close the read client.
   */
  void close();

}
