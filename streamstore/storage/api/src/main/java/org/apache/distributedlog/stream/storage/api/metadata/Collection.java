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

import java.util.List;
import java.util.Set;
import org.apache.distributedlog.stream.proto.CollectionMetadata;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * A handle to operate on a {@code Collection}.
 */
public interface Collection {

  /**
   * Get the id of the collection.
   *
   * @return collection id.
   */
  long getId();

  /**
   * Get the name of the collection.
   *
   * @return collection name.
   */
  String getName();

  /**
   * Get the metadata of this collection.
   *
   * @return collection metadata.
   */
  CollectionMetadata getMetadata();

  /**
   * Set the metadata of this collection.
   *
   * @param metadata the new metadata of this collection.
   */
  void setMetadata(CollectionMetadata metadata);

  /**
   * Get the existing streams in this collections.
   *
   * @return list of the existing streams.
   */
  Set<String> getStreams();

  /**
   * Get the existing streams properties in this collections.
   *
   * @return list of the existing streams.
   */
  List<StreamProperties> getStreamsProperties();

    /**
     * Add the given <i>streamName</i> to this collection.
     *
     * @param streamName stream name.
     * @param streamProps stream properties
     * @return true on success. false on failure.
     */
  boolean addStream(String streamName, StreamProperties streamProps);

  /**
   * Remove the given <i>streamName</i> from this collection.
   *
   * @param streamName stream name.
   * @return true on success. false on failure.
   */
  StreamProperties removeStream(String streamName);

  /**
   * Get the given <i>streamName</i> from this collection.
   *
   * @param streamName stream name.
   * @return true on success. false on failure.
   */
  StreamProperties getStream(String streamName);

  /**
   * Remove the given <i>streamId</i> from this collection.
   *
   * @param streamId stream id.
   * @return true on success. false on failure.
   */
  StreamProperties removeStream(long streamId);

  /**
   * Get the given <i>streamId</i> from this collection.
   *
   * @param streamId stream id.
   * @return true on success. false on failure.
   */
  StreamProperties getStream(long streamId);

}
