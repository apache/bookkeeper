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
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A root range client that talks to root range.
 *
 * <p>It is an internal client to get namespace specific metadata.
 */
public interface RootRangeClient {

    /**
     * Create a namespace.
     *
     * @param namespace namespace
     * @param colConf   namespace configuration
     * @return a future represents the result of creation.
     */
    CompletableFuture<NamespaceProperties> createNamespace(String namespace,
                                                           NamespaceConfiguration colConf);

    /**
     * Delete a namespace.
     *
     * @param namespace namespace.
     * @return a future represents the result of deletion.
     */
    CompletableFuture<Boolean> deleteNamespace(String namespace);

    /**
     * Get the namespace properties of the provided {@code namespace}.
     *
     * @param namespace namespace name
     * @return a future represents namespace properties.
     */
    CompletableFuture<NamespaceProperties> getNamespace(String namespace);

    /**
     * Create a stream.
     *
     * @param colName    namespace name
     * @param streamName stream name
     * @param streamConf stream configuration
     * @return response of creating a stream.
     */
    CompletableFuture<StreamProperties> createStream(String colName,
                                                     String streamName,
                                                     StreamConfiguration streamConf);

    /**
     * Delete the stream {@code streamName}.
     *
     * @param colName    namespace name
     * @param streamName stream name
     * @return a future represents the metadata of a stream.
     */
    CompletableFuture<Boolean> deleteStream(String colName,
                                            String streamName);

    /**
     * Get the stream metadata of a given stream {@code streamName}.
     *
     * @param colName    namespace name
     * @param streamName stream name
     * @return a future represents the metadata of a stream.
     */
    CompletableFuture<StreamProperties> getStream(String colName,
                                                  String streamName);

    /**
     * Get the stream metadata of a given stream identified by {@code streamId}.
     *
     * @param streamId stream id
     * @return a future represents the metadata of a stream.
     */
    CompletableFuture<StreamProperties> getStream(long streamId);

}
