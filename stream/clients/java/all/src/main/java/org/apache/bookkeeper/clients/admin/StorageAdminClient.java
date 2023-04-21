/*
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
package org.apache.bookkeeper.clients.admin;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.util.AutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A storage admin client.
 */
@Public
@Evolving
public interface StorageAdminClient extends AutoAsyncCloseable {

    /**
     * Convert the storage admin client to a client.
     *
     * @return storage client
     */
    default StorageClient asClient() {
        return asClient(null);
    }

    /**
     * Convert the storage admin client to a client with default <tt>namespace</tt>.
     *
     * @param namespace namespace
     * @return storage client
     */
    StorageClient asClient(String namespace);

    /**
     * Create a <code>namespace</code> with the provided namespace configuration <tt>conf</tt>.
     *
     * @param namespace namespace
     * @param conf namespace configuration
     * @return a future represent the creation result
     */
    CompletableFuture<NamespaceProperties> createNamespace(String namespace,
                                                           NamespaceConfiguration conf);

    /**
     * Delete a <code>namespace</code>.
     *
     * @param namespace namespace
     * @return a future represents the deletion result
     */
    CompletableFuture<Boolean> deleteNamespace(String namespace);

    /**
     * Get the namespace properties of a given <code>namespace</code>.
     *
     * @param namespace namespace
     * @return a future represents the get result
     */
    CompletableFuture<NamespaceProperties> getNamespace(String namespace);

    /**
     * Create a stream <code>streamName</code> under namespace <code>namespace</code>
     * with the provided stream configuration <tt>streamConfiguration</tt>.
     *
     * @param namespace namespace
     * @param streamName stream name
     * @param streamConfiguration stream configuration
     * @return a future represents the creation result
     */
    CompletableFuture<StreamProperties> createStream(String namespace,
                                                     String streamName,
                                                     StreamConfiguration streamConfiguration);

    /**
     * Delete a <code>stream</code> from the provided <tt>namespace</tt>.
     *
     * @param namespace namespace
     * @param streamName stream name
     * @return a future represents the deletion result
     */
    CompletableFuture<Boolean> deleteStream(String namespace, String streamName);

    /**
     * Retrieve the stream properties of a given <code>stream</code> under
     * the provided <code>namespace</code>.
     *
     * @param namespace namespace
     * @param streamName stream name
     * @return a future represents the get result
     */
    CompletableFuture<StreamProperties> getStream(String namespace, String streamName);

}
