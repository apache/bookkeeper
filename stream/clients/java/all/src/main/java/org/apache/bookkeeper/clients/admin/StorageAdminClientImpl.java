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

package org.apache.bookkeeper.clients.admin;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.clients.StorageClientImpl;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.internal.StorageServerClientManagerImpl;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * A storage admin client.
 */
@Slf4j
public class StorageAdminClientImpl extends AbstractAutoAsyncCloseable implements StorageAdminClient {

    // clients
    private final StorageClientSettings settings;
    private final ClientResources resources;
    private final StorageServerClientManager clientManager;
    private final RootRangeClient rootRangeClient;

    /**
     * Create a stream admin client with provided {@code withSettings}.
     *
     * @param settings withSettings to create an admin client.
     * @param resources resources used by this client
     */
    public StorageAdminClientImpl(StorageClientSettings settings,
                                  ClientResources resources) {
        this(
            settings,
            resources,
            () -> new StorageServerClientManagerImpl(settings, resources.scheduler()));
    }

    StorageAdminClientImpl(StorageClientSettings settings,
                           ClientResources resources,
                           Supplier<StorageServerClientManager> factory) {
        this.settings = settings;
        this.resources = resources;
        this.clientManager = factory.get();
        this.rootRangeClient = this.clientManager.getRootRangeClient();
    }

    @Override
    public StorageClient asClient(String namespace) {
        return new StorageClientImpl(
            namespace, settings, resources, clientManager, false);
    }

    @Override
    public CompletableFuture<NamespaceProperties> createNamespace(String namespace,
                                                                  NamespaceConfiguration colConf) {
        return rootRangeClient.createNamespace(namespace, colConf);
    }

    @Override
    public CompletableFuture<Boolean> deleteNamespace(String namespace) {
        return rootRangeClient.deleteNamespace(namespace);
    }

    @Override
    public CompletableFuture<NamespaceProperties> getNamespace(String namespace) {
        return rootRangeClient.getNamespace(namespace);
    }

    @Override
    public CompletableFuture<StreamProperties> createStream(String namespace,
                                                            String streamName,
                                                            StreamConfiguration streamConf) {
        return rootRangeClient.createStream(namespace, streamName, streamConf);
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(String namespace,
                                                   String streamName) {
        return rootRangeClient.deleteStream(namespace, streamName);
    }

    @Override
    public CompletableFuture<StreamProperties> getStream(String namespace,
                                                         String streamName) {
        return rootRangeClient.getStream(namespace, streamName);
    }

    //
    // Closeable API
    //

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        clientManager.closeAsync().whenComplete((result, cause) -> {
            closeFuture.complete(null);
        });
    }

    @Override
    public void close() {
        try {
            closeAsync().get();
        } catch (InterruptedException e) {
            log.debug("Interrupted on closing stream admin client", e);
        } catch (ExecutionException e) {
            log.debug("Failed to cloe stream admin client", e);
        }
    }
}
