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

package org.apache.bookkeeper.clients;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.exceptions.ApiException;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.internal.StorageServerClientManagerImpl;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.kv.ByteBufTableImpl;
import org.apache.bookkeeper.clients.impl.kv.PByteBufTableImpl;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.ExceptionUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * The implementation of {@link StorageClient} client.
 */
@Slf4j
public class StorageClientImpl extends AbstractAutoAsyncCloseable implements StorageClient {

    private static final String COMPONENT_NAME = StorageClientImpl.class.getSimpleName();

    private final String defaultNamespace;
    private final StorageClientSettings settings;
    private final ClientResources resources;
    private final OrderedScheduler scheduler;

    // clients
    private final StorageServerClientManager serverManager;
    private final boolean ownServerManager;

    StorageClientImpl(String namespaceName,
                      StorageClientSettings settings,
                      ClientResources resources) {
        this(
            namespaceName,
            settings,
            resources,
            new StorageServerClientManagerImpl(settings, resources.scheduler()),
            true);
    }

    public StorageClientImpl(String namespaceName,
                             StorageClientSettings settings,
                             ClientResources resources,
                             StorageServerClientManager serverManager,
                             boolean ownServerManager) {
        this.defaultNamespace = namespaceName;
        this.settings = settings;
        this.resources = resources;
        this.serverManager = serverManager;
        this.ownServerManager = ownServerManager;
        this.scheduler = SharedResourceManager.shared().get(resources.scheduler());
    }

    CompletableFuture<StreamProperties> getStreamProperties(String namespaceName,
                                                            String streamName) {
        return this.serverManager.getRootRangeClient().getStream(namespaceName, streamName);
    }

    //
    // Tables
    //

    @Override
    public CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String tableName) {
        return openPTable(defaultNamespace, tableName);
    }

    @Override
    public CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String namespaceName,
                                                                  String tableName) {
        return ExceptionUtils.callAndHandleClosedAsync(
            COMPONENT_NAME,
            isClosed(),
            (future) -> openTableImpl(namespaceName, tableName, future));
    }

    @Override
    public CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String table) {
        return openTable(defaultNamespace, table);
    }

    @Override
    public CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String namespaceName,
                                                                String table) {
        return openPTable(namespaceName, table)
            .thenApply(pTable -> new ByteBufTableImpl(pTable));
    }

    private void openTableImpl(String namespaceName,
                               String tableName,
                               CompletableFuture<PTable<ByteBuf, ByteBuf>> future) {
        FutureUtils.proxyTo(
            getStreamProperties(namespaceName, tableName).thenComposeAsync(props -> {
                if (log.isInfoEnabled()) {
                    log.info("Retrieved table properties for table {}/{} : {}", namespaceName, tableName, props);
                }
                if (StorageType.TABLE != props.getStreamConf().getStorageType()) {
                    return FutureUtils.exception(new ApiException(
                        "Can't open a non-table storage entity : " + props.getStreamConf().getStorageType())
                    );
                }
                return new PByteBufTableImpl(
                    tableName,
                    props,
                    serverManager,
                    scheduler.chooseThread(props.getStreamId()),
                    settings.backoffPolicy()
                ).initialize();
            }),
            future
        );
    }

    //
    // Closeable API
    //

    @Override
    protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
        scheduler.submit(() -> {
            if (ownServerManager) {
                serverManager.close();
            }
            closeFuture.complete(null);
            SharedResourceManager.shared().release(resources.scheduler(), scheduler);
        });
    }

    @Override
    public void close() {
        try {
            super.close(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Encountered exceptions on closing the storage client", e);
        }
        scheduler.forceShutdown(100, TimeUnit.MILLISECONDS);
    }
}
