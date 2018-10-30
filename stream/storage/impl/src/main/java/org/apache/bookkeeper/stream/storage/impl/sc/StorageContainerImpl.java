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

package org.apache.bookkeeper.stream.storage.impl.sc;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerService;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerServiceFactory;
import org.apache.bookkeeper.stream.storage.impl.routing.RoutingHeaderProxyInterceptor;

/**
 * The default implementation of {@link StorageContainer}.
 */
@Slf4j
class StorageContainerImpl implements StorageContainer {

    private final String containerName;
    private final StorageContainerService service;
    private final long scId;
    private final Server grpcServer;
    private volatile Channel channel = Channel404.of();

    StorageContainerImpl(StorageContainerServiceFactory serviceFactory, long scId) {
        this.scId = scId;
        this.service = serviceFactory.createStorageContainerService(scId);
        this.containerName = "container-" + scId;
        this.grpcServer = buildGrpcServer(containerName, service.getRegisteredServices());
    }

    private static Server buildGrpcServer(String serverName,
                                          Collection<ServerServiceDefinition> services) {
        InProcessServerBuilder builder = InProcessServerBuilder.forName(serverName);
        services.forEach(service -> builder.addService(service));
        return builder
            .directExecutor()
            .build();
    }

    //
    // Services
    //

    @Override
    public long getId() {
        return scId;
    }

    @Override
    public CompletableFuture<StorageContainer> start() {
        log.info("Starting storage container ({}) ...", getId());

        return service.start().thenCompose(ignored -> {
            try {
                grpcServer.start();
                log.info("Successfully started storage container ({}).", getId());

                channel = InProcessChannelBuilder.forName(containerName)
                    .usePlaintext()
                    .directExecutor()
                    // attach routing header interceptor
                    .intercept(new RoutingHeaderProxyInterceptor())
                    .build();
                return FutureUtils.value(StorageContainerImpl.this);
            } catch (IOException e) {
                log.error("Failed to start the grpc server for storage container ({})", getId(), e);
                return FutureUtils.exception(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> stop() {
        log.info("Stopping storage container ({}) ...", getId());

        Channel existingChannel = channel;

        // set channel to 404
        channel = Channel404.of();

        if (null != existingChannel) {
            if (existingChannel instanceof ManagedChannel) {
                ((ManagedChannel) existingChannel).shutdown();
            }
        }

        if (null != grpcServer) {
            grpcServer.shutdown();
        }

        return service.stop().thenApply(ignored -> {

            log.info("Successfully stopped storage container ({}).", getId());
            return null;
        });
    }

    //
    // Grpc
    //

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void close() {
        stop().join();
    }

}
