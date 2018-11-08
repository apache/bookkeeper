/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.server.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.HandlerRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.grpc.proxy.ProxyHandlerRegistry;
import org.apache.bookkeeper.common.grpc.stats.MonitoringServerInterceptor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.server.exceptions.StorageServerRuntimeException;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.impl.grpc.GrpcServices;

/**
 * KeyRange Server.
 */
@Slf4j
public class GrpcServer extends AbstractLifecycleComponent<StorageServerConfiguration> {

    public static GrpcServer build(GrpcServerSpec spec) {
        return new GrpcServer(
            spec.storeSupplier().get(),
            spec.storeServerConf(),
            spec.endpoint(),
            spec.localServerName(),
            spec.localHandlerRegistry(),
            spec.statsLogger());
    }

    private final Endpoint myEndpoint;
    private final Server grpcServer;

    public GrpcServer(StorageContainerStore storageContainerStore,
                      StorageServerConfiguration conf,
                      Endpoint myEndpoint,
                      StatsLogger statsLogger) {
        this(storageContainerStore, conf, myEndpoint, null, null, statsLogger);
    }

    @VisibleForTesting
    public GrpcServer(StorageContainerStore storageContainerStore,
                      StorageServerConfiguration conf,
                      Endpoint myEndpoint,
                      String localServerName,
                      HandlerRegistry localHandlerRegistry,
                      StatsLogger statsLogger) {
        super("range-grpc-server", conf, statsLogger);
        this.myEndpoint = myEndpoint;
        if (null != localServerName) {
            InProcessServerBuilder serverBuilder = InProcessServerBuilder
                .forName(localServerName)
                .directExecutor();
            if (null != localHandlerRegistry) {
                serverBuilder = serverBuilder.fallbackHandlerRegistry(localHandlerRegistry);
            }
            this.grpcServer = serverBuilder.build();
        } else {
            MonitoringServerInterceptor monitoringInterceptor =
                MonitoringServerInterceptor.create(statsLogger.scope("services"), true);
            ProxyHandlerRegistry.Builder proxyRegistryBuilder = ProxyHandlerRegistry.newBuilder()
                .setChannelFinder(storageContainerStore);
            for (ServerServiceDefinition definition : GrpcServices.create(null)) {
                ServerServiceDefinition monitoredService = ServerInterceptors.intercept(
                    definition,
                    monitoringInterceptor
                );
                proxyRegistryBuilder = proxyRegistryBuilder.addService(monitoredService);
            }
            ServerServiceDefinition locationService = ServerInterceptors.intercept(
                new GrpcStorageContainerService(storageContainerStore),
                monitoringInterceptor
            );
            this.grpcServer = ServerBuilder.forPort(this.myEndpoint.getPort())
                .addService(locationService)
                .fallbackHandlerRegistry(proxyRegistryBuilder.build())
                .build();
        }
    }

    @VisibleForTesting
    Server getGrpcServer() {
        return grpcServer;
    }

    @Override
    protected void doStart() {
        try {
            grpcServer.start();
        } catch (IOException e) {
            log.error("Failed to start grpc server", e);
            throw new StorageServerRuntimeException("Failed to start grpc server", e);
        }
    }

    @Override
    protected void doStop() {
        grpcServer.shutdown();
    }

    @Override
    protected void doClose() throws IOException {
    }

}
