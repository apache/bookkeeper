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

package org.apache.bookkeeper.clients.impl.channel;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.container.StorageContainerClientInterceptor;
import org.apache.bookkeeper.clients.resolver.EndpointResolver;
import org.apache.bookkeeper.clients.utils.GrpcUtils;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.storage.MetaRangeServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.MetaRangeServiceGrpc.MetaRangeServiceFutureStub;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceFutureStub;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc.StorageContainerServiceFutureStub;
import org.apache.bookkeeper.stream.proto.storage.TableServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.TableServiceGrpc.TableServiceFutureStub;

/**
 * A channel connected to a range server.
 *
 * <p>The channel is multiplexed for different rpc usage.
 */
public class StorageServerChannel implements AutoCloseable {

    public static Function<Endpoint, StorageServerChannel> factory(StorageClientSettings settings) {
        return (endpoint) -> new StorageServerChannel(
            endpoint,
            Optional.empty(),
            settings.usePlaintext(),
            settings.endpointResolver());
    }

    private final Optional<String> token;
    private final Channel channel;

    @GuardedBy("this")
    private RootRangeServiceFutureStub rootRangeService;
    @GuardedBy("this")
    private MetaRangeServiceFutureStub metaRangeService;
    @GuardedBy("this")
    private StorageContainerServiceFutureStub scService;
    @GuardedBy("this")
    private TableServiceFutureStub kvService;

    /**
     * Construct a range server channel to a given range server endpoint.
     *
     * @param endpoint range server endpoint.
     * @param token    token used to access range server
     * @param usePlainText whether to plain text protocol or not
     */
    public StorageServerChannel(Endpoint endpoint,
                                Optional<String> token,
                                boolean usePlainText,
                                EndpointResolver endpointResolver) {
        this.token = token;
        Endpoint resolvedEndpoint = endpointResolver.resolve(endpoint);
        this.channel = ManagedChannelBuilder.forAddress(
            resolvedEndpoint.getHostname(),
            resolvedEndpoint.getPort())
            .usePlaintext(usePlainText)
            .build();
    }

    @VisibleForTesting
    public StorageServerChannel(ManagedChannel channel,
                                Optional<String> token) {
        this((Channel) channel, token);
    }

    protected StorageServerChannel(Channel channel,
                                   Optional<String> token) {
        this.token = token;
        this.channel = channel;
    }

    public synchronized RootRangeServiceFutureStub getRootRangeService() {
        if (null == rootRangeService) {
            rootRangeService = GrpcUtils.configureGrpcStub(
                RootRangeServiceGrpc.newFutureStub(channel),
                token);
        }
        return rootRangeService;
    }

    public synchronized MetaRangeServiceFutureStub getMetaRangeService() {
        if (null == metaRangeService) {
            metaRangeService = GrpcUtils.configureGrpcStub(
                MetaRangeServiceGrpc.newFutureStub(channel),
                token);
        }
        return metaRangeService;
    }

    public synchronized StorageContainerServiceFutureStub getStorageContainerService() {
        if (null == scService) {
            scService = GrpcUtils.configureGrpcStub(
                StorageContainerServiceGrpc.newFutureStub(channel),
                token);
        }
        return scService;
    }

    public synchronized TableServiceFutureStub getTableService() {
        if (null == kvService) {
            kvService = GrpcUtils.configureGrpcStub(
                TableServiceGrpc.newFutureStub(channel),
                token);
        }
        return kvService;
    }

    /**
     * Create an intercepted server channel that add additional storage container metadata.
     *
     * @param scId storage container id
     * @return an intercepted server channel.
     */
    public StorageServerChannel intercept(long scId) {
        Channel interceptedChannel = ClientInterceptors.intercept(
            this.channel,
            new StorageContainerClientInterceptor(scId));

        return new StorageServerChannel(
            interceptedChannel,
            this.token);
    }

    @Override
    public void close() {
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdown();
        }
    }
}
