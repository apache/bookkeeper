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

package org.apache.bookkeeper.clients.impl.internal;

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.GetStorageContainerEndpointsFunction;
import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStorageContainerEndpointRequest;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.internal.api.LocationClient;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.clients.utils.GrpcChannels;
import org.apache.bookkeeper.clients.utils.GrpcUtils;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.Retries;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc.StorageContainerServiceFutureStub;

/**
 * Default Implementation of {@link LocationClient}.
 */
public class LocationClientImpl implements LocationClient {

    private final StorageClientSettings settings;
    private final OrderedScheduler scheduler;
    private final ManagedChannel channel;
    private final StorageContainerServiceFutureStub locationService;

    @SuppressWarnings("deprecation")
    public LocationClientImpl(StorageClientSettings settings,
                              OrderedScheduler scheduler) {
        this.settings = settings;
        this.scheduler = scheduler;
        this.channel = GrpcChannels.createChannelBuilder(
            settings.serviceUri(), settings
        ).build();
        this.locationService = GrpcUtils.configureGrpcStub(
            StorageContainerServiceGrpc.newFutureStub(channel),
            Optional.empty());
    }

    private Stream<Long> getDefaultBackoffs() {
        return Backoff.exponential(
            ClientConstants.DEFAULT_BACKOFF_START_MS,
            ClientConstants.DEFAULT_BACKOFF_MULTIPLIER,
            ClientConstants.DEFAULT_BACKOFF_MAX_MS);
    }

    @VisibleForTesting
    static final Predicate<Throwable> LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE =
        cause -> shouldRetryOnException(cause);

    private static boolean shouldRetryOnException(Throwable cause) {
        if (cause instanceof StatusRuntimeException || cause instanceof StatusException) {
            Status status;
            if (cause instanceof StatusException) {
                status = ((StatusException) cause).getStatus();
            } else {
                status = ((StatusRuntimeException) cause).getStatus();
            }
            switch (status.getCode()) {
                case INVALID_ARGUMENT:
                case ALREADY_EXISTS:
                case PERMISSION_DENIED:
                case UNAUTHENTICATED:
                    return false;
                default:
                    return true;
            }
        } else if (cause instanceof RuntimeException) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public CompletableFuture<List<OneStorageContainerEndpointResponse>>
    locateStorageContainers(List<Revisioned<Long>> storageContainerIds) {
        GetStorageContainerEndpointRequest request = createGetStorageContainerEndpointRequest(storageContainerIds);
        return Retries.run(
            getDefaultBackoffs(),
            LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE,
            () -> fromListenableFuture(
                locationService.getStorageContainerEndpoint(request),
                GetStorageContainerEndpointsFunction),
            scheduler,
            request);
    }

    @Override
    public void close() {
        channel.shutdown();
    }

}
