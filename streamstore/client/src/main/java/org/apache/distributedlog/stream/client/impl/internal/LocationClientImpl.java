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

package org.apache.distributedlog.stream.client.impl.internal;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.fromListenableFuture;
import static org.apache.distributedlog.stream.client.impl.internal.ProtocolInternalUtils.GetStorageContainerEndpointsFunction;
import static org.apache.distributedlog.stream.client.utils.ClientConstants.DEFAULT_BACKOFF_MAX_MS;
import static org.apache.distributedlog.stream.client.utils.ClientConstants.DEFAULT_BACKOFF_MULTIPLIER;
import static org.apache.distributedlog.stream.client.utils.ClientConstants.DEFAULT_BACKOFF_START_MS;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetStorageContainerEndpointRequest;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.Retries;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.distributedlog.stream.client.StreamSettings;
import org.apache.distributedlog.stream.client.internal.api.LocationClient;
import org.apache.distributedlog.stream.client.resolver.SimpleStreamResolverFactory;
import org.apache.distributedlog.stream.client.utils.GrpcUtils;
import org.apache.distributedlog.stream.proto.rangeservice.GetStorageContainerEndpointRequest;
import org.apache.distributedlog.stream.proto.rangeservice.OneStorageContainerEndpointResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerServiceGrpc;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerServiceGrpc.StorageContainerServiceFutureStub;

/**
 * Default Implementation of {@link LocationClient}.
 */
public class LocationClientImpl implements LocationClient {

  private final StreamSettings settings;
  private final OrderedScheduler scheduler;
  private final ManagedChannel channel;
  private final StorageContainerServiceFutureStub locationService;

  public LocationClientImpl(StreamSettings settings,
                            OrderedScheduler scheduler) {
    this.settings = settings;
    this.scheduler = scheduler;
    ManagedChannelBuilder builder = settings.managedChannelBuilder().orElse(
      ManagedChannelBuilder
        .forTarget("stream")
        .nameResolverFactory(getResolver(settings))
    );
    if (settings.usePlaintext()) {
      builder = builder.usePlaintext(true);
    }
    this.channel = builder.build();
    this.locationService = GrpcUtils.configureGrpcStub(
      StorageContainerServiceGrpc.newFutureStub(channel),
      Optional.empty());
  }

  private NameResolver.Factory getResolver(StreamSettings settings) {
    if (settings.nameResolverFactory().isPresent()) {
      return settings.nameResolverFactory().get();
    } else {
      return SimpleStreamResolverFactory.of(settings.endpoints());
    }
  }

  private Stream<Long> getDefaultBackoffs() {
    return Backoff.exponential(
      DEFAULT_BACKOFF_START_MS,
      DEFAULT_BACKOFF_MULTIPLIER,
      DEFAULT_BACKOFF_MAX_MS);
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
        case NOT_FOUND:
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
      scheduler);
  }

  @Override
  public void close() {
    channel.shutdown();
  }

}
