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

package org.apache.distributedlog.stream.client.impl.channel;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import org.apache.distributedlog.stream.client.utils.GrpcUtils;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.proto.rangeservice.MetaRangeServiceGrpc;
import org.apache.distributedlog.stream.proto.rangeservice.MetaRangeServiceGrpc.MetaRangeServiceFutureStub;
import org.apache.distributedlog.stream.proto.rangeservice.RootRangeServiceGrpc;
import org.apache.distributedlog.stream.proto.rangeservice.RootRangeServiceGrpc.RootRangeServiceFutureStub;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerServiceGrpc;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerServiceGrpc.StorageContainerServiceFutureStub;
import org.apache.distributedlog.stream.proto.rangeservice.TableServiceGrpc;
import org.apache.distributedlog.stream.proto.rangeservice.TableServiceGrpc.TableServiceFutureStub;

/**
 * A channel connected to a range server.
 *
 * <p>The channel is multiplexed for different rpc usage.
 */
public class RangeServerChannel implements AutoCloseable {

  public static Function<Endpoint, RangeServerChannel> factory(boolean usePlaintext) {
    return (endpoint) -> new RangeServerChannel(endpoint, Optional.empty(), usePlaintext);
  }

  private final Optional<String> token;
  private final ManagedChannel channel;

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
   * @param token token used to access range server
   */
  public RangeServerChannel(Endpoint endpoint,
                            Optional<String> token,
                            boolean usePlainText) {
    this.token = token;
    this.channel = ManagedChannelBuilder.forAddress(
      endpoint.getHostname(),
      endpoint.getPort())
      .usePlaintext(usePlainText)
      .build();
  }

  @VisibleForTesting
  public RangeServerChannel(ManagedChannel channel,
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

  @Override
  public void close() {
    channel.shutdown();
  }
}
