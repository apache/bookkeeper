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

package org.apache.distributedlog.stream.client.grpc;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Optional;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.client.ClientResources;
import org.apache.distributedlog.stream.client.StreamSettings;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannel;
import org.apache.distributedlog.stream.client.impl.internal.RangeServerClientManagerImpl;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.proto.rangeservice.GetStorageContainerEndpointRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetStorageContainerEndpointResponse;
import org.apache.distributedlog.stream.proto.rangeservice.OneStorageContainerEndpointRequest;
import org.apache.distributedlog.stream.proto.rangeservice.OneStorageContainerEndpointResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerEndpoint;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerServiceGrpc.StorageContainerServiceImplBase;
import org.junit.After;
import org.junit.Before;

/**
 * Test Base for Grpc related tests.
 */
public abstract class GrpcClientTestBase {

  protected static final Endpoint ENDPOINT = Endpoint.newBuilder()
    .setHostname("127.0.0.1")
    .setPort(4181)
    .build();

  protected String serverName = "fake server for " + getClass();
  protected final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  protected Server fakeServer;
  protected OrderedScheduler scheduler;

  protected StreamSettings settings;
  protected final ClientResources resources = ClientResources.create();
  protected RangeServerClientManagerImpl serverManager;

  @Before
  public void setUp() throws Exception {
    fakeServer = InProcessServerBuilder
      .forName(serverName)
      .fallbackHandlerRegistry(serviceRegistry)
      .directExecutor()
      .build()
      .start();
    scheduler = OrderedScheduler.newSchedulerBuilder()
      .name("scheduler-" + getClass())
      .numThreads(Runtime.getRuntime().availableProcessors())
      .build();
    settings = StreamSettings.newBuilder()
      .managedChannelBuilder(InProcessChannelBuilder.forName(serverName).directExecutor())
      .usePlaintext(true)
      .build();
    serverManager = new RangeServerClientManagerImpl(
      settings,
      resources.scheduler(),
      endpoint -> new RangeServerChannel(
        InProcessChannelBuilder.forName(serverName).directExecutor().build(),
        Optional.empty()));
    StorageContainerServiceImplBase scService = new StorageContainerServiceImplBase() {
      @Override
      public void getStorageContainerEndpoint(GetStorageContainerEndpointRequest request,
                                        StreamObserver<GetStorageContainerEndpointResponse> responseObserver) {
        GetStorageContainerEndpointResponse.Builder respBuilder = GetStorageContainerEndpointResponse.newBuilder();
        respBuilder.setStatusCode(StatusCode.SUCCESS);
        for (OneStorageContainerEndpointRequest oneReq : request.getRequestsList()) {
          OneStorageContainerEndpointResponse oneResp = OneStorageContainerEndpointResponse.newBuilder()
            .setEndpoint(StorageContainerEndpoint.newBuilder()
              .setStorageContainerId(oneReq.getStorageContainer())
              .setRevision(oneReq.getRevision() + 1)
              .setRwEndpoint(ENDPOINT))
            .build();
          respBuilder.addResponses(oneResp);
        }
        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
      }
    };
    serviceRegistry.addService(scService.bindService());

    doSetup();
  }

  protected abstract void doSetup() throws Exception;

  @After
  public void tearDown() throws Exception {
    doTeardown();
    if (null != serverManager) {
      serverManager.close();
    }
    if (null != fakeServer) {
      fakeServer.shutdown();
    }
    if (null != scheduler) {
      scheduler.shutdown();
    }
  }

  protected abstract void doTeardown() throws Exception;

}
