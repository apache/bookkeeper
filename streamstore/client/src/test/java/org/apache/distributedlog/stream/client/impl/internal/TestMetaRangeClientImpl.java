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

import static org.apache.distributedlog.stream.client.impl.internal.ProtocolInternalUtils.createActiveRanges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.client.grpc.GrpcClientTestBase;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannelManager;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannel;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannelManager;
import org.apache.distributedlog.stream.client.internal.api.HashStreamRanges;
import org.apache.distributedlog.stream.client.internal.api.LocationClient;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.rangeservice.GetActiveRangesResponse;
import org.apache.distributedlog.stream.proto.rangeservice.MetaRangeServiceGrpc.MetaRangeServiceImplBase;
import org.apache.distributedlog.stream.proto.rangeservice.RelatedRanges;
import org.apache.distributedlog.stream.proto.rangeservice.RelationType;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;
import org.junit.Test;

/**
 * Test Case for {@link MetaRangeClientImpl}.
 */
public class TestMetaRangeClientImpl extends GrpcClientTestBase {

  private static final long streamId = 1234L;
  private static final long groupId = 456L;
  private static final StreamProperties streamProps = StreamProperties.newBuilder()
    .setStreamId(streamId)
    .setStorageContainerId(groupId)
    .setStreamName("test-meta-range-client")
    .setStreamConf(StreamConfiguration.newBuilder().build())
    .build();
  private final LocationClient locationClient = mock(LocationClient.class);
  private MetaRangeClientImpl metaRangeClient;
  private final RangeServerChannel rsChannel = mock(RangeServerChannel.class);
  private final RangeServerChannelManager channelManager = new RangeServerChannelManager(
    ep -> rsChannel);

  @Override
  protected void doSetup() throws Exception {
    scheduler = OrderedScheduler.newSchedulerBuilder()
      .numThreads(1)
      .name("test-meta-range-client")
      .build();
    metaRangeClient = new MetaRangeClientImpl(
      streamProps,
      scheduler,
      new StorageContainerChannelManager(
        channelManager,
        locationClient,
        scheduler));
  }

  @Override
  protected void doTeardown() throws Exception {
    if (null != scheduler) {
      scheduler.shutdown();
    }
  }


  private RelatedRanges buildRelatedRange(long startKey,
                                          long endKey,
                                          long rangeId,
                                          long groupId,
                                          List<Long> parentRanges) {
    return RelatedRanges.newBuilder()
      .setProps(buildRangeMeta(
        startKey, endKey, rangeId, groupId))
      .setType(RelationType.PARENTS)
      .addAllRelatedRanges(parentRanges)
      .build();
  }


  private RangeProperties buildRangeMeta(long startKey,
                                         long endKey,
                                         long rangeId,
                                         long groupId) {
    return RangeProperties.newBuilder()
      .setStartHashKey(startKey)
      .setEndHashKey(endKey)
      .setRangeId(rangeId)
      .setStorageContainerId(groupId)
      .build();
  }

  @Test
  public void testGetActiveStreamRanges() throws Exception {
    CompletableFuture<RangeServerChannel> serviceFuture = FutureUtils.createFuture();
    metaRangeClient.getStorageContainerClient().setRangeServerChannelFuture(serviceFuture);

    // create response
    GetActiveRangesResponse getActiveRangesResponse = GetActiveRangesResponse.newBuilder()
      .addRanges(
        buildRelatedRange(Long.MIN_VALUE, 0L, 123L, 1L, Lists.newArrayList(113L))
      ).addRanges(
        buildRelatedRange(0L, Long.MAX_VALUE, 124L, 2L, Lists.newArrayList(114L))
      ).build();
    StorageContainerResponse response = StorageContainerResponse.newBuilder()
      .setCode(StatusCode.SUCCESS)
      .setGetActiveRangesResp(getActiveRangesResponse)
      .build();

    MetaRangeServiceImplBase metaRangeService = new MetaRangeServiceImplBase() {
      @Override
      public void getActiveRanges(StorageContainerRequest request,
                                  StreamObserver<StorageContainerResponse> responseObserver) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    };
    serviceRegistry.addService(metaRangeService.bindService());

    RangeServerChannel rsChannel = new RangeServerChannel(
      InProcessChannelBuilder.forName(serverName).directExecutor().build(),
      Optional.empty());
    serviceFuture.complete(rsChannel);

    HashStreamRanges expectedStream = createActiveRanges(response.getGetActiveRangesResp());
    CompletableFuture<HashStreamRanges> getFuture = metaRangeClient.getActiveDataRanges();
    assertEquals(expectedStream, getFuture.get());
  }

  @Test
  public void testGetActiveStreamRangesFailure() throws Exception {
    CompletableFuture<RangeServerChannel> serviceFuture = FutureUtils.createFuture();
    metaRangeClient.getStorageContainerClient().setRangeServerChannelFuture(serviceFuture);

    MetaRangeServiceImplBase metaRangeService = new MetaRangeServiceImplBase() {
      @Override
      public void getActiveRanges(StorageContainerRequest request,
                                  StreamObserver<StorageContainerResponse> responseObserver) {
        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL));
      }
    };
    serviceRegistry.addService(metaRangeService.bindService());

    RangeServerChannel rsChannel = new RangeServerChannel(
      InProcessChannelBuilder.forName(serverName).directExecutor().build(),
      Optional.empty());
    serviceFuture.complete(rsChannel);

    CompletableFuture<HashStreamRanges> getFuture = metaRangeClient.getActiveDataRanges();
    try {
      getFuture.get();
      fail("should fail on rpc failure");
    } catch (ExecutionException ee) {
      assertNotNull(ee.getCause());
      assertTrue(ee.getCause() instanceof StatusRuntimeException);
      StatusRuntimeException se = (StatusRuntimeException) ee.getCause();
      assertEquals(Status.INTERNAL, se.getStatus());
    }
  }

}
