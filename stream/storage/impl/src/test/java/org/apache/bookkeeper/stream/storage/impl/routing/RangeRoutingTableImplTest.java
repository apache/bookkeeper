/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.routing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils;
import org.apache.bookkeeper.clients.impl.routing.RangeRouter;
import org.apache.bookkeeper.common.router.BytesHashRouter;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.MetaRangeServiceGrpc.MetaRangeServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.RelatedRanges;
import org.apache.bookkeeper.stream.proto.storage.RelationType;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.util.ProtoUtils;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.junit.Test;

/**
 * Unit test {@link RangeRoutingTable}.
 */
public class RangeRoutingTableImplTest extends GrpcClientTestBase {

    private final long scId = 1234L;
    private final long streamId = 123456L;
    private GetActiveRangesResponse getActiveRangesResponse;
    private CompletableFuture<GetActiveRangesResponse> responseSupplier;
    private StreamProperties props;
    private List<RangeProperties> rangeProps;
    private RangeRoutingTableImpl routingTable;
    private RangeRouter<byte[]> rangeRouter;

    @Override
    protected void doSetup() throws Exception {
        this.props = StreamProperties.newBuilder()
            .setStorageContainerId(scId)
            .setStreamId(streamId)
            .setStreamName("metaclient-stream")
            .setStreamConf(StreamConfiguration.newBuilder().build())
            .build();
        this.rangeProps = ProtoUtils.split(
            streamId,
            24,
            23456L,
            StorageContainerPlacementPolicyImpl.of(4)
        );
        final GetActiveRangesResponse.Builder getActiveRangesResponseBuilder = GetActiveRangesResponse.newBuilder();
        for (RangeProperties range : rangeProps) {
            RelatedRanges.Builder rrBuilder = RelatedRanges.newBuilder()
                .setProps(range)
                .setType(RelationType.PARENTS)
                .addAllRelatedRanges(Collections.emptyList());
            getActiveRangesResponseBuilder.addRanges(rrBuilder);
        }
        this.getActiveRangesResponse = getActiveRangesResponseBuilder
            .setCode(StatusCode.SUCCESS)
            .build();
        RootRangeServiceImplBase rootRangeService = new RootRangeServiceImplBase() {
            @Override
            public void getStream(GetStreamRequest request,
                                  StreamObserver<GetStreamResponse> responseObserver) {
                responseObserver.onNext(GetStreamResponse.newBuilder()
                    .setCode(StatusCode.SUCCESS)
                    .setStreamProps(props)
                    .build());
                responseObserver.onCompleted();
            }
        };
        serviceRegistry.addService(rootRangeService);

        this.responseSupplier = new CompletableFuture<>();
        // register a good meta range service
        MetaRangeServiceImplBase metaRangeService = new MetaRangeServiceImplBase() {
            @Override
            public void getActiveRanges(GetActiveRangesRequest request,
                                        StreamObserver<GetActiveRangesResponse> responseObserver) {
                try {
                    responseObserver.onNext(responseSupplier.get());
                    responseObserver.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(e);
                } catch (ExecutionException e) {
                    responseObserver.onError(e);
                }
            }
        };
        serviceRegistry.addService(metaRangeService);

        this.routingTable = new RangeRoutingTableImpl(serverManager);
        this.rangeRouter = new RangeRouter<>(BytesHashRouter.of());
        this.rangeRouter.setRanges(ProtocolInternalUtils.createActiveRanges(getActiveRangesResponse));
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    @Test
    public void testGetRange() throws Exception {
        String key = "foo";
        byte[] keyBytes = key.getBytes(UTF_8);
        RangeProperties rangeProps = routingTable.getRange(streamId, keyBytes);
        // the first get will return null since there is nothing in
        assertNull(rangeProps);
        // the fetch request is outstanding
        CompletableFuture<RangeRouter<byte[]>> outstandingFetchFuture =
            routingTable.getOutstandingFetchRequest(streamId);
        assertNotNull(outstandingFetchFuture);
        assertFalse(outstandingFetchFuture.isDone());

        // complete the response supplier, so the fetch request can complete to update the cache
        responseSupplier.complete(getActiveRangesResponse);

        // wait until the stuff is cached.
        while (null == routingTable.getRangeRouter(streamId)) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // if the router is created, it should return the cached router
        rangeProps = routingTable.getRange(streamId, keyBytes);
        assertNotNull(rangeProps);
        assertEquals(rangeRouter.getRangeProperties(keyBytes), rangeProps);
    }

    @Test
    public void testGetRangeException() throws Exception {
        String key = "foo";
        byte[] keyBytes = key.getBytes(UTF_8);
        RangeProperties rangeProps = routingTable.getRange(streamId, keyBytes);
        // the first get will return null since there is nothing in
        assertNull(rangeProps);
        // the fetch request is outstanding
        CompletableFuture<RangeRouter<byte[]>> outstandingFetchFuture =
            routingTable.getOutstandingFetchRequest(streamId);
        assertNotNull(outstandingFetchFuture);
        assertFalse(outstandingFetchFuture.isDone());

        // complete the response supplier, so the fetch request can complete to update the cache
        responseSupplier.completeExceptionally(new Exception("fetch failed"));

        // wait until the fetch is done.
        try {
            outstandingFetchFuture.get();
            fail("Fetch request should fail");
        } catch (Exception e) {
            // expected
        }

        // once the fetch is done, nothing should be cached and the outstanding fetch request should be removed
        assertNull(routingTable.getRangeRouter(streamId));
        assertNull(routingTable.getOutstandingFetchRequest(streamId));
    }

}
