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

package org.apache.bookkeeper.clients.impl.container;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannelManager;
import org.apache.bookkeeper.clients.impl.internal.api.LocationClient;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;
import org.junit.Test;

/**
 * Test Case for {@link StorageContainerChannel}.
 */
public class TestStorageContainerChannel extends GrpcClientTestBase {

    private OrderedScheduler scheduler;
    private final LocationClient locationClient = mock(LocationClient.class);

    private StorageServerChannel mockChannel = newMockServerChannel();
    private StorageServerChannel mockChannel2 = newMockServerChannel();
    private StorageServerChannel mockChannel3 = newMockServerChannel();
    private final Endpoint endpoint = Endpoint.newBuilder()
        .setHostname("127.0.0.1")
        .setPort(8181)
        .build();
    private final Endpoint endpoint2 = Endpoint.newBuilder()
        .setHostname("127.0.0.2")
        .setPort(8282)
        .build();
    private final Endpoint endpoint3 = Endpoint.newBuilder()
        .setHostname("127.0.0.3")
        .setPort(8383)
        .build();
    private final StorageServerChannelManager channelManager = new StorageServerChannelManager(
        ep -> {
            if (endpoint2 == ep) {
                return mockChannel2;
            } else if (endpoint3 == ep) {
                return mockChannel3;
            } else {
                return mockChannel;
            }
        });

    private StorageContainerChannel scClient;

    @Override
    protected void doSetup() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .numThreads(1)
            .name("test-range-server-manager")
            .build();
        scClient = new StorageContainerChannel(
            ROOT_STORAGE_CONTAINER_ID,
            channelManager,
            locationClient,
            scheduler.chooseThread(ROOT_STORAGE_CONTAINER_ID));
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    private StorageServerChannel newMockServerChannel() {
        StorageServerChannel channel = mock(StorageServerChannel.class);
        when(channel.intercept(anyLong())).thenReturn(channel);
        return channel;
    }

    private void ensureCallbackExecuted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        scheduler.submit(() -> latch.countDown());
        latch.await();
    }

    @Test
    public void testGetRootRangeServiceSuccess() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses = FutureUtils.createFuture();
        when(locationClient.locateStorageContainers(anyList())).thenReturn(locateResponses);

        // the future is not set before #getRootRangeService
        assertNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        // call #getRootRangeService
        CompletableFuture<StorageServerChannel> rsChannelFuture = scClient.getStorageContainerChannelFuture();
        // the future is set and the locationClient#locateStorageContainers is called
        assertNotNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // if the request is outstanding, a second call will not call locationClient#locateStorageContainers
        CompletableFuture<StorageServerChannel> rsChannelFuture1 = scClient.getStorageContainerChannelFuture();
        assertTrue(rsChannelFuture == rsChannelFuture1);
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // prepare the result and complete the request
        OneStorageContainerEndpointResponse oneResp = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(1000L)
                    .setRwEndpoint(endpoint)
                    .addRoEndpoint(endpoint)
                    .build())
            .build();
        locateResponses.complete(Lists.newArrayList(oneResp));
        // get the service
        StorageServerChannel rsChannel = rsChannelFuture.get();
        assertTrue(rsChannel == mockChannel);
        // verify storage container info
        StorageContainerInfo scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1000L, scInfo.getRevision());
        assertEquals(endpoint, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint, endpoint), scInfo.getReadEndpoints());
        // verify channel
        assertEquals(mockChannel, channelManager.getChannel(endpoint));

        verify(locationClient, times(1)).locateStorageContainers(anyList());
    }

    @Test
    public void testGetRootRangeServiceFailureWhenClosingChannelManager() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses = FutureUtils.createFuture();
        when(locationClient.locateStorageContainers(anyList())).thenReturn(locateResponses);

        // the future is not set before #getRootRangeService
        assertNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        // call #getRootRangeService
        CompletableFuture<StorageServerChannel> rsChannelFuture = scClient.getStorageContainerChannelFuture();
        // the future is set and the locationClient#locateStorageContainers is called
        assertNotNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // if the request is outstanding, a second call will not call locationClient#locateStorageContainers
        CompletableFuture<StorageServerChannel> rsChannelFuture1 = scClient.getStorageContainerChannelFuture();
        assertTrue(rsChannelFuture == rsChannelFuture1);
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // closing the channel manager
        channelManager.close();
        // prepare the result and complete the request
        OneStorageContainerEndpointResponse oneResp = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(1000L)
                    .setRwEndpoint(endpoint)
                    .addRoEndpoint(endpoint)
                    .build())
            .build();
        locateResponses.complete(Lists.newArrayList(oneResp));
        // verify the result
        try {
            rsChannelFuture.get();
            fail("Should fail get root range service if channel manager is shutting down.");
        } catch (ExecutionException ee) {
            assertNotNull(ee.getCause());
            assertTrue(ee.getCause() instanceof ObjectClosedException);
        }
        // verify storage container info
        StorageContainerInfo scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1000L, scInfo.getRevision());
        assertEquals(endpoint, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint, endpoint), scInfo.getReadEndpoints());
        // verify channel
        assertNull(channelManager.getChannel(endpoint));

        verify(locationClient, times(1)).locateStorageContainers(anyList());
    }

    @Test
    public void testGetRootRangeServiceFailureOnStaleGroupInfo() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses1 = FutureUtils.createFuture();
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses2 = FutureUtils.createFuture();
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses3 = FutureUtils.createFuture();
        when(locationClient.locateStorageContainers(anyList()))
            .thenReturn(locateResponses1)
            .thenReturn(locateResponses3);

        // the future is not set before #getRootRangeService
        assertNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        // call #getRootRangeService
        CompletableFuture<StorageServerChannel> rsChannelFuture = scClient.getStorageContainerChannelFuture();
        // the future is set and the locationClient#locateStorageContainers is called
        assertNotNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // if the request is outstanding, a second call will not call locationClient#locateStorageContainers
        CompletableFuture<StorageServerChannel> rsChannelFuture1 = scClient.getStorageContainerChannelFuture();
        assertTrue(rsChannelFuture == rsChannelFuture1);
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());

        //
        // Complete the first response
        //

        // prepare the result and complete the request
        OneStorageContainerEndpointResponse oneResp1 = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(1000L)
                    .setRwEndpoint(endpoint)
                    .addRoEndpoint(endpoint)
                    .build())
            .build();
        locateResponses1.complete(Lists.newArrayList(oneResp1));
        // get the service
        StorageServerChannel rsChannel = rsChannelFuture.get();
        assertTrue(rsChannel == mockChannel);
        // verify storage container info
        StorageContainerInfo scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1000L, scInfo.getRevision());
        assertEquals(endpoint, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint, endpoint), scInfo.getReadEndpoints());
        // verify channel
        assertEquals(mockChannel, channelManager.getChannel(endpoint));

        //
        // Reset and complete the second response
        //

        scClient.resetStorageServerChannelFuture();
        rsChannelFuture = scClient.getStorageContainerChannelFuture();

        OneStorageContainerEndpointResponse oneResp2 = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(999L)
                    .setRwEndpoint(endpoint2)
                    .addRoEndpoint(endpoint2)
                    .build())
            .build();
        locateResponses2.complete(Lists.newArrayList(oneResp2));
        ensureCallbackExecuted();

        // verify storage container info : group info will not be updated
        scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1000L, scInfo.getRevision());
        assertEquals(endpoint, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint, endpoint), scInfo.getReadEndpoints());
        // the future will not be completed
        assertFalse(rsChannelFuture.isDone());

        //
        // complete the third response
        //

        scClient.resetStorageServerChannelFuture();
        rsChannelFuture = scClient.getStorageContainerChannelFuture();

        OneStorageContainerEndpointResponse oneResp3 = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(1001L)
                    .setRwEndpoint(endpoint3)
                    .addRoEndpoint(endpoint3)
                    .build())
            .build();
        locateResponses3.complete(Lists.newArrayList(oneResp3));
        ensureCallbackExecuted();

        StorageServerChannel rsChannel3 = rsChannelFuture.get();
        assertTrue(rsChannel3 == mockChannel3);
        // verify storage container info : group info will not be updated
        scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1001L, scInfo.getRevision());
        assertEquals(endpoint3, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint3, endpoint3), scInfo.getReadEndpoints());

        verify(locationClient, times(3)).locateStorageContainers(anyList());
    }

    @Test
    public void testGetRootRangeServiceUnexpectedException() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses1 = FutureUtils.createFuture();
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses2 = FutureUtils.createFuture();
        when(locationClient.locateStorageContainers(anyList()))
            .thenReturn(locateResponses1)
            .thenReturn(locateResponses2);

        // the future is not set before #getRootRangeService
        assertNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        // call #getRootRangeService
        CompletableFuture<StorageServerChannel> rsChannelFuture = scClient.getStorageContainerChannelFuture();
        // the future is set and the locationClient#locateStorageContainers is called
        assertNotNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // if the request is outstanding, a second call will not call locationClient#locateStorageContainers
        CompletableFuture<StorageServerChannel> rsChannelFuture1 = scClient.getStorageContainerChannelFuture();
        assertTrue(rsChannelFuture == rsChannelFuture1);
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // prepare the result and complete the request
        OneStorageContainerEndpointResponse oneResp = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(1000L)
                    .setRwEndpoint(endpoint)
                    .addRoEndpoint(endpoint)
                    .build())
            .build();
        // complete with wrong responses
        locateResponses1.complete(Lists.newArrayList(oneResp, oneResp));
        ensureCallbackExecuted();
        // verify channel
        assertNull(channelManager.getChannel(endpoint));
        // verify storage container info
        assertNull(scClient.getStorageContainerInfo());

        // complete with right responses
        locateResponses2.complete(Lists.newArrayList(oneResp));

        // get the service
        StorageServerChannel rsChannel = rsChannelFuture.get();
        assertTrue(rsChannel == mockChannel);
        // verify storage container info
        StorageContainerInfo scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1000L, scInfo.getRevision());
        assertEquals(endpoint, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint, endpoint), scInfo.getReadEndpoints());
        // verify channel
        assertEquals(mockChannel, channelManager.getChannel(endpoint));

        verify(locationClient, times(2)).locateStorageContainers(anyList());
    }

    @Test
    public void testGetRootRangeServiceExceptionally() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses1 = FutureUtils.createFuture();
        CompletableFuture<List<OneStorageContainerEndpointResponse>> locateResponses2 = FutureUtils.createFuture();
        when(locationClient.locateStorageContainers(anyList()))
            .thenReturn(locateResponses1)
            .thenReturn(locateResponses2);

        // the future is not set before #getRootRangeService
        assertNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        // call #getRootRangeService
        CompletableFuture<StorageServerChannel> rsChannelFuture = scClient.getStorageContainerChannelFuture();
        // the future is set and the locationClient#locateStorageContainers is called
        assertNotNull(scClient.getStorageServerChannelFuture());
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // if the request is outstanding, a second call will not call locationClient#locateStorageContainers
        CompletableFuture<StorageServerChannel> rsChannelFuture1 = scClient.getStorageContainerChannelFuture();
        assertTrue(rsChannelFuture == rsChannelFuture1);
        assertNull(scClient.getStorageContainerInfo());
        verify(locationClient, times(1)).locateStorageContainers(anyList());
        // prepare the result and complete the request
        OneStorageContainerEndpointResponse oneResp = OneStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS)
            .setEndpoint(
                StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(ROOT_STORAGE_CONTAINER_ID)
                    .setRevision(1000L)
                    .setRwEndpoint(endpoint)
                    .addRoEndpoint(endpoint)
                    .build())
            .build();
        // complete exceptionally
        locateResponses1.completeExceptionally(new ClientException("test-exception"));
        ensureCallbackExecuted();
        // verify channel
        assertNull(channelManager.getChannel(endpoint));
        // verify storage container info
        assertNull(scClient.getStorageContainerInfo());

        // complete with right responses
        locateResponses2.complete(Lists.newArrayList(oneResp));

        // get the service
        StorageServerChannel rsChannel = rsChannelFuture.get();
        assertTrue(rsChannel == mockChannel);
        // verify storage container info
        StorageContainerInfo scInfo = scClient.getStorageContainerInfo();
        assertEquals(ROOT_STORAGE_CONTAINER_ID, scInfo.getGroupId());
        assertEquals(1000L, scInfo.getRevision());
        assertEquals(endpoint, scInfo.getWriteEndpoint());
        assertEquals(Lists.newArrayList(endpoint, endpoint), scInfo.getReadEndpoints());
        // verify channel
        assertEquals(mockChannel, channelManager.getChannel(endpoint));

        verify(locationClient, times(2)).locateStorageContainers(anyList());
    }

}
