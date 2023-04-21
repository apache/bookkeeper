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

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createRootRangeException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.InvalidNamespaceNameException;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamExistsException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannelManager;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannelManager;
import org.apache.bookkeeper.clients.impl.internal.api.LocationClient;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * The Test Base of {@link RootRangeClientImpl}.
 */
public abstract class RootRangeClientImplTestBase extends GrpcClientTestBase {

    @Rule
    public final TestName testName = new TestName();

    private OrderedScheduler scheduler;
    private RootRangeClientImpl rootRangeClient;
    private final LocationClient locationClient = mock(LocationClient.class);

    private StorageServerChannel mockChannel = mock(StorageServerChannel.class);
    private StorageServerChannel mockChannel2 = mock(StorageServerChannel.class);
    private StorageServerChannel mockChannel3 = mock(StorageServerChannel.class);
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

    @Override
    protected void doSetup() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .numThreads(1)
            .name("test-range-server-manager")
            .build();
        rootRangeClient = new RootRangeClientImpl(
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

    protected abstract RootRangeServiceImplBase createRootRangeServiceForSuccess();

    protected abstract void verifySuccess(RootRangeClient rootRangeClient) throws Exception;

    @Test
    public void testRequestSuccess() throws Exception {
        CompletableFuture<StorageServerChannel> serviceFuture = FutureUtils.createFuture();
        rootRangeClient.getStorageContainerClient().setStorageServerChannelFuture(serviceFuture);

        RootRangeServiceImplBase rootRangeService = createRootRangeServiceForSuccess();
        serviceRegistry.addService(rootRangeService.bindService());
        @Cleanup StorageServerChannel rsChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serviceFuture.complete(rsChannel);

        verifySuccess(rootRangeClient);
    }


    protected abstract RootRangeServiceImplBase createRootRangeServiceForRequestFailure();

    protected abstract void verifyRequestFailure(RootRangeClient rootRangeClient) throws Exception;

    @Test
    public void testRequestFailure() throws Exception {
        CompletableFuture<StorageServerChannel> serviceFuture = FutureUtils.createFuture();
        rootRangeClient.getStorageContainerClient().setStorageServerChannelFuture(serviceFuture);

        RootRangeServiceImplBase rootRangeService = createRootRangeServiceForRequestFailure();
        serviceRegistry.addService(rootRangeService.bindService());
        @Cleanup StorageServerChannel rsChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serviceFuture.complete(rsChannel);

        verifyRequestFailure(rootRangeClient);
    }


    protected abstract RootRangeServiceImplBase createRootRangeServiceForRpcFailure();

    protected abstract void verifyRpcFailure(RootRangeClient rootRangeClient) throws Exception;

    @Test
    public void testRpcFailure() throws Exception {
        CompletableFuture<StorageServerChannel> serviceFuture = FutureUtils.createFuture();
        rootRangeClient.getStorageContainerClient().setStorageServerChannelFuture(serviceFuture);

        RootRangeServiceImplBase rootRangeService = createRootRangeServiceForRpcFailure();
        serviceRegistry.addService(rootRangeService.bindService());
        @Cleanup StorageServerChannel rsChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serviceFuture.complete(rsChannel);

        verifyRpcFailure(rootRangeClient);
    }

    protected abstract void verifyChannelFailure(IOException expectedException, RootRangeClient rootRangeClient)
        throws Exception;

    @Test
    public void testChannelFailure() throws Exception {
        CompletableFuture<StorageServerChannel> serviceFuture = FutureUtils.createFuture();
        rootRangeClient.getStorageContainerClient().setStorageServerChannelFuture(serviceFuture);

        IOException ioe = new IOException(testName.getMethodName());
        serviceFuture.completeExceptionally(ioe);

        verifyChannelFailure(ioe, rootRangeClient);
    }

    @Test
    public void testCreateRootRangeException() {
        String name = "test-create-root-range-exception";
        // stream exists exception
        Throwable cause1 = createRootRangeException(name, StatusCode.STREAM_EXISTS);
        assertTrue(cause1 instanceof StreamExistsException);
        StreamExistsException see = (StreamExistsException) cause1;
        // stream not found
        Throwable cause2 = createRootRangeException(name, StatusCode.STREAM_NOT_FOUND);
        assertTrue(cause2 instanceof StreamNotFoundException);
        StreamNotFoundException snfe = (StreamNotFoundException) cause2;
        // failure
        Throwable cause3 = createRootRangeException(name, StatusCode.FAILURE);
        assertTrue(cause3 instanceof ClientException);
        ClientException se = (ClientException) cause3;
        assertEquals("fail to access its root range : code = " + StatusCode.FAILURE,
            se.getMessage());
        // unexpected
        Throwable cause4 = createRootRangeException(name, StatusCode.BAD_VERSION);
        assertTrue(cause4 instanceof ClientException);
        // namespace exists exception
        Throwable cause5 = createRootRangeException(name, StatusCode.NAMESPACE_EXISTS);
        assertTrue(cause5 instanceof NamespaceExistsException);
        // namespace not-found exception
        Throwable cause6 = createRootRangeException(name, StatusCode.NAMESPACE_NOT_FOUND);
        assertTrue(cause6 instanceof NamespaceNotFoundException);
        // invalid namespace name
        Throwable cause7 = createRootRangeException(name, StatusCode.INVALID_NAMESPACE_NAME);
        assertTrue(cause7 instanceof InvalidNamespaceNameException);
    }
}
