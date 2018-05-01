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
package org.apache.bookkeeper.clients.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ListenableFutureRpcProcessor}.
 */
public class ListenableFutureRpcProcessorTest {

    private ListenableFutureRpcProcessor<String, String, String> processor;
    private StorageContainerChannel scChannel;
    private ScheduledExecutorService executor;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        scChannel = mock(StorageContainerChannel.class);
        processor = spy(new ListenableFutureRpcProcessor<String, String, String>(
            scChannel, executor, ClientConstants.DEFAULT_INFINIT_BACKOFF_POLICY) {
            @Override
            protected String createRequest() {
                return null;
            }

            @Override
            protected ListenableFuture<String> sendRPC(StorageServerChannel rsChannel, String s) {
                return null;
            }

            @Override
            protected String processResponse(String response) throws Exception {
                return null;
            }
        });
    }

    @Test
    public void testFailToConnect() {
        CompletableFuture<StorageServerChannel> serverFuture = new CompletableFuture<>();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverFuture);

        CompletableFuture<String> resultFuture = processor.process();
        verify(scChannel, times(1)).getStorageContainerChannelFuture();

        // inject channel failure
        Exception testExc = new Exception("test-exception");
        serverFuture.completeExceptionally(testExc);

        try {
            FutureUtils.result(resultFuture);
            fail("Should fail the process if failed to connect to storage server");
        } catch (Exception e) {
            assertSame(testExc, e);
        }
    }

    @Test
    public void testProcessSuccessfully() throws Exception {
        String request = "request";
        String response = "response";
        String result = "result";

        StorageServerChannel serverChannel = mock(StorageServerChannel.class);

        CompletableFuture<StorageServerChannel> serverFuture = new CompletableFuture<>();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverFuture);

        SettableFuture<String> rpcFuture = SettableFuture.create();

        // mock the process method
        when(processor.createRequest()).thenReturn(request);
        when(processor.sendRPC(same(serverChannel), eq(request))).thenReturn(rpcFuture);
        when(processor.processResponse(eq(response))).thenReturn(result);

        CompletableFuture<String> resultFuture = processor.process();
        verify(scChannel, times(1)).getStorageContainerChannelFuture();

        // complete the server future to return a mock server channel
        FutureUtils.complete(serverFuture, serverChannel);

        // complete the rpc future to return the response
        rpcFuture.set(response);

        assertEquals(result, resultFuture.get());
    }

    @Test
    public void testProcessResponseException() throws Exception {
        String request = "request";
        String response = "response";

        StorageServerChannel serverChannel = mock(StorageServerChannel.class);

        CompletableFuture<StorageServerChannel> serverFuture = new CompletableFuture<>();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverFuture);

        SettableFuture<String> rpcFuture = SettableFuture.create();

        Exception testException = new Exception("test-exception");

        // mock the process method
        when(processor.createRequest()).thenReturn(request);
        when(processor.sendRPC(same(serverChannel), eq(request))).thenReturn(rpcFuture);
        when(processor.processResponse(eq(response))).thenThrow(testException);

        CompletableFuture<String> resultFuture = processor.process();
        verify(scChannel, times(1)).getStorageContainerChannelFuture();

        // complete the server future to return a mock server channel
        FutureUtils.complete(serverFuture, serverChannel);

        // complete the rpc future to return the response
        rpcFuture.set(response);

        try {
            FutureUtils.result(resultFuture);
            fail("Should throw exception on processing result");
        } catch (Exception e) {
            assertSame(testException, e);
        }
    }

    @Test
    public void testProcessRpcException() throws Exception {
        String request = "request";
        String response = "response";
        String result = "result";

        StorageServerChannel serverChannel = mock(StorageServerChannel.class);

        CompletableFuture<StorageServerChannel> serverFuture = new CompletableFuture<>();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverFuture);

        SettableFuture<String> rpcFuture = SettableFuture.create();

        // mock the process method
        when(processor.createRequest()).thenReturn(request);
        when(processor.sendRPC(same(serverChannel), eq(request))).thenReturn(rpcFuture);
        when(processor.processResponse(eq(response))).thenReturn(result);

        CompletableFuture<String> resultFuture = processor.process();
        verify(scChannel, times(1)).getStorageContainerChannelFuture();

        // complete the server future to return a mock server channel
        FutureUtils.complete(serverFuture, serverChannel);

        // complete the rpc future with `Status.INTERNAL`
        rpcFuture.setException(new StatusRuntimeException(Status.INTERNAL));

        try {
            FutureUtils.result(resultFuture);
            fail("Should throw fail immediately if rpc request failed");
        } catch (Exception e) {
            assertTrue(e instanceof StatusRuntimeException);
            StatusRuntimeException sre = (StatusRuntimeException) e;
            assertEquals(Status.INTERNAL, sre.getStatus());
        }
    }

    @Test
    public void testProcessRetryNotFoundRpcException() throws Exception {
        String request = "request";
        String response = "response";
        String result = "result";

        StorageServerChannel serverChannel = mock(StorageServerChannel.class);

        CompletableFuture<StorageServerChannel> serverFuture = new CompletableFuture<>();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverFuture);

        AtomicInteger numRpcs = new AtomicInteger(0);

        // mock the process method
        when(processor.createRequest()).thenReturn(request);
        when(processor.processResponse(eq(response))).thenReturn(result);
        when(processor.sendRPC(same(serverChannel), eq(request))).thenAnswer(invocationOnMock -> {
            SettableFuture<String> rpcFuture = SettableFuture.create();
            if (numRpcs.getAndIncrement() > 2) {
                rpcFuture.set(response);
            } else {
                rpcFuture.setException(new StatusRuntimeException(Status.NOT_FOUND));
            }
            return rpcFuture;
        });

        CompletableFuture<String> resultFuture = processor.process();

        // complete the server future to return a mock server channel
        FutureUtils.complete(serverFuture, serverChannel);

        assertEquals(result, FutureUtils.result(resultFuture));
        verify(scChannel, times(4)).getStorageContainerChannelFuture();
    }
}
