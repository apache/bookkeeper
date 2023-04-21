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
package org.apache.bookkeeper.clients.impl.internal;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link RootRangeClientImplWithRetries}.
 */
public class RootRangeClientImplWithRetriesTest {

    private static final int NUM_RETRIES = 3;

    private static final String NS_NAME = "test-namespace";
    private static final NamespaceConfiguration NS_CONF = NamespaceConfiguration.newBuilder().build();
    private static final NamespaceProperties NS_PROPS = NamespaceProperties.newBuilder().build();
    private static final String STREAM_NAME = "test-stream";
    private static final StreamConfiguration STREAM_CONF = StreamConfiguration.newBuilder().build();
    private static final StreamProperties STREAM_PROPS = StreamProperties.newBuilder().build();

    private AtomicInteger callCounter;
    private RootRangeClient client;
    private OrderedScheduler scheduler;
    private RootRangeClientImplWithRetries clientWithRetries;

    @Before
    public void setup() {
        this.callCounter = new AtomicInteger(NUM_RETRIES);
        this.client = mock(RootRangeClient.class);
        this.scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();
        this.clientWithRetries = new RootRangeClientImplWithRetries(
            client,
            Backoff.Constant.of(10, NUM_RETRIES),
            scheduler);
    }

    @Test
    public void testCreateNamespace() throws Exception {
        when(client.createNamespace(anyString(), any(NamespaceConfiguration.class)))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(NS_PROPS);
                }
            });

        assertSame(NS_PROPS, FutureUtils.result(clientWithRetries.createNamespace(NS_NAME, NS_CONF)));
    }

    @Test
    public void testDeleteNamespace() throws Exception {
        when(client.deleteNamespace(anyString()))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(true);
                }
            });

        assertTrue(FutureUtils.result(clientWithRetries.deleteNamespace(NS_NAME)));
    }

    @Test
    public void testGetNamespace() throws Exception {
        when(client.getNamespace(anyString()))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(NS_PROPS);
                }
            });

        assertSame(NS_PROPS, FutureUtils.result(clientWithRetries.getNamespace(NS_NAME)));
    }

    @Test
    public void testCreateStream() throws Exception {
        when(client.createStream(anyString(), anyString(), any(StreamConfiguration.class)))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(STREAM_PROPS);
                }
            });

        assertSame(STREAM_PROPS, FutureUtils.result(clientWithRetries.createStream(NS_NAME, STREAM_NAME, STREAM_CONF)));
    }

    @Test
    public void testDeleteStream() throws Exception {
        when(client.deleteStream(anyString(), anyString()))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(true);
                }
            });

        assertTrue(FutureUtils.result(clientWithRetries.deleteStream(NS_NAME, STREAM_NAME)));
    }

    @Test
    public void testGetStream() throws Exception {
        when(client.getStream(anyString(), anyString()))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(STREAM_PROPS);
                }
            });

        assertSame(STREAM_PROPS, FutureUtils.result(clientWithRetries.getStream(NS_NAME, STREAM_NAME)));
    }

    @Test
    public void testGetStreamById() throws Exception {
        when(client.getStream(anyLong()))
            .thenAnswer(invocationOnMock -> {
                if (callCounter.decrementAndGet() > 0) {
                    return FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND));
                } else {
                    return FutureUtils.value(STREAM_PROPS);
                }
            });

        assertSame(STREAM_PROPS, FutureUtils.result(clientWithRetries.getStream(1234L)));
    }
}
