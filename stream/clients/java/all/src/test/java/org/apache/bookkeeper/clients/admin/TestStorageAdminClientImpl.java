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

package org.apache.bookkeeper.clients.admin;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test for {@link StorageAdminClientImpl}.
 */
public class TestStorageAdminClientImpl {

    private static final NamespaceConfiguration colConf = NamespaceConfiguration.newBuilder()
        .setDefaultStreamConf(DEFAULT_STREAM_CONF)
        .build();
    private static final NamespaceProperties colProps = NamespaceProperties.newBuilder()
        .setNamespaceId(System.currentTimeMillis())
        .setNamespaceName("namespace")
        .setDefaultStreamConf(DEFAULT_STREAM_CONF)
        .build();
    private static final StreamProperties streamProps = StreamProperties.newBuilder()
        .setStreamId(System.currentTimeMillis())
        .setStorageContainerId(System.currentTimeMillis())
        .setStreamName("stream_" + System.currentTimeMillis())
        .setStreamConf(DEFAULT_STREAM_CONF)
        .build();

    @Rule
    public TestName testName = new TestName();

    private RootRangeClient mockRootRangeClient = mock(RootRangeClient.class);
    private StorageServerClientManager mockManager = mock(StorageServerClientManager.class);
    private StorageAdminClientImpl adminClient;

    @Before
    public void setUp() {
        when(mockManager.getRootRangeClient()).thenReturn(mockRootRangeClient);
        this.adminClient = new StorageAdminClientImpl(
            StorageClientSettings.newBuilder()
                .serviceUri("bk://localhost:4181")
                .build(),
            ClientResources.create(),
            () -> mockManager);
    }

    @Test
    public void testCreateNamespace() throws Exception {
        String colName = testName.getMethodName();
        when(mockRootRangeClient.createNamespace(colName, colConf))
            .thenReturn(FutureUtils.value(colProps));
        assertEquals(colProps, FutureUtils.result(adminClient.createNamespace(colName, colConf)));
        verify(mockRootRangeClient, times(1)).createNamespace(colName, colConf);
    }

    @Test
    public void testDeleteNamespace() throws Exception {
        String colName = testName.getMethodName();
        when(mockRootRangeClient.deleteNamespace(colName))
            .thenReturn(FutureUtils.value(true));
        assertEquals(true, FutureUtils.result(adminClient.deleteNamespace(colName)));
        verify(mockRootRangeClient, times(1)).deleteNamespace(colName);
    }

    @Test
    public void testGetNamespace() throws Exception {
        String colName = testName.getMethodName();
        when(mockRootRangeClient.getNamespace(colName))
            .thenReturn(FutureUtils.value(colProps));
        assertEquals(colProps, FutureUtils.result(adminClient.getNamespace(colName)));
        verify(mockRootRangeClient, times(1)).getNamespace(colName);
    }

    @Test
    public void testCreateStream() throws Exception {
        String colName = testName.getMethodName();
        String streamName = colName + "_stream";
        when(mockRootRangeClient.createStream(colName, streamName, DEFAULT_STREAM_CONF))
            .thenReturn(FutureUtils.value(streamProps));
        assertEquals(streamProps, FutureUtils.result(
            adminClient.createStream(colName, streamName, DEFAULT_STREAM_CONF)));
        verify(mockRootRangeClient, times(1)).createStream(colName, streamName, DEFAULT_STREAM_CONF);
    }

    @Test
    public void testDeleteStream() throws Exception {
        String colName = testName.getMethodName();
        String streamName = colName + "_stream";
        when(mockRootRangeClient.deleteStream(colName, streamName))
            .thenReturn(FutureUtils.value(true));
        assertEquals(true, FutureUtils.result(adminClient.deleteStream(colName, streamName)));
        verify(mockRootRangeClient, times(1)).deleteStream(colName, streamName);
    }

    @Test
    public void testGetStream() throws Exception {
        String colName = testName.getMethodName();
        String streamName = colName + "_stream";
        when(mockRootRangeClient.getStream(colName, streamName))
            .thenReturn(FutureUtils.value(streamProps));
        assertEquals(streamProps, FutureUtils.result(adminClient.getStream(colName, streamName)));
        verify(mockRootRangeClient, times(1)).getStream(colName, streamName);
    }

}
