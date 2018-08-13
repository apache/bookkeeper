/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.exceptions.ApiException;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.kv.ByteBufTableImpl;
import org.apache.bookkeeper.clients.impl.kv.PByteBufTableImpl;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Unit test {@link StorageClientImpl}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    StorageClientImpl.class,
})
@Slf4j
public class StorageClientImplTest extends GrpcClientTestBase {

    private static final String NAMESPACE = "test-namespace";
    private static final String STREAM_NAME = "test-stream-name";
    private static final StreamProperties STREAM_PROPERTIES = StreamProperties.newBuilder()
        .setStreamId(1234L)
        .setStreamConf(DEFAULT_STREAM_CONF)
        .setStreamName(STREAM_NAME)
        .setStorageContainerId(16)
        .build();

    private StorageClientImpl client;

    @Override
    protected void doSetup() {
        this.client = spy(new StorageClientImpl(
            NAMESPACE,
            settings,
            ClientResources.create()
        ));
    }

    @Override
    protected void doTeardown() {
        this.client.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenPTable() throws Exception {
        StreamProperties streamProps = StreamProperties.newBuilder(STREAM_PROPERTIES)
            .setStreamConf(StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
                .setStorageType(StorageType.TABLE)
                .build())
            .build();
        when(client.getStreamProperties(anyString()))
            .thenReturn(FutureUtils.value(streamProps));

        PByteBufTableImpl tableImpl = mock(PByteBufTableImpl.class);
        when(tableImpl.initialize()).thenReturn(FutureUtils.value(tableImpl));

        PowerMockito.whenNew(PByteBufTableImpl.class)
            .withAnyArguments()
            .thenReturn(tableImpl);

        PTable<ByteBuf, ByteBuf> returnedTableImpl = FutureUtils.result(
            client.openPTable(STREAM_NAME)
        );

        assertSame(tableImpl, returnedTableImpl);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenTable() throws Exception {
        StreamProperties streamProps = StreamProperties.newBuilder(STREAM_PROPERTIES)
            .setStreamConf(StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
                .setStorageType(StorageType.TABLE)
                .build())
            .build();
        when(client.getStreamProperties(anyString()))
            .thenReturn(FutureUtils.value(streamProps));

        PByteBufTableImpl tableImpl = mock(PByteBufTableImpl.class);
        when(tableImpl.initialize()).thenReturn(FutureUtils.value(tableImpl));

        PowerMockito.whenNew(PByteBufTableImpl.class)
            .withAnyArguments()
            .thenReturn(tableImpl);

        Table<ByteBuf, ByteBuf> returnedTableImpl = FutureUtils.result(
            client.openTable(STREAM_NAME)
        );
        assertTrue(returnedTableImpl instanceof ByteBufTableImpl);
        ByteBufTableImpl bytesTableImpl = (ByteBufTableImpl) returnedTableImpl;

        assertSame(tableImpl, Whitebox.getInternalState(bytesTableImpl, "underlying"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenPTableIllegalOp() throws Exception {
        StreamProperties streamProps = StreamProperties.newBuilder(STREAM_PROPERTIES)
            .setStreamConf(StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
                .setStorageType(StorageType.STREAM)
                .build())
            .build();
        when(client.getStreamProperties(anyString()))
            .thenReturn(FutureUtils.value(streamProps));

        PByteBufTableImpl tableImpl = mock(PByteBufTableImpl.class);
        when(tableImpl.initialize()).thenReturn(FutureUtils.value(tableImpl));

        PowerMockito.whenNew(PByteBufTableImpl.class)
            .withAnyArguments()
            .thenReturn(tableImpl);

        try {
            FutureUtils.result(client.openPTable(STREAM_NAME));
            fail("Should fail #openTable on opening a non-table storage entity");
        } catch (ApiException sae) {
            // expected exception
        }
    }
}
