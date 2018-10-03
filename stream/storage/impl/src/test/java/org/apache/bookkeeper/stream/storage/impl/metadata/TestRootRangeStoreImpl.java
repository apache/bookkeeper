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

package org.apache.bookkeeper.stream.storage.impl.metadata;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.MIN_DATA_STREAM_ID;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStreamRequest;
import static org.apache.bookkeeper.stream.storage.impl.metadata.RootRangeStoreImpl.NS_ID_KEY;
import static org.apache.bookkeeper.stream.storage.impl.metadata.RootRangeStoreImpl.STREAM_ID_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Bytes;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCAsyncStoreTestBase;
import org.junit.Test;

/**
 * Unit test for {@link RootRangeStoreImpl}.
 */
@Slf4j
public class TestRootRangeStoreImpl extends MVCCAsyncStoreTestBase {

    private final NamespaceConfiguration namespaceConf =
        NamespaceConfiguration.newBuilder()
            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
            .build();

    private final StreamConfiguration streamConf =
        StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
            .build();


    private RootRangeStoreImpl rootRangeStore;

    @Override
    protected void doSetup() throws Exception {
        rootRangeStore = new RootRangeStoreImpl(
            store,
            StorageContainerPlacementPolicyImpl.of(1024),
            scheduler.chooseThread());
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != store) {
            store.close();
        }
    }

    //
    // Tests for Namespace API
    //

    private void verifyNamespaceExists(String nsName, long nsId) throws Exception {
        assertNotNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getNamespaceIdKey(nsId))));
        assertNotNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getNamespaceNameKey(nsName))));
    }

    private void verifyNamespaceNotExists(String nsName, long nsId) throws Exception {
        assertNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getNamespaceIdKey(nsId))));
        assertNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getNamespaceNameKey(nsName))));
    }

    private void verifyNamespaceId(long nsId) throws Exception {
        byte[] nsIdKey = FutureUtils.result(store.get(NS_ID_KEY));
        if (nsId < 0) {
            assertNull(nsIdKey);
        } else {
            assertNotNull(nsIdKey);
            assertEquals(nsId, Bytes.toLong(nsIdKey, 0));
        }
    }

    private void verifyStreamId(long streamId) throws Exception {
        byte[] streamIdKey = FutureUtils.result(store.get(STREAM_ID_KEY));
        if (streamId < 0) {
            assertNull(streamIdKey);
        } else {
            assertNotNull(streamIdKey);
            assertEquals(streamId, Bytes.toLong(streamIdKey, 0));
        }
    }

    private CreateNamespaceResponse createNamespaceAndVerify(String nsName, long expectedNsId)
        throws Exception {
        CompletableFuture<CreateNamespaceResponse> createFuture = rootRangeStore.createNamespace(
            createCreateNamespaceRequest(nsName, namespaceConf));
        CreateNamespaceResponse response = FutureUtils.result(createFuture);
        assertEquals(StatusCode.SUCCESS, response.getCode());
        assertEquals(expectedNsId, response.getNsProps().getNamespaceId());
        assertEquals(nsName, response.getNsProps().getNamespaceName());
        assertEquals(namespaceConf.getDefaultStreamConf(), response.getNsProps().getDefaultStreamConf());

        return response;
    }

    private void getNamespaceAndVerify(String nsName,
                                       long expectedNsId,
                                       StreamConfiguration streamConf) throws Exception {
        CompletableFuture<GetNamespaceResponse> getFuture = rootRangeStore.getNamespace(
            createGetNamespaceRequest(nsName));
        GetNamespaceResponse getResp = FutureUtils.result(getFuture);
        assertEquals(expectedNsId, getResp.getNsProps().getNamespaceId());
        assertEquals(streamConf, getResp.getNsProps().getDefaultStreamConf());
    }

    @Test
    public void testCreateNamespaceInvalidName() throws Exception {
        String nsName = "";
        CompletableFuture<CreateNamespaceResponse> createFuture = rootRangeStore.createNamespace(
            createCreateNamespaceRequest(nsName, namespaceConf));
        CreateNamespaceResponse response = FutureUtils.result(createFuture);
        assertEquals(StatusCode.INVALID_NAMESPACE_NAME, response.getCode());

        verifyNamespaceId(-1);
    }

    @Test
    public void testCreateNamespaceSuccess() throws Exception {
        String nsName = name.getMethodName();

        CreateNamespaceResponse response = createNamespaceAndVerify(nsName, 0L);

        verifyNamespaceExists(nsName, response.getNsProps().getNamespaceId());
        verifyNamespaceId(0L);
    }

    @Test
    public void testCreateNamespaceExists() throws Exception {
        String nsName = name.getMethodName();

        // create first namespace
        CreateNamespaceResponse response = createNamespaceAndVerify(nsName, 0L);
        verifyNamespaceExists(nsName, response.getNsProps().getNamespaceId());
        verifyNamespaceId(0L);

        // create the namespace with same name will fail
        CreateNamespaceResponse response2 = FutureUtils.result(
            rootRangeStore.createNamespace(
                createCreateNamespaceRequest(nsName, namespaceConf)));
        assertEquals(StatusCode.INTERNAL_SERVER_ERROR, response2.getCode());

        // namespace will not be advanced
        verifyNamespaceId(0L);
    }

    @Test
    public void testDeleteNamespaceInvalidName() throws Exception {
        String nsName = "";
        CompletableFuture<DeleteNamespaceResponse> deleteFuture = rootRangeStore.deleteNamespace(
            createDeleteNamespaceRequest(nsName));
        DeleteNamespaceResponse response = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.INVALID_NAMESPACE_NAME, response.getCode());
    }

    private DeleteNamespaceResponse deleteNamespaceAndVerify(String nsName) throws Exception {
        CompletableFuture<DeleteNamespaceResponse> deleteFuture = rootRangeStore.deleteNamespace(
            createDeleteNamespaceRequest(nsName));
        DeleteNamespaceResponse deleteResp = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.SUCCESS, deleteResp.getCode());
        return deleteResp;
    }

    @Test
    public void testDeleteNamespaceSuccess() throws Exception {
        String nsName = name.getMethodName();

        CreateNamespaceResponse createResp = createNamespaceAndVerify(nsName, 0L);
        verifyNamespaceExists(nsName, createResp.getNsProps().getNamespaceId());
        verifyNamespaceId(0L);

        deleteNamespaceAndVerify(nsName);
        verifyNamespaceNotExists(nsName, createResp.getNsProps().getNamespaceId());
        verifyNamespaceId(0L);
    }

    @Test
    public void testDeleteNamespaceNotFound() throws Exception {
        String nsName = name.getMethodName();
        CompletableFuture<DeleteNamespaceResponse> deleteFuture = rootRangeStore.deleteNamespace(
            createDeleteNamespaceRequest(nsName));
        // create first namespace
        DeleteNamespaceResponse response = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.NAMESPACE_NOT_FOUND, response.getCode());

        verifyNamespaceNotExists(nsName, 0L);
        verifyNamespaceId(-1L);
    }

    @Test
    public void testGetNamespaceInvalidName() throws Exception {
        String nsName = "";
        CompletableFuture<GetNamespaceResponse> getFuture = rootRangeStore.getNamespace(
            createGetNamespaceRequest(nsName));
        GetNamespaceResponse response = FutureUtils.result(getFuture);
        assertEquals(StatusCode.INVALID_NAMESPACE_NAME, response.getCode());
    }

    @Test
    public void testGetNamespaceNotFound() throws Exception {
        String nsName = name.getMethodName();
        CompletableFuture<GetNamespaceResponse> getFuture = rootRangeStore.getNamespace(
            createGetNamespaceRequest(nsName));
        // create first namespace
        GetNamespaceResponse response = FutureUtils.result(getFuture);
        assertEquals(StatusCode.NAMESPACE_NOT_FOUND, response.getCode());

        verifyNamespaceNotExists(nsName, 0L);
        verifyNamespaceId(-1L);
    }

    @Test
    public void testGetNamespaceSuccess() throws Exception {
        String nsName = name.getMethodName();

        CreateNamespaceResponse response = createNamespaceAndVerify(nsName, 0L);
        getNamespaceAndVerify(nsName, 0L, namespaceConf.getDefaultStreamConf());

        verifyNamespaceId(0);
    }

    //
    // Tests for Stream API
    //

    @Test
    public void testCreateStreamInvalidName() throws Exception {
        String nsName = name.getMethodName();
        String streamName = "";
        CompletableFuture<CreateStreamResponse> createFuture = rootRangeStore.createStream(
            createCreateStreamRequest(nsName, streamName, streamConf));
        CreateStreamResponse response = FutureUtils.result(createFuture);
        assertEquals(StatusCode.INVALID_STREAM_NAME, response.getCode());
    }

    @Test
    public void testCreateStreamNamespaceNotFound() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();
        CompletableFuture<CreateStreamResponse> createFuture = rootRangeStore.createStream(
            createCreateStreamRequest(nsName, streamName, streamConf));
        CreateStreamResponse response = FutureUtils.result(createFuture);
        assertEquals(StatusCode.NAMESPACE_NOT_FOUND, response.getCode());
    }

    private CreateStreamResponse createStreamAndVerify(String nsName,
                                                       String streamName,
                                                       long expectedStreamId) throws Exception {
        CompletableFuture<CreateStreamResponse> createFuture = rootRangeStore.createStream(
            createCreateStreamRequest(nsName, streamName, streamConf));
        CreateStreamResponse response = FutureUtils.result(createFuture);
        assertEquals(StatusCode.SUCCESS, response.getCode());
        assertEquals(MIN_DATA_STREAM_ID, response.getStreamProps().getStreamId());
        assertEquals(streamName, response.getStreamProps().getStreamName());
        assertEquals(streamConf, response.getStreamProps().getStreamConf());
        assertTrue(response.getStreamProps().getStorageContainerId() >= 0);
        return response;
    }

    private void verifyStreamExists(long nsId, String streamName, long streamId) throws Exception {
        assertNotNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getStreamNameKey(nsId, streamName))));
        assertNotNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getStreamIdKey(nsId, streamId))));
    }

    private void verifyStreamNotExists(long nsId, String streamName, long streamId) throws Exception {
        assertNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getStreamNameKey(nsId, streamName))));
        assertNull(
            FutureUtils.result(
                store.get(RootRangeStoreImpl.getStreamIdKey(nsId, streamId))));
    }

    @Test
    public void testCreateStreamSuccess() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        CreateNamespaceResponse createResp = createNamespaceAndVerify(nsName, 0L);
        createStreamAndVerify(nsName, streamName, MIN_DATA_STREAM_ID);

        verifyStreamExists(
            createResp.getNsProps().getNamespaceId(),
            streamName,
            MIN_DATA_STREAM_ID);
        verifyStreamId(MIN_DATA_STREAM_ID);
    }

    @Test
    public void testCreateStreamExists() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        CreateNamespaceResponse createResp = createNamespaceAndVerify(nsName, 0L);
        createStreamAndVerify(nsName, streamName, MIN_DATA_STREAM_ID);

        verifyStreamExists(
            createResp.getNsProps().getNamespaceId(),
            streamName,
            MIN_DATA_STREAM_ID);
        verifyStreamId(MIN_DATA_STREAM_ID);

        // create the namespace with same name will fail
        CreateStreamResponse response2 = FutureUtils.result(
            rootRangeStore.createStream(
                createCreateStreamRequest(nsName, streamName, streamConf)));
        // TODO: change it later
        assertEquals(StatusCode.INTERNAL_SERVER_ERROR, response2.getCode());

        verifyStreamId(MIN_DATA_STREAM_ID);
    }

    @Test
    public void testDeleteStreamInvalidName() throws Exception {
        String nsName = name.getMethodName();
        String streamName = "";
        CompletableFuture<DeleteStreamResponse> deleteFuture = rootRangeStore.deleteStream(
            createDeleteStreamRequest(nsName, streamName));
        DeleteStreamResponse response = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.INVALID_STREAM_NAME, response.getCode());
    }

    @Test
    public void testDeleteStreamNamespaceNotFound() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();
        CompletableFuture<DeleteStreamResponse> deleteFuture = rootRangeStore.deleteStream(
            createDeleteStreamRequest(nsName, streamName));
        DeleteStreamResponse response = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.NAMESPACE_NOT_FOUND, response.getCode());
    }

    @Test
    public void testDeleteStreamSuccess() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        CreateNamespaceResponse createResp = createNamespaceAndVerify(nsName, 0L);
        createStreamAndVerify(nsName, streamName, MIN_DATA_STREAM_ID);

        verifyStreamExists(
            createResp.getNsProps().getNamespaceId(),
            streamName,
            MIN_DATA_STREAM_ID);
        verifyStreamId(MIN_DATA_STREAM_ID);

        CompletableFuture<DeleteStreamResponse> deleteFuture = rootRangeStore.deleteStream(
            createDeleteStreamRequest(nsName, streamName));
        DeleteStreamResponse deleteResp = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.SUCCESS, deleteResp.getCode());

        verifyStreamNotExists(
            createResp.getNsProps().getNamespaceId(),
            streamName,
            MIN_DATA_STREAM_ID);
        verifyStreamId(MIN_DATA_STREAM_ID);
    }

    @Test
    public void testDeleteStreamNotFound() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        createNamespaceAndVerify(nsName, 0L);

        CompletableFuture<DeleteStreamResponse> deleteFuture = rootRangeStore.deleteStream(
            createDeleteStreamRequest(nsName, streamName));
        DeleteStreamResponse response = FutureUtils.result(deleteFuture);
        assertEquals(StatusCode.STREAM_NOT_FOUND, response.getCode());

        verifyStreamId(-1L);
    }

    @Test
    public void testGetStreamInvalidName() throws Exception {
        String nsName = name.getMethodName();
        String streamName = "";
        CompletableFuture<GetStreamResponse> getFuture = rootRangeStore.getStream(
            createGetStreamRequest(nsName, streamName));
        GetStreamResponse response = FutureUtils.result(getFuture);
        assertEquals(StatusCode.INVALID_STREAM_NAME, response.getCode());
    }

    @Test
    public void testGetStreamNamespaceNotFound() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();
        CompletableFuture<GetStreamResponse> getFuture = rootRangeStore.getStream(
            createGetStreamRequest(nsName, streamName));
        GetStreamResponse response = FutureUtils.result(getFuture);
        assertEquals(StatusCode.NAMESPACE_NOT_FOUND, response.getCode());
    }

    @Test
    public void testGetStreamSuccess() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        createNamespaceAndVerify(nsName, 0L);
        createStreamAndVerify(nsName, streamName, MIN_DATA_STREAM_ID);
        verifyStreamId(MIN_DATA_STREAM_ID);

        CompletableFuture<GetStreamResponse> getFuture = rootRangeStore.getStream(
            createGetStreamRequest(nsName, streamName));
        GetStreamResponse getResp = FutureUtils.result(getFuture);
        assertEquals(StatusCode.SUCCESS, getResp.getCode());
        assertEquals(MIN_DATA_STREAM_ID, getResp.getStreamProps().getStreamId());
        assertEquals(streamName, getResp.getStreamProps().getStreamName());
        assertEquals(streamConf, getResp.getStreamProps().getStreamConf());
    }

    @Test
    public void testGetStreamByIdSuccess() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        createNamespaceAndVerify(nsName, 0L);
        createStreamAndVerify(nsName, streamName, MIN_DATA_STREAM_ID);
        verifyStreamId(MIN_DATA_STREAM_ID);

        CompletableFuture<GetStreamResponse> getFuture = rootRangeStore.getStream(
            createGetStreamRequest(MIN_DATA_STREAM_ID));
        GetStreamResponse getResp = FutureUtils.result(getFuture);
        assertEquals(StatusCode.SUCCESS, getResp.getCode());
        assertEquals(MIN_DATA_STREAM_ID, getResp.getStreamProps().getStreamId());
        assertEquals(streamName, getResp.getStreamProps().getStreamName());
        assertEquals(streamConf, getResp.getStreamProps().getStreamConf());
    }

    @Test
    public void testGetStreamNotFound() throws Exception {
        String nsName = name.getMethodName();
        String streamName = name.getMethodName();

        createNamespaceAndVerify(nsName, 0L);

        verifyStreamId(-1);

        CompletableFuture<GetStreamResponse> getFuture = rootRangeStore.getStream(
            createGetStreamRequest(nsName, streamName));
        GetStreamResponse response = FutureUtils.result(getFuture);
        assertEquals(StatusCode.STREAM_NOT_FOUND, response.getCode());
    }

    @Test
    public void testGetStreamByIdNotFound() throws Exception {
        String nsName = name.getMethodName();

        createNamespaceAndVerify(nsName, 0L);

        verifyStreamId(-1);

        CompletableFuture<GetStreamResponse> getFuture = rootRangeStore.getStream(
            createGetStreamRequest(MIN_DATA_STREAM_ID));
        GetStreamResponse response = FutureUtils.result(getFuture);
        assertEquals(StatusCode.STREAM_NOT_FOUND, response.getCode());
    }

}
