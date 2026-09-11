/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.impl;

import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetActiveRangesRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStreamRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCalls;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import lombok.CustomLog;
import org.apache.bookkeeper.clients.impl.container.StorageContainerClientInterceptor;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.grpc.proxy.ProxyHandlerRegistry;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.StreamName;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.MetaRangeServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.StorageResources;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.apache.bookkeeper.stream.storage.api.service.RangeStoreServiceFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.grpc.GrpcServices;
import org.apache.bookkeeper.stream.storage.impl.sc.LocalStorageContainerManager;
import org.apache.bookkeeper.stream.storage.impl.service.RangeStoreContainerServiceFactoryImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link StorageContainerStoreImpl}.
 */
@CustomLog
public class TestStorageContainerStoreImpl {

    private static final StreamProperties streamProps = createStreamProps();

    private static StreamProperties createStreamProps() {
        StreamProperties props = new StreamProperties()
            .setStorageContainerId(System.currentTimeMillis())
            .setStreamId(System.currentTimeMillis())
            .setStreamName("test-create-add-stream-request");
        props.setStreamConf().copyFrom(DEFAULT_STREAM_CONF);
        return props;
    }

    private static StreamName createStreamName(String name) {
        return new StreamName()
            .setNamespaceName(name + "_col")
            .setStreamName(name + "_stream");
    }

    private static Endpoint createEndpoint(String hostname,
                                           int port) {
        return new Endpoint()
            .setHostname(hostname)
            .setPort(port);
    }

    private final CompositeConfiguration compConf =
        new CompositeConfiguration();
    private final StorageConfiguration storageConf =
        new StorageConfiguration(compConf);
    private final NamespaceConfiguration namespaceConf = createNamespaceConf();

    private static NamespaceConfiguration createNamespaceConf() {
        NamespaceConfiguration conf = new NamespaceConfiguration();
        conf.setDefaultStreamConf().copyFrom(DEFAULT_STREAM_CONF);
        return conf;
    }
    private final StorageResources resources = StorageResources.create();
    private RangeStoreService mockRangeStoreService;
    private StorageContainerStoreImpl rangeStore;
    private Server server;
    private Channel channel;
    private long scId;

    private <ReqT, RespT> ListenableFuture<RespT> futureCall(MethodDescriptor<ReqT, RespT> method, ReqT request) {
        return ClientCalls.futureUnaryCall(channel.newCall(method, CallOptions.DEFAULT), request);
    }

    //
    // Utils for table api
    //
    private static final byte[] TEST_ROUTING_KEY = "test-routing-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TEST_KEY = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TEST_VAL = "test-val".getBytes(StandardCharsets.UTF_8);

    private static RoutingHeader testRoutingHeader() {
        return new RoutingHeader()
            .setRKey(TEST_ROUTING_KEY)
            .setStreamId(1234L)
            .setRangeId(1256L);
    }

    private static PutRequest createPutRequest() {
        PutRequest req = new PutRequest()
            .setKey(TEST_KEY)
            .setValue(TEST_VAL);
        req.setHeader().copyFrom(testRoutingHeader());
        return req;
    }

    private static PutResponse createPutResponse(StatusCode code) {
        PutResponse resp = new PutResponse();
        ResponseHeader header = resp.setHeader();
        header.setRoutingHeader().copyFrom(testRoutingHeader());
        header.setCode(code);
        return resp;
    }

    private static RangeRequest createRangeRequest() {
        RangeRequest req = new RangeRequest()
            .setKey(TEST_KEY);
        req.setHeader().copyFrom(testRoutingHeader());
        return req;
    }

    private static RangeResponse createRangeResponse(StatusCode code) {
        RangeResponse resp = new RangeResponse();
        ResponseHeader header = resp.setHeader();
        header.setRoutingHeader().copyFrom(testRoutingHeader());
        header.setCode(code);
        resp.setCount(0);
        return resp;
    }

    private static DeleteRangeRequest createDeleteRequest() {
        DeleteRangeRequest req = new DeleteRangeRequest()
            .setKey(TEST_KEY);
        req.setHeader().copyFrom(testRoutingHeader());
        return req;
    }

    private static DeleteRangeResponse createDeleteResponse(StatusCode code) {
        DeleteRangeResponse resp = new DeleteRangeResponse();
        ResponseHeader header = resp.setHeader();
        header.setRoutingHeader().copyFrom(testRoutingHeader());
        header.setCode(code);
        resp.setDeleted(0);
        return resp;
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        Endpoint endpoint = createEndpoint("127.0.0.1", 0);

        // create the client manager
        MVCCStoreFactory storeFactory = mock(MVCCStoreFactory.class);
        MVCCAsyncStore<byte[], byte[]> store = mock(MVCCAsyncStore.class);
        when(storeFactory.openStore(anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(FutureUtils.value(store));
        when(storeFactory.closeStores(anyLong()))
            .thenReturn(FutureUtils.Void());

        RangeStoreServiceFactory rangeStoreServiceFactory = mock(RangeStoreServiceFactory.class);
        mockRangeStoreService = mock(RangeStoreService.class);
        when(mockRangeStoreService.start()).thenReturn(FutureUtils.Void());
        when(mockRangeStoreService.stop()).thenReturn(FutureUtils.Void());
        when(rangeStoreServiceFactory.createService(anyLong()))
            .thenReturn(mockRangeStoreService);

        rangeStore = new StorageContainerStoreImpl(
            storageConf,
            (storeConf, rgRegistry)
                -> new LocalStorageContainerManager(endpoint, storeConf, rgRegistry, 2),
            new RangeStoreContainerServiceFactoryImpl(rangeStoreServiceFactory),
            null,
            NullStatsLogger.INSTANCE);

        rangeStore.start();

        final String serverName = "test-server";

        Collection<ServerServiceDefinition> grpcServices = GrpcServices.create(null);
        ProxyHandlerRegistry.Builder registryBuilder = ProxyHandlerRegistry.newBuilder();
        grpcServices.forEach(service -> registryBuilder.addService(service));
        ProxyHandlerRegistry registry = registryBuilder
            .setChannelFinder(rangeStore)
            .build();
        server = InProcessServerBuilder.forName(serverName)
            .fallbackHandlerRegistry(registry)
            .directExecutor()
            .build()
            .start();

        channel = InProcessChannelBuilder.forName(serverName)
            .usePlaintext()
            .build();

        scId = ThreadLocalRandom.current().nextInt(2);

        // intercept the channel with storage container information.
        channel = ClientInterceptors.intercept(
            channel,
            new StorageContainerClientInterceptor(scId));
    }

    @After
    public void tearDown() {
        if (null != channel) {
            if (channel instanceof ManagedChannel) {
                ((ManagedChannel) channel).shutdown();
            }
        }
        if (null != server) {
            server.shutdown();
        }
        if (null != rangeStore) {
            rangeStore.close();
        }
    }

    private <T> void verifyNotFoundException(CompletableFuture<T> future,
                                             Status status)
        throws InterruptedException {
        try {
            future.get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof StatusRuntimeException);
            StatusRuntimeException sre = (StatusRuntimeException) ee.getCause();
            assertEquals(status, sre.getStatus());
        }
    }

    //
    // Namespace API
    //

    @Test
    public void testCreateNamespaceNoRootStorageContainerStore() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        String colName = "test-create-namespace-no-root-storage-container-store";
        verifyNotFoundException(fromListenableFuture(
            futureCall(RootRangeServiceGrpc.getCreateNamespaceMethod(),
                createCreateNamespaceRequest(colName, namespaceConf))),
            Status.NOT_FOUND);
    }

    @Test
    public void testDeleteNamespaceNoRootStorageContainerStore() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        String colName = "test-delete-namespace-no-root-storage-container-store";
        verifyNotFoundException(fromListenableFuture(
            futureCall(RootRangeServiceGrpc.getDeleteNamespaceMethod(), createDeleteNamespaceRequest(colName))),
            Status.NOT_FOUND);
    }

    @Test
    public void testGetNamespaceNoRootStorageContainerStore() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        String colName = "test-get-namespace-no-root-storage-container-store";
        verifyNotFoundException(fromListenableFuture(
            futureCall(RootRangeServiceGrpc.getGetNamespaceMethod(), createGetNamespaceRequest(colName))),
            Status.NOT_FOUND);
    }

    @Test
    public void testCreateNamespaceMockRootStorageContainerStore() throws Exception {
        String colName = "test-create-namespace-mock-root-storage-container-store";

        CreateNamespaceResponse createResp = new CreateNamespaceResponse()
            .setCode(StatusCode.NAMESPACE_EXISTS);
        CreateNamespaceRequest request = createCreateNamespaceRequest(colName, namespaceConf);

        when(mockRangeStoreService.createNamespace(request))
            .thenReturn(CompletableFuture.completedFuture(createResp));

        CompletableFuture<CreateNamespaceResponse> createRespFuture =
            fromListenableFuture(futureCall(RootRangeServiceGrpc.getCreateNamespaceMethod(), request));
        assertEquals(createResp, createRespFuture.get());
        verify(mockRangeStoreService, times(1)).createNamespace(request);
    }

    @Test
    public void testDeleteNamespaceMockRootStorageContainerStore() throws Exception {
        String colName = "test-delete-namespace-no-root-storage-container-store";

        DeleteNamespaceResponse deleteResp = new DeleteNamespaceResponse()
            .setCode(StatusCode.NAMESPACE_NOT_FOUND);
        DeleteNamespaceRequest request = createDeleteNamespaceRequest(colName);

        when(mockRangeStoreService.deleteNamespace(request))
            .thenReturn(CompletableFuture.completedFuture(deleteResp));

        CompletableFuture<DeleteNamespaceResponse> deleteRespFuture =
            fromListenableFuture(futureCall(RootRangeServiceGrpc.getDeleteNamespaceMethod(), request));
        verify(mockRangeStoreService, times(1)).deleteNamespace(request);
        assertEquals(deleteResp, deleteRespFuture.get());
    }

    @Test
    public void testGetNamespaceMockRootStorageContainerStore() throws Exception {
        String colName = "test-get-namespace-no-root-storage-container-store";

        GetNamespaceResponse getResp = new GetNamespaceResponse()
            .setCode(StatusCode.NAMESPACE_NOT_FOUND);
        GetNamespaceRequest request = createGetNamespaceRequest(colName);

        when(mockRangeStoreService.getNamespace(request)).thenReturn(
            CompletableFuture.completedFuture(getResp));

        CompletableFuture<GetNamespaceResponse> getRespFuture =
            fromListenableFuture(futureCall(RootRangeServiceGrpc.getGetNamespaceMethod(), request));
        verify(mockRangeStoreService, times(1)).getNamespace(request);
        assertEquals(getResp, getRespFuture.get());
    }

    //
    // Test Stream API
    //

    @Test
    public void testCreateStreamNoRootStorageContainerStore() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        String colName = "test-create-namespace-no-root-storage-container-store";
        String streamName = colName;
        verifyNotFoundException(fromListenableFuture(
            futureCall(RootRangeServiceGrpc.getCreateStreamMethod(),
                createCreateStreamRequest(colName, streamName, DEFAULT_STREAM_CONF))),
            Status.NOT_FOUND);
    }

    @Test
    public void testDeleteStreamNoRootStorageContainerStore() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        String colName = "test-delete-namespace-no-root-storage-container-store";
        String streamName = colName;
        verifyNotFoundException(fromListenableFuture(
            futureCall(RootRangeServiceGrpc.getDeleteStreamMethod(), createDeleteStreamRequest(colName, streamName))),
            Status.NOT_FOUND);
    }

    @Test
    public void testGetStreamNoRootStorageContainerStore() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        String colName = "test-get-namespace-no-root-storage-container-store";
        String streamName = colName;
        verifyNotFoundException(fromListenableFuture(
            futureCall(RootRangeServiceGrpc.getGetStreamMethod(), createGetStreamRequest(colName, streamName))),
            Status.NOT_FOUND);
    }

    @Test
    public void testCreateStreamMockRootStorageContainerStore() throws Exception {
        String colName = "test-create-namespace-mock-root-storage-container-store";
        String streamName = colName;

        CreateStreamResponse createResp = new CreateStreamResponse()
            .setCode(StatusCode.STREAM_EXISTS);
        CreateStreamRequest createReq = createCreateStreamRequest(colName, streamName, DEFAULT_STREAM_CONF);
        when(mockRangeStoreService.createStream(createReq)).thenReturn(
            CompletableFuture.completedFuture(createResp));

        CompletableFuture<CreateStreamResponse> createRespFuture =
            fromListenableFuture(futureCall(RootRangeServiceGrpc.getCreateStreamMethod(), createReq));
        verify(mockRangeStoreService, times(1)).createStream(createReq);
        assertEquals(createResp, createRespFuture.get());
    }

    @Test
    public void testDeleteStreamMockRootStorageContainerStore() throws Exception {
        String colName = "test-delete-namespace-no-root-storage-container-store";
        String streamName = colName;

        DeleteStreamResponse deleteResp = new DeleteStreamResponse()
            .setCode(StatusCode.STREAM_NOT_FOUND);
        DeleteStreamRequest deleteReq = createDeleteStreamRequest(colName, streamName);
        when(mockRangeStoreService.deleteStream(deleteReq)).thenReturn(
            CompletableFuture.completedFuture(deleteResp));

        CompletableFuture<DeleteStreamResponse> deleteRespFuture =
            fromListenableFuture(futureCall(RootRangeServiceGrpc.getDeleteStreamMethod(), deleteReq));
        verify(mockRangeStoreService, times(1)).deleteStream(deleteReq);
        assertEquals(deleteResp, deleteRespFuture.get());
    }

    @Test
    public void testGetStreamMockRootStorageContainerStore() throws Exception {
        String colName = "test-get-namespace-no-root-storage-container-store";
        String streamName = colName;

        GetStreamResponse getResp = new GetStreamResponse()
            .setCode(StatusCode.STREAM_NOT_FOUND);
        GetStreamRequest getReq = createGetStreamRequest(colName, streamName);
        when(mockRangeStoreService.getStream(getReq)).thenReturn(
            CompletableFuture.completedFuture(getResp));

        CompletableFuture<GetStreamResponse> getRespFuture =
            fromListenableFuture(futureCall(RootRangeServiceGrpc.getGetStreamMethod(), getReq));
        verify(mockRangeStoreService, times(1)).getStream(getReq);
        assertEquals(getResp, getRespFuture.get());
    }

    @Test
    public void testGetActiveRangesNoManager() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        verifyNotFoundException(fromListenableFuture(
            futureCall(MetaRangeServiceGrpc.getGetActiveRangesMethod(), createGetActiveRangesRequest(34L))),
            Status.NOT_FOUND);
    }

    @Test
    public void testGetActiveRangesMockManager() throws Exception {
        GetActiveRangesResponse resp = new GetActiveRangesResponse()
            .setCode(StatusCode.STREAM_NOT_FOUND);
        GetActiveRangesRequest request = createGetActiveRangesRequest(34L);

        when(mockRangeStoreService.getActiveRanges(request))
            .thenReturn(CompletableFuture.completedFuture(resp));

        CompletableFuture<GetActiveRangesResponse> future = fromListenableFuture(
            futureCall(MetaRangeServiceGrpc.getGetActiveRangesMethod(), request));
        verify(mockRangeStoreService, times(1)).getActiveRanges(request);
        assertEquals(resp, future.get());
    }


    //
    // Table API
    //

    @Test
    public void testPutNoStorageContainer() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        verifyNotFoundException(fromListenableFuture(
            futureCall(TableServiceGrpc.getPutMethod(), createPutRequest())),
            Status.NOT_FOUND);
    }

    @Test
    public void testDeleteNoStorageContainer() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        verifyNotFoundException(fromListenableFuture(
            futureCall(TableServiceGrpc.getDeleteMethod(), createDeleteRequest())),
            Status.NOT_FOUND);
    }

    @Test
    public void testRangeNoStorageContainer() throws Exception {
        rangeStore.getRegistry().stopStorageContainer(scId).join();

        verifyNotFoundException(fromListenableFuture(
            futureCall(TableServiceGrpc.getRangeMethod(), createRangeRequest())),
            Status.NOT_FOUND);
    }

    @Test
    public void testRangeMockStorageContainer() throws Exception {
        RangeResponse response = createRangeResponse(StatusCode.SUCCESS);
        RangeRequest request = createRangeRequest();

        when(mockRangeStoreService.range(request))
            .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<RangeResponse> future = fromListenableFuture(
            futureCall(TableServiceGrpc.getRangeMethod(), request));
        verify(mockRangeStoreService, times(1)).range(eq(request));
        assertEquals(response, future.get());
    }

    @Test
    public void testDeleteMockStorageContainer() throws Exception {
        DeleteRangeResponse response = createDeleteResponse(StatusCode.SUCCESS);
        DeleteRangeRequest request = createDeleteRequest();

        when(mockRangeStoreService.delete(request))
            .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<DeleteRangeResponse> future = fromListenableFuture(
            futureCall(TableServiceGrpc.getDeleteMethod(), request));
        verify(mockRangeStoreService, times(1)).delete(eq(request));
        assertEquals(response, future.get());
    }

    @Test
    public void testPutMockStorageContainer() throws Exception {
        PutResponse response = createPutResponse(StatusCode.SUCCESS);
        PutRequest request = createPutRequest();

        when(mockRangeStoreService.put(request))
            .thenReturn(CompletableFuture.completedFuture(response));

        CompletableFuture<PutResponse> future = fromListenableFuture(
            futureCall(TableServiceGrpc.getPutMethod(), request));
        verify(mockRangeStoreService, times(1)).put(eq(request));
        assertEquals(response, future.get());
    }

}
