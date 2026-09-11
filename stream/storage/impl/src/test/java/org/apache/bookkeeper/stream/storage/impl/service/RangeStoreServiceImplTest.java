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
package org.apache.bookkeeper.stream.storage.impl.service;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.CONTAINER_META_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.CONTAINER_META_STREAM_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STREAM_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
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
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.apache.bookkeeper.stream.storage.api.kv.TableStore;
import org.apache.bookkeeper.stream.storage.api.metadata.MetaRangeStore;
import org.apache.bookkeeper.stream.storage.api.metadata.RootRangeStore;
import org.apache.bookkeeper.stream.storage.impl.kv.TableStoreFactory;
import org.apache.bookkeeper.stream.storage.impl.metadata.MetaRangeStoreFactory;
import org.apache.bookkeeper.stream.storage.impl.metadata.RootRangeStoreFactory;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link RangeStoreServiceImpl}.
 */
public class RangeStoreServiceImplTest {

    private static final long SCID = 3456L;
    private static final long STREAM_ID = 1234L;
    private static final long RANGE_ID = 3456L;
    private static final RangeId RID = RangeId.of(STREAM_ID, RANGE_ID);

    private MVCCStoreFactory mvccStoreFactory;
    private RootRangeStoreFactory rrStoreFactory;
    private MetaRangeStoreFactory mrStoreFactory;
    private TableStoreFactory tableStoreFactory;
    private RangeStoreServiceImpl container;
    private OrderedScheduler scheduler;
    private RootRangeStore rrStore;
    private MVCCAsyncStore<byte[], byte[]> rrMvccStore;
    private MetaRangeStore mrStore;
    private MVCCAsyncStore<byte[], byte[]> mrMvccStore;
    private TableStore trStore;
    private MVCCAsyncStore<byte[], byte[]> trMvccStore;

    private StorageServerClientManager clientManager;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        this.scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();
        this.mvccStoreFactory = mock(MVCCStoreFactory.class);
        this.rrStoreFactory = mock(RootRangeStoreFactory.class);
        this.mrStoreFactory = mock(MetaRangeStoreFactory.class);
        this.tableStoreFactory = mock(TableStoreFactory.class);

        this.rrMvccStore = mock(MVCCAsyncStore.class);
        this.mrMvccStore = mock(MVCCAsyncStore.class);
        this.trMvccStore = mock(MVCCAsyncStore.class);

        this.clientManager = mock(StorageServerClientManager.class);

        this.container = new RangeStoreServiceImpl(
            SCID,
            scheduler,
            mvccStoreFactory,
            clientManager,
            rrStoreFactory,
            mrStoreFactory,
            tableStoreFactory);

        assertEquals(SCID, this.container.getId());
    }

    @After
    public void tearDown() {
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    private void mockStorageContainer(long scId) {
        when(mvccStoreFactory.openStore(
            eq(ROOT_STORAGE_CONTAINER_ID),
            eq(ROOT_STREAM_ID),
            eq(ROOT_RANGE_ID),
            eq(0))
        ).thenReturn(FutureUtils.value(rrMvccStore));
        when(mvccStoreFactory.openStore(
            eq(scId),
            eq(CONTAINER_META_STREAM_ID),
            eq(CONTAINER_META_RANGE_ID),
            eq(0))
        ).thenReturn(FutureUtils.value(mrMvccStore));
        when(mvccStoreFactory.openStore(
            eq(scId),
            eq(STREAM_ID),
            eq(RANGE_ID),
            anyInt())
        ).thenReturn(FutureUtils.value(trMvccStore));
        this.rrStore = mock(RootRangeStore.class);
        when(rrStoreFactory.createStore(eq(rrMvccStore)))
            .thenReturn(rrStore);
        this.mrStore = mock(MetaRangeStore.class);
        when(mrStoreFactory.createStore(eq(mrMvccStore)))
            .thenReturn(mrStore);
        this.trStore = mock(TableStore.class);
        when(tableStoreFactory.createStore(eq(trMvccStore)))
            .thenReturn(trStore);
        when(clientManager.getStreamProperties(eq(STREAM_ID)))
            .thenReturn(FutureUtils.value(new StreamProperties()));
    }

    @Test
    public void testStart() throws Exception {
        mockStorageContainer(SCID);

        FutureUtils.result(container.start());

        // root range is not started because it is not the root container
        verify(mvccStoreFactory, times(0))
            .openStore(eq(ROOT_STORAGE_CONTAINER_ID), eq(ROOT_STREAM_ID), eq(ROOT_RANGE_ID), eq(0));
        verify(rrStoreFactory, times(0))
            .createStore(eq(rrMvccStore));

        // meta range should be started
        verify(mvccStoreFactory, times(1))
            .openStore(eq(SCID), eq(CONTAINER_META_STREAM_ID), eq(CONTAINER_META_RANGE_ID), eq(0));
        verify(mrStoreFactory, times(1))
            .createStore(eq(mrMvccStore));
    }

    @Test
    public void testStartRootContainer() throws Exception {
        mockStorageContainer(ROOT_STORAGE_CONTAINER_ID);

        RangeStoreServiceImpl container = new RangeStoreServiceImpl(
            ROOT_STORAGE_CONTAINER_ID,
            scheduler,
            mvccStoreFactory,
            clientManager,
            rrStoreFactory,
            mrStoreFactory,
            tableStoreFactory);
        FutureUtils.result(container.start());

        // root range is not started because it is not the root container
        verify(mvccStoreFactory, times(1))
            .openStore(eq(ROOT_STORAGE_CONTAINER_ID), eq(ROOT_STREAM_ID), eq(ROOT_RANGE_ID), eq(0));
        verify(rrStoreFactory, times(1))
            .createStore(eq(rrMvccStore));

        // meta range should be started
        verify(mvccStoreFactory, times(1))
            .openStore(eq(ROOT_STORAGE_CONTAINER_ID), eq(CONTAINER_META_STREAM_ID), eq(CONTAINER_META_RANGE_ID), eq(0));
        verify(mrStoreFactory, times(1))
            .createStore(eq(mrMvccStore));
    }

    @Test
    public void testClose() throws Exception {
        mockStorageContainer(SCID);

        when(mvccStoreFactory.closeStores(eq(SCID)))
            .thenReturn(FutureUtils.Void());

        FutureUtils.result(container.stop());

        verify(mvccStoreFactory, times(1))
            .closeStores(eq(SCID));
    }

    //
    // Root Range Methods
    //

    @Test
    public void testCreateNamespace() throws Exception {
        mockStorageContainer(SCID);

        CreateNamespaceResponse expectedResp = new CreateNamespaceResponse();
        when(rrStore.createNamespace(any(CreateNamespaceRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        CreateNamespaceRequest expectedReq = new CreateNamespaceRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.createNamespace(expectedReq)));
        verify(rrStore, times(1))
            .createNamespace(same(expectedReq));
    }

    @Test
    public void testDeleteNamespace() throws Exception {
        mockStorageContainer(SCID);

        DeleteNamespaceResponse expectedResp = new DeleteNamespaceResponse();
        when(rrStore.deleteNamespace(any(DeleteNamespaceRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        DeleteNamespaceRequest expectedReq = new DeleteNamespaceRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.deleteNamespace(expectedReq)));
        verify(rrStore, times(1))
            .deleteNamespace(same(expectedReq));
    }

    @Test
    public void testGetNamespace() throws Exception {
        mockStorageContainer(SCID);

        GetNamespaceResponse expectedResp = new GetNamespaceResponse();
        when(rrStore.getNamespace(any(GetNamespaceRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        GetNamespaceRequest expectedReq = new GetNamespaceRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.getNamespace(expectedReq)));
        verify(rrStore, times(1))
            .getNamespace(same(expectedReq));
    }

    @Test
    public void testCreateStream() throws Exception {
        mockStorageContainer(SCID);

        CreateStreamResponse expectedResp = new CreateStreamResponse();
        when(rrStore.createStream(any(CreateStreamRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        CreateStreamRequest expectedReq = new CreateStreamRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.createStream(expectedReq)));
        verify(rrStore, times(1))
            .createStream(same(expectedReq));
    }

    @Test
    public void testDeleteStream() throws Exception {
        mockStorageContainer(SCID);

        DeleteStreamResponse expectedResp = new DeleteStreamResponse();
        when(rrStore.deleteStream(any(DeleteStreamRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        DeleteStreamRequest expectedReq = new DeleteStreamRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.deleteStream(expectedReq)));
        verify(rrStore, times(1))
            .deleteStream(same(expectedReq));
    }

    @Test
    public void testGetStream() throws Exception {
        mockStorageContainer(SCID);

        GetStreamResponse expectedResp = new GetStreamResponse();
        when(rrStore.getStream(any(GetStreamRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        GetStreamRequest expectedReq = new GetStreamRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.getStream(expectedReq)));
        verify(rrStore, times(1))
            .getStream(same(expectedReq));
    }

    //
    // Meta Range Methods
    //

    @Test
    public void testGetActiveRanges() throws Exception {
        mockStorageContainer(SCID);

        GetActiveRangesResponse expectedResp = new GetActiveRangesResponse();
        when(mrStore.getActiveRanges(any(GetActiveRangesRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        GetActiveRangesRequest expectedReq = new GetActiveRangesRequest();
        assertSame(
            expectedResp,
            FutureUtils.result(mrStore.getActiveRanges(expectedReq)));
        verify(mrStore, times(1))
            .getActiveRanges(same(expectedReq));
    }

    //
    // Table API
    //

    private static RoutingHeader newRoutingHeader() {
        return new RoutingHeader()
            .setStreamId(STREAM_ID)
            .setRangeId(RANGE_ID);
    }

    private PutRequest newPutRequest() {
        PutRequest req = new PutRequest();
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private DeleteRangeRequest newDeleteRequest() {
        DeleteRangeRequest req = new DeleteRangeRequest();
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private RangeRequest newRangeRequest() {
        RangeRequest req = new RangeRequest();
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private IncrementRequest newIncrRequest() {
        IncrementRequest req = new IncrementRequest();
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private TxnRequest newTxnRequest() {
        TxnRequest req = new TxnRequest();
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    @Test
    public void testRangeWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        RangeResponse expectedResp = new RangeResponse();
        when(trStore.range(any(RangeRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        RangeRequest request = newRangeRequest();
        RangeResponse response = FutureUtils.result(container.range(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testRangeWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        RangeResponse expectedResp = new RangeResponse();
        when(trStore.range(any(RangeRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        RangeRequest request = newRangeRequest();
        RangeResponse response = FutureUtils.result(container.range(request));
        assertSame(expectedResp, response);
    }

    @Test
    public void testPutWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        PutResponse expectedResp = new PutResponse();
        when(trStore.put(any(PutRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        PutRequest request = newPutRequest();
        PutResponse response = FutureUtils.result(container.put(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testPutWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        PutResponse expectedResp = new PutResponse();
        when(trStore.put(any(PutRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        PutRequest request = newPutRequest();
        PutResponse response = FutureUtils.result(container.put(request));
        assertSame(expectedResp, response);
    }

    @Test
    public void testDeleteWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        DeleteRangeResponse expectedResp = new DeleteRangeResponse();
        when(trStore.delete(any(DeleteRangeRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        DeleteRangeRequest request = newDeleteRequest();
        DeleteRangeResponse response = FutureUtils.result(container.delete(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testDeleteWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        DeleteRangeResponse expectedResp = new DeleteRangeResponse();
        when(trStore.delete(any(DeleteRangeRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        DeleteRangeRequest request = newDeleteRequest();
        DeleteRangeResponse response = FutureUtils.result(container.delete(request));
        assertSame(expectedResp, response);
    }

    @Test
    public void testTxnWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        TxnResponse expectedResp = new TxnResponse();
        when(trStore.txn(any(TxnRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        TxnRequest request = newTxnRequest();
        TxnResponse response = FutureUtils.result(container.txn(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testTxnWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        TxnResponse expectedResp = new TxnResponse();
        when(trStore.txn(any(TxnRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        TxnRequest request = newTxnRequest();
        TxnResponse response = FutureUtils.result(container.txn(request));
        assertSame(expectedResp, response);
    }
}
