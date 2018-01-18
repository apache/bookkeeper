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
package org.apache.distributedlog.stream.storage.impl.sc;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.CONTAINER_META_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.CONTAINER_META_STREAM_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STREAM_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.PutRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RoutingHeader;
import org.apache.distributedlog.stream.proto.kv.rpc.TxnRequest;
import org.apache.distributedlog.stream.proto.storage.CreateNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.CreateNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.storage.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.storage.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.storage.GetNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.GetNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.GetStreamRequest;
import org.apache.distributedlog.stream.proto.storage.GetStreamResponse;
import org.apache.distributedlog.stream.proto.storage.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.storage.StorageContainerRequest.Type;
import org.apache.distributedlog.stream.proto.storage.StorageContainerResponse;
import org.apache.distributedlog.stream.protocol.RangeId;
import org.apache.distributedlog.stream.storage.api.kv.TableStore;
import org.apache.distributedlog.stream.storage.api.metadata.MetaRangeStore;
import org.apache.distributedlog.stream.storage.api.metadata.RootRangeStore;
import org.apache.distributedlog.stream.storage.impl.kv.TableStoreFactory;
import org.apache.distributedlog.stream.storage.impl.metadata.MetaRangeStoreFactory;
import org.apache.distributedlog.stream.storage.impl.metadata.RootRangeStoreFactory;
import org.apache.distributedlog.stream.storage.impl.store.MVCCStoreFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link StorageContainerImpl}.
 */
public class StorageContainerImplTest {

    private static final long SCID = 3456L;
    private static final long STREAM_ID = 1234L;
    private static final long RANGE_ID = 3456L;
    private static final RangeId RID = RangeId.of(STREAM_ID, RANGE_ID);

    private MVCCStoreFactory mvccStoreFactory;
    private RootRangeStoreFactory rrStoreFactory;
    private MetaRangeStoreFactory mrStoreFactory;
    private TableStoreFactory tableStoreFactory;
    private StorageContainerImpl container;
    private OrderedScheduler scheduler;
    private RootRangeStore rrStore;
    private MVCCAsyncStore<byte[], byte[]> rrMvccStore;
    private MetaRangeStore mrStore;
    private MVCCAsyncStore<byte[], byte[]> mrMvccStore;
    private TableStore trStore;
    private MVCCAsyncStore<byte[], byte[]> trMvccStore;

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

        this.container = new StorageContainerImpl(
            SCID,
            scheduler,
            mvccStoreFactory,
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
            eq(ROOT_RANGE_ID))
        ).thenReturn(FutureUtils.value(rrMvccStore));
        when(mvccStoreFactory.openStore(
            eq(scId),
            eq(CONTAINER_META_STREAM_ID),
            eq(CONTAINER_META_RANGE_ID))
        ).thenReturn(FutureUtils.value(mrMvccStore));
        when(mvccStoreFactory.openStore(
            eq(scId),
            eq(STREAM_ID),
            eq(RANGE_ID)
        )).thenReturn(FutureUtils.value(trMvccStore));
        this.rrStore = mock(RootRangeStore.class);
        when(rrStoreFactory.createStore(eq(rrMvccStore)))
            .thenReturn(rrStore);
        this.mrStore = mock(MetaRangeStore.class);
        when(mrStoreFactory.createStore(eq(mrMvccStore)))
            .thenReturn(mrStore);
        this.trStore = mock(TableStore.class);
        when(tableStoreFactory.createStore(eq(trMvccStore)))
            .thenReturn(trStore);
    }

    @Test
    public void testStart() throws Exception {
        mockStorageContainer(SCID);

        FutureUtils.result(container.start());

        // root range is not started because it is not the root container
        verify(mvccStoreFactory, times(0))
            .openStore(eq(ROOT_STORAGE_CONTAINER_ID), eq(ROOT_STREAM_ID), eq(ROOT_RANGE_ID));
        verify(rrStoreFactory, times(0))
            .createStore(eq(rrMvccStore));

        // meta range should be started
        verify(mvccStoreFactory, times(1))
            .openStore(eq(SCID), eq(CONTAINER_META_STREAM_ID), eq(CONTAINER_META_RANGE_ID));
        verify(mrStoreFactory, times(1))
            .createStore(eq(mrMvccStore));
    }

    @Test
    public void testStartRootContainer() throws Exception {
        mockStorageContainer(ROOT_STORAGE_CONTAINER_ID);

        StorageContainerImpl container = new StorageContainerImpl(
            ROOT_STORAGE_CONTAINER_ID,
            scheduler,
            mvccStoreFactory,
            rrStoreFactory,
            mrStoreFactory,
            tableStoreFactory);
        FutureUtils.result(container.start());

        // root range is not started because it is not the root container
        verify(mvccStoreFactory, times(1))
            .openStore(eq(ROOT_STORAGE_CONTAINER_ID), eq(ROOT_STREAM_ID), eq(ROOT_RANGE_ID));
        verify(rrStoreFactory, times(1))
            .createStore(eq(rrMvccStore));

        // meta range should be started
        verify(mvccStoreFactory, times(1))
            .openStore(eq(ROOT_STORAGE_CONTAINER_ID), eq(CONTAINER_META_STREAM_ID), eq(CONTAINER_META_RANGE_ID));
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

        CreateNamespaceResponse expectedResp = CreateNamespaceResponse.getDefaultInstance();
        when(rrStore.createNamespace(any(CreateNamespaceRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        CreateNamespaceRequest expectedReq = CreateNamespaceRequest.getDefaultInstance();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.createNamespace(expectedReq)));
        verify(rrStore, times(1))
            .createNamespace(same(expectedReq));
    }

    @Test
    public void testDeleteNamespace() throws Exception {
        mockStorageContainer(SCID);

        DeleteNamespaceResponse expectedResp = DeleteNamespaceResponse.getDefaultInstance();
        when(rrStore.deleteNamespace(any(DeleteNamespaceRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        DeleteNamespaceRequest expectedReq = DeleteNamespaceRequest.getDefaultInstance();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.deleteNamespace(expectedReq)));
        verify(rrStore, times(1))
            .deleteNamespace(same(expectedReq));
    }

    @Test
    public void testGetNamespace() throws Exception {
        mockStorageContainer(SCID);

        GetNamespaceResponse expectedResp = GetNamespaceResponse.getDefaultInstance();
        when(rrStore.getNamespace(any(GetNamespaceRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        GetNamespaceRequest expectedReq = GetNamespaceRequest.getDefaultInstance();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.getNamespace(expectedReq)));
        verify(rrStore, times(1))
            .getNamespace(same(expectedReq));
    }

    @Test
    public void testCreateStream() throws Exception {
        mockStorageContainer(SCID);

        CreateStreamResponse expectedResp = CreateStreamResponse.getDefaultInstance();
        when(rrStore.createStream(any(CreateStreamRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        CreateStreamRequest expectedReq = CreateStreamRequest.getDefaultInstance();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.createStream(expectedReq)));
        verify(rrStore, times(1))
            .createStream(same(expectedReq));
    }

    @Test
    public void testDeleteStream() throws Exception {
        mockStorageContainer(SCID);

        DeleteStreamResponse expectedResp = DeleteStreamResponse.getDefaultInstance();
        when(rrStore.deleteStream(any(DeleteStreamRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        DeleteStreamRequest expectedReq = DeleteStreamRequest.getDefaultInstance();
        assertSame(
            expectedResp,
            FutureUtils.result(rrStore.deleteStream(expectedReq)));
        verify(rrStore, times(1))
            .deleteStream(same(expectedReq));
    }

    @Test
    public void testGetStream() throws Exception {
        mockStorageContainer(SCID);

        GetStreamResponse expectedResp = GetStreamResponse.getDefaultInstance();
        when(rrStore.getStream(any(GetStreamRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        GetStreamRequest expectedReq = GetStreamRequest.getDefaultInstance();
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

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(mrStore.getActiveRanges(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        StorageContainerRequest expectedReq = StorageContainerRequest.getDefaultInstance();
        assertSame(
            expectedResp,
            FutureUtils.result(mrStore.getActiveRanges(expectedReq)));
        verify(mrStore, times(1))
            .getActiveRanges(same(expectedReq));
    }

    //
    // Table API
    //

    private StorageContainerRequest newStorageContainerRequest(Type type) {
        StorageContainerRequest.Builder reqBuilder = StorageContainerRequest.newBuilder()
            .setScId(SCID)
            .setType(type);
        RoutingHeader header = RoutingHeader.newBuilder()
            .setStreamId(STREAM_ID)
            .setRangeId(RANGE_ID)
            .build();
        switch (type) {
            case KV_PUT:
                reqBuilder = reqBuilder.setKvPutReq(
                    PutRequest.newBuilder().setHeader(header));
                break;
            case KV_DELETE:
                reqBuilder = reqBuilder.setKvDeleteReq(
                    DeleteRangeRequest.newBuilder().setHeader(header));
                break;
            case KV_RANGE:
                reqBuilder = reqBuilder.setKvRangeReq(
                    RangeRequest.newBuilder().setHeader(header));
                break;
            case KV_TXN:
                reqBuilder = reqBuilder.setKvTxnReq(
                    TxnRequest.newBuilder().setHeader(header));
                break;
            default:
                break;
        }
        return reqBuilder.build();
    }

    @Test
    public void testRangeWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.range(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_RANGE);
        StorageContainerResponse response = FutureUtils.result(container.range(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testRangeWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.range(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_RANGE);
        StorageContainerResponse response = FutureUtils.result(container.range(request));
        assertSame(expectedResp, response);
    }

    @Test
    public void testPutWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.put(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_PUT);
        StorageContainerResponse response = FutureUtils.result(container.put(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testPutWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.put(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_PUT);
        StorageContainerResponse response = FutureUtils.result(container.put(request));
        assertSame(expectedResp, response);
    }

    @Test
    public void testDeleteWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.delete(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_DELETE);
        StorageContainerResponse response = FutureUtils.result(container.delete(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testDeleteWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.delete(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_DELETE);
        StorageContainerResponse response = FutureUtils.result(container.delete(request));
        assertSame(expectedResp, response);
    }

    @Test
    public void testTxnWhenTableStoreNotCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.txn(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_TXN);
        StorageContainerResponse response = FutureUtils.result(container.txn(request));
        assertSame(expectedResp, response);
        assertSame(trStore, container.getTableStoreCache().getTableStore(RID));
    }

    @Test
    public void testTxnWhenTableStoreCached() throws Exception {
        mockStorageContainer(SCID);

        StorageContainerResponse expectedResp = StorageContainerResponse.getDefaultInstance();
        when(trStore.txn(any(StorageContainerRequest.class)))
            .thenReturn(FutureUtils.value(expectedResp));
        container.getTableStoreCache().getTableStores().put(RID, trStore);

        StorageContainerRequest request = newStorageContainerRequest(Type.KV_TXN);
        StorageContainerResponse response = FutureUtils.result(container.txn(request));
        assertSame(expectedResp, response);
    }
}
