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
package org.apache.bookkeeper.stream.storage.impl.kv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.apache.bookkeeper.stream.storage.api.kv.TableStore;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link TableStoreCache}.
 */
public class TableStoreCacheTest {

    private static final long SCID = 3456L;
    private static final RangeId RID = RangeId.of(1234L, 3456L);

    private MVCCStoreFactory factory;
    private TableStoreCache storeCache;

    @Before
    public void setUp() {
        this.factory = mock(MVCCStoreFactory.class);
        this.storeCache = new TableStoreCache(this.factory);
    }

    @Test
    public void testGetTableStoreWithoutOpen() {
        assertNull(storeCache.getTableStore(RID));
        assertTrue(storeCache.getTableStores().isEmpty());
        assertTrue(storeCache.getTableStoresOpening().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOpenTableStoreSuccessWhenStoreIsNotCached() throws Exception {
        assertNull(storeCache.getTableStore(RID));
        assertTrue(storeCache.getTableStores().isEmpty());
        assertTrue(storeCache.getTableStoresOpening().isEmpty());

        MVCCAsyncStore<byte[], byte[]> mvccStore = mock(MVCCAsyncStore.class);
        when(factory.openStore(eq(SCID), eq(RID.getStreamId()), eq(RID.getRangeId()), anyInt()))
            .thenReturn(FutureUtils.value(mvccStore));
        TableStore store = FutureUtils.result(storeCache.openTableStore(SCID, RID, 0));
        assertEquals(1, storeCache.getTableStores().size());
        assertEquals(0, storeCache.getTableStoresOpening().size());
        assertTrue(storeCache.getTableStores().containsKey(RID));
        assertSame(store, storeCache.getTableStores().get(RID));
    }

    @Test
    public void testOpenTableStoreFailureWhenStoreIsNotCached() throws Exception {
        assertNull(storeCache.getTableStore(RID));
        assertTrue(storeCache.getTableStores().isEmpty());
        assertTrue(storeCache.getTableStoresOpening().isEmpty());

        Exception cause = new Exception("Failure");
        when(factory.openStore(eq(SCID), eq(RID.getStreamId()), eq(RID.getRangeId()), anyInt()))
            .thenReturn(FutureUtils.exception(cause));
        try {
            FutureUtils.result(storeCache.openTableStore(SCID, RID, 0));
            fail("Should fail to open table if the underlying factory fails to open a local store");
        } catch (Exception ee) {
            assertSame(cause, ee);
        }
        assertEquals(0, storeCache.getTableStores().size());
        assertEquals(0, storeCache.getTableStoresOpening().size());
    }

    @Test
    public void testOpenTableStoreWhenStoreIsCached() throws Exception {
        TableStore store = mock(TableStore.class);
        storeCache.getTableStores().put(RID, store);

        assertSame(store, storeCache.getTableStore(RID));
        assertSame(store, FutureUtils.result(storeCache.openTableStore(SCID, RID, 0)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConcurrentOpenTableStore() throws Exception {
        MVCCAsyncStore<byte[], byte[]> mvccStore1 = mock(MVCCAsyncStore.class);
        MVCCAsyncStore<byte[], byte[]> mvccStore2 = mock(MVCCAsyncStore.class);
        CompletableFuture<MVCCAsyncStore<byte[], byte[]>> future1 = FutureUtils.createFuture();
        CompletableFuture<MVCCAsyncStore<byte[], byte[]>> future2 = FutureUtils.createFuture();
        when(factory.openStore(eq(SCID), eq(RID.getStreamId()), eq(RID.getRangeId()), anyInt()))
            .thenReturn(future1)
            .thenReturn(future2);

        CompletableFuture<TableStore> openFuture1 =
            storeCache.openTableStore(SCID, RID, 0);
        assertEquals(0, storeCache.getTableStores().size());
        assertEquals(1, storeCache.getTableStoresOpening().size());
        CompletableFuture<TableStore> openFuture2 =
            storeCache.openTableStore(SCID, RID, 0);
        assertEquals(0, storeCache.getTableStores().size());
        assertEquals(1, storeCache.getTableStoresOpening().size());
        assertSame(openFuture1, openFuture2);

        future1.complete(mvccStore1);
        future1.complete(mvccStore2);

        TableStore store1 = FutureUtils.result(openFuture1);
        TableStore store2 = FutureUtils.result(openFuture2);
        assertSame(store1, store2);
        assertEquals(0, storeCache.getTableStoresOpening().size());
        assertEquals(1, storeCache.getTableStores().size());
        assertSame(store1, storeCache.getTableStores().get(RID));

        verify(factory, times(1))
            .openStore(eq(SCID), eq(RID.getStreamId()), eq(RID.getRangeId()), anyInt());
    }

}
