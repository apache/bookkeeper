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

package org.apache.distributedlog.stream.storage.impl;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetActiveRangesRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetStreamRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.client.utils.NetUtils;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.StreamName;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;
import org.apache.distributedlog.stream.storage.RangeStoreBuilder;
import org.apache.distributedlog.stream.storage.StorageResources;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.sc.LocalStorageContainerManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test of {@link RangeStoreImpl}.
 */
@Slf4j
public class TestRangeStoreImpl {

  private static final StreamProperties streamProps = StreamProperties.newBuilder()
    .setStorageContainerId(System.currentTimeMillis())
    .setStreamId(System.currentTimeMillis())
    .setStreamName("test-create-add-stream-request")
    .setStreamConf(DEFAULT_STREAM_CONF)
    .build();

  private static StreamName createStreamName(String name) {
    return StreamName.newBuilder()
      .setColName(name + "_col")
      .setStreamName(name + "_stream")
      .build();
  }

  private final CompositeConfiguration compConf =
    new CompositeConfiguration();
  private final StorageConfiguration storageConf =
    new StorageConfiguration(compConf);
  private final CollectionConfiguration collectionConf =
    CollectionConfiguration.newBuilder()
      .setDefaultStreamConf(DEFAULT_STREAM_CONF)
      .build();
  private StorageResources storageResources;
  private RangeStoreImpl rangeStore;

  @Before
  public void setUp() throws Exception {
    storageResources = StorageResources.create();

    Endpoint endpoint = NetUtils.getLocalEndpoint(0, false);

    // create the client manager
    RangeServerClientManager clientManager = mock(RangeServerClientManager.class);

    rangeStore = (RangeStoreImpl) RangeStoreBuilder.newBuilder()
      .withStorageConfiguration(storageConf)
      .withStorageResources(storageResources)
      .withStorageContainerManagerFactory((numScs, storeConf, rgRegistry)
        -> new LocalStorageContainerManager(endpoint, storeConf, rgRegistry, 2))
      .withClientManagerSupplier(() -> clientManager)
      .build();
    rangeStore.start();
  }

  @After
  public void tearDown() {
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
  // Collection API
  //

  @Test
  public void testCreateCollectionNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-create-collection-no-root-storage-container-store";
    verifyNotFoundException(
      rangeStore.createCollection(createCreateCollectionRequest(colName, collectionConf)),
      Status.NOT_FOUND);
  }

  @Test
  public void testDeleteCollectionNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-delete-collection-no-root-storage-container-store";
    verifyNotFoundException(
      rangeStore.deleteCollection(createDeleteCollectionRequest(colName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testGetCollectionNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-get-collection-no-root-storage-container-store";
    verifyNotFoundException(
      rangeStore.getCollection(createGetCollectionRequest(colName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testCreateCollectionMockRootStorageContainerStore() throws Exception {
    String colName = "test-create-collection-mock-root-storage-container-store";

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    CreateCollectionResponse createResp = CreateCollectionResponse.newBuilder()
      .setCode(StatusCode.COLLECTION_EXISTS)
      .build();
    CreateCollectionRequest request = createCreateCollectionRequest(colName, collectionConf);

    when(scStore.createCollection(request))
      .thenReturn(CompletableFuture.completedFuture(createResp));

    CompletableFuture<CreateCollectionResponse> createRespFuture =
      rangeStore.createCollection(request);
    verify(scStore, times(1)).createCollection(request);
    assertTrue(createResp == createRespFuture.get());
  }

  @Test
  public void testDeleteCollectionMockRootStorageContainerStore() throws Exception {
    String colName = "test-delete-collection-no-root-storage-container-store";

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    DeleteCollectionResponse deleteResp = DeleteCollectionResponse.newBuilder()
      .setCode(StatusCode.COLLECTION_NOT_FOUND)
      .build();
    DeleteCollectionRequest request = createDeleteCollectionRequest(colName);

    when(scStore.deleteCollection(request))
      .thenReturn(CompletableFuture.completedFuture(deleteResp));

    CompletableFuture<DeleteCollectionResponse> deleteRespFuture =
      rangeStore.deleteCollection(request);
    verify(scStore, times(1)).deleteCollection(request);
    assertTrue(deleteResp == deleteRespFuture.get());
  }

  @Test
  public void testGetCollectionMockRootStorageContainerStore() throws Exception {
    String colName = "test-get-collection-no-root-storage-container-store";

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    GetCollectionResponse getResp = GetCollectionResponse.newBuilder()
      .setCode(StatusCode.COLLECTION_NOT_FOUND)
      .build();
    GetCollectionRequest request = createGetCollectionRequest(colName);

    when(scStore.getCollection(request)).thenReturn(
      CompletableFuture.completedFuture(getResp));

    CompletableFuture<GetCollectionResponse> getRespFuture =
      rangeStore.getCollection(request);
    verify(scStore, times(1)).getCollection(request);
    assertTrue(getResp == getRespFuture.get());
  }

  //
  // Test Stream API
  //

  @Test
  public void testCreateStreamNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-create-collection-no-root-storage-container-store";
    String streamName = colName;
    verifyNotFoundException(
      rangeStore.createStream(createCreateStreamRequest(colName, streamName, DEFAULT_STREAM_CONF)),
      Status.NOT_FOUND);
  }

  @Test
  public void testDeleteStreamNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-delete-collection-no-root-storage-container-store";
    String streamName = colName;
    verifyNotFoundException(
      rangeStore.deleteStream(createDeleteStreamRequest(colName, streamName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testGetStreamNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-get-collection-no-root-storage-container-store";
    String streamName = colName;
    verifyNotFoundException(
      rangeStore.getStream(createGetStreamRequest(colName, streamName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testCreateStreamMockRootStorageContainerStore() throws Exception {
    String colName = "test-create-collection-mock-root-storage-container-store";
    String streamName = colName;

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    CreateStreamResponse createResp = CreateStreamResponse.newBuilder()
      .setCode(StatusCode.STREAM_EXISTS)
      .build();
    CreateStreamRequest createReq = createCreateStreamRequest(colName, streamName, DEFAULT_STREAM_CONF);
    when(scStore.createStream(createReq)).thenReturn(
      CompletableFuture.completedFuture(createResp));

    CompletableFuture<CreateStreamResponse> createRespFuture =
      rangeStore.createStream(createReq);
    verify(scStore, times(1)).createStream(createReq);
    assertTrue(createResp == createRespFuture.get());
  }

  @Test
  public void testDeleteStreamMockRootStorageContainerStore() throws Exception {
    String colName = "test-delete-collection-no-root-storage-container-store";
    String streamName = colName;

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    DeleteStreamResponse deleteResp = DeleteStreamResponse.newBuilder()
      .setCode(StatusCode.STREAM_NOT_FOUND)
      .build();
    DeleteStreamRequest deleteReq = createDeleteStreamRequest(colName, streamName);
    when(scStore.deleteStream(deleteReq)).thenReturn(
      CompletableFuture.completedFuture(deleteResp));

    CompletableFuture<DeleteStreamResponse> deleteRespFuture =
      rangeStore.deleteStream(deleteReq);
    verify(scStore, times(1)).deleteStream(deleteReq);
    assertTrue(deleteResp == deleteRespFuture.get());
  }

  @Test
  public void testGetStreamMockRootStorageContainerStore() throws Exception {
    String colName = "test-get-collection-no-root-storage-container-store";
    String streamName = colName;

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    GetStreamResponse getResp = GetStreamResponse.newBuilder()
      .setCode(StatusCode.STREAM_NOT_FOUND)
      .build();
    GetStreamRequest getReq = createGetStreamRequest(colName, streamName);
    when(scStore.getStream(getReq)).thenReturn(
      CompletableFuture.completedFuture(getResp));

    CompletableFuture<GetStreamResponse> getRespFuture =
      rangeStore.getStream(getReq);
    verify(scStore, times(1)).getStream(getReq);
    assertTrue(getResp == getRespFuture.get());
  }

  @Test
  public void testGetActiveRangesNoManager() throws Exception {
    verifyNotFoundException(
      rangeStore.getActiveRanges(createGetActiveRangesRequest(12L, 34L)),
      Status.NOT_FOUND);
  }

  @Test
  public void testGetActiveRangesMockManager() throws Exception {
    long scId = System.currentTimeMillis();

    StreamProperties props = StreamProperties.newBuilder(streamProps)
      .setStorageContainerId(scId)
      .build();

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(scId, scStore);

    StorageContainerResponse resp = StorageContainerResponse.newBuilder()
      .setCode(StatusCode.STREAM_NOT_FOUND)
      .build();
    StorageContainerRequest request = createGetActiveRangesRequest(scId, 34L);

    when(scStore.getActiveRanges(request))
      .thenReturn(CompletableFuture.completedFuture(resp));

    CompletableFuture<StorageContainerResponse> future =
      rangeStore.getActiveRanges(request);
    verify(scStore, times(1)).getActiveRanges(request);
    assertTrue(resp == future.get());
  }

}
