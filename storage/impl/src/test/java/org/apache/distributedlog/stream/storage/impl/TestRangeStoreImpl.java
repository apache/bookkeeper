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
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateNamespaceRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteNamespaceRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetActiveRangesRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetNamespaceRequest;
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
import org.apache.distributedlog.clients.impl.internal.api.StorageServerClientManager;
import org.apache.distributedlog.clients.utils.NetUtils;
import org.apache.distributedlog.stream.proto.NamespaceConfiguration;
import org.apache.distributedlog.stream.proto.StreamName;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.common.Endpoint;
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
import org.apache.distributedlog.stream.proto.storage.StatusCode;
import org.apache.distributedlog.stream.proto.storage.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.storage.StorageContainerResponse;
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
  private final NamespaceConfiguration namespaceConf =
    NamespaceConfiguration.newBuilder()
      .setDefaultStreamConf(DEFAULT_STREAM_CONF)
      .build();
  private StorageResources storageResources;
  private RangeStoreImpl rangeStore;

  @Before
  public void setUp() throws Exception {
    storageResources = StorageResources.create();

    Endpoint endpoint = NetUtils.getLocalEndpoint(0, false);

    // create the client manager
    StorageServerClientManager clientManager = mock(StorageServerClientManager.class);

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
  // Namespace API
  //

  @Test
  public void testCreateNamespaceNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-create-namespace-no-root-storage-container-store";
    verifyNotFoundException(
      rangeStore.createNamespace(createCreateNamespaceRequest(colName, namespaceConf)),
      Status.NOT_FOUND);
  }

  @Test
  public void testDeleteNamespaceNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-delete-namespace-no-root-storage-container-store";
    verifyNotFoundException(
      rangeStore.deleteNamespace(createDeleteNamespaceRequest(colName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testGetNamespaceNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-get-namespace-no-root-storage-container-store";
    verifyNotFoundException(
      rangeStore.getNamespace(createGetNamespaceRequest(colName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testCreateNamespaceMockRootStorageContainerStore() throws Exception {
    String colName = "test-create-namespace-mock-root-storage-container-store";

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    CreateNamespaceResponse createResp = CreateNamespaceResponse.newBuilder()
      .setCode(StatusCode.COLLECTION_EXISTS)
      .build();
    CreateNamespaceRequest request = createCreateNamespaceRequest(colName, namespaceConf);

    when(scStore.createNamespace(request))
      .thenReturn(CompletableFuture.completedFuture(createResp));

    CompletableFuture<CreateNamespaceResponse> createRespFuture =
      rangeStore.createNamespace(request);
    verify(scStore, times(1)).createNamespace(request);
    assertTrue(createResp == createRespFuture.get());
  }

  @Test
  public void testDeleteNamespaceMockRootStorageContainerStore() throws Exception {
    String colName = "test-delete-namespace-no-root-storage-container-store";

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    DeleteNamespaceResponse deleteResp = DeleteNamespaceResponse.newBuilder()
      .setCode(StatusCode.COLLECTION_NOT_FOUND)
      .build();
    DeleteNamespaceRequest request = createDeleteNamespaceRequest(colName);

    when(scStore.deleteNamespace(request))
      .thenReturn(CompletableFuture.completedFuture(deleteResp));

    CompletableFuture<DeleteNamespaceResponse> deleteRespFuture =
      rangeStore.deleteNamespace(request);
    verify(scStore, times(1)).deleteNamespace(request);
    assertTrue(deleteResp == deleteRespFuture.get());
  }

  @Test
  public void testGetNamespaceMockRootStorageContainerStore() throws Exception {
    String colName = "test-get-namespace-no-root-storage-container-store";

    StorageContainer scStore = mock(StorageContainer.class);
    when(scStore.stop()).thenReturn(FutureUtils.value(null));
    rangeStore.getRegistry().setStorageContainer(ROOT_STORAGE_CONTAINER_ID, scStore);
    GetNamespaceResponse getResp = GetNamespaceResponse.newBuilder()
      .setCode(StatusCode.COLLECTION_NOT_FOUND)
      .build();
    GetNamespaceRequest request = createGetNamespaceRequest(colName);

    when(scStore.getNamespace(request)).thenReturn(
      CompletableFuture.completedFuture(getResp));

    CompletableFuture<GetNamespaceResponse> getRespFuture =
      rangeStore.getNamespace(request);
    verify(scStore, times(1)).getNamespace(request);
    assertTrue(getResp == getRespFuture.get());
  }

  //
  // Test Stream API
  //

  @Test
  public void testCreateStreamNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-create-namespace-no-root-storage-container-store";
    String streamName = colName;
    verifyNotFoundException(
      rangeStore.createStream(createCreateStreamRequest(colName, streamName, DEFAULT_STREAM_CONF)),
      Status.NOT_FOUND);
  }

  @Test
  public void testDeleteStreamNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-delete-namespace-no-root-storage-container-store";
    String streamName = colName;
    verifyNotFoundException(
      rangeStore.deleteStream(createDeleteStreamRequest(colName, streamName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testGetStreamNoRootStorageContainerStore() throws Exception {
    rangeStore.getRegistry().stopStorageContainer(ROOT_STORAGE_CONTAINER_ID).join();

    String colName = "test-get-namespace-no-root-storage-container-store";
    String streamName = colName;
    verifyNotFoundException(
      rangeStore.getStream(createGetStreamRequest(colName, streamName)),
      Status.NOT_FOUND);
  }

  @Test
  public void testCreateStreamMockRootStorageContainerStore() throws Exception {
    String colName = "test-create-namespace-mock-root-storage-container-store";
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
    String colName = "test-delete-namespace-no-root-storage-container-store";
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
    String colName = "test-get-namespace-no-root-storage-container-store";
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
