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

package org.apache.distributedlog.stream.storage.impl.metadata;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.MIN_DATA_STREAM_ID;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateNamespaceName;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateStreamName;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Bytes;
import org.apache.distributedlog.statelib.api.mvcc.KVRecord;
import org.apache.distributedlog.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.statelib.api.mvcc.op.CompareResult;
import org.apache.distributedlog.statelib.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statelib.api.mvcc.op.PutOp;
import org.apache.distributedlog.statelib.api.mvcc.op.RangeOp;
import org.apache.distributedlog.statelib.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statelib.api.mvcc.op.TxnOpBuilder;
import org.apache.distributedlog.stream.proto.NamespaceMetadata;
import org.apache.distributedlog.stream.proto.NamespaceProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamName;
import org.apache.distributedlog.stream.proto.StreamProperties;
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
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.metadata.RootRangeStore;

/**
 * A statestore backed implementation of {@link RootRangeStore}.
 */
@Slf4j
public class RootRangeStoreImpl
    implements RootRangeStore {

  private static final byte SYSTEM_TAG = (byte) 0xff;
  private static final byte NS_NAME_TAG = (byte) 0x01;
  private static final byte NS_ID_TAG = (byte) 0x01;

  // separator used for separating streams within a same namespace
  private static final byte NS_STREAM_NAME_SEP = (byte) 0x03;
  private static final byte NS_STREAM_ID_SEP = (byte) 0x04;
  private static final byte NS_END_SEP = (byte) 0xff;

  static final byte[] NS_ID_KEY = new byte[] { SYSTEM_TAG, 'n', 's', 'i', 'd' };
  static final byte[] STREAM_ID_KEY = new byte[] { SYSTEM_TAG, 's', 't', 'r', 'e', 'a', 'm', 'i', 'd' };

  /**
   * ns name key: [NS_NAME_TAG][ns_name].
   */
  static final byte[] getNamespaceNameKey(String nsName) {
    byte[] nsNameBytes = nsName.getBytes(UTF_8);
    byte[] nsNameKey = new byte[nsNameBytes.length + 1];
    System.arraycopy(nsNameBytes, 0, nsNameKey, 1, nsNameBytes.length);
    nsNameKey[0] = NS_NAME_TAG;
    return nsNameKey;
  }

  /**
   * ns id key: [NS_ID_TAG][ns_id].
   */
  static final byte[] getNamespaceIdKey(long nsId) {
    byte[] nsIdBytes = new byte[Long.BYTES + 1];
    nsIdBytes[0] = NS_ID_TAG;
    Bytes.toBytes(nsId, nsIdBytes, 1);
    return nsIdBytes;
  }

  /**
   * ns id end key: [NS_ID_TAG][ns_id][NS_END_SEP].
   */
  static final byte[] getNamespaceIdEndKey(long nsId) {
    byte[] nsIdBytes = new byte[Long.BYTES + 2];
    nsIdBytes[0] = NS_ID_TAG;
    Bytes.toBytes(nsId, nsIdBytes, 1);
    nsIdBytes[Long.BYTES + 1] = NS_END_SEP;
    return nsIdBytes;
  }

  /**
   * stream name key: [NS_ID_TAG][ns_id][STREAM_NAME_SEP][stream_name].
   */
  static final byte[] getStreamNameKey(long nsId, String streamName) {
    byte[] streamNameBytes = streamName.getBytes(UTF_8);
    byte[] streamNameKey = new byte[streamNameBytes.length + Long.BYTES + 2];
    streamNameKey[0] = NS_ID_TAG;
    Bytes.toBytes(nsId, streamNameKey, 1);
    streamNameKey[Long.BYTES + 1] = NS_STREAM_NAME_SEP;
    System.arraycopy(streamNameBytes, 0, streamNameKey, Long.BYTES + 2, streamNameBytes.length);
    return streamNameKey;
  }

  /**
   * stream name id: [NS_ID_TAG][ns_id][STREAM_ID_SEP][stream_id].
   */
  static final byte[] getStreamIdKey(long nsId, long streamId) {
    byte[] streamIdBytes = new byte[2 * Long.BYTES + 2];
    streamIdBytes[0] = NS_ID_TAG;
    Bytes.toBytes(nsId, streamIdBytes, 1);
    streamIdBytes[Long.BYTES + 1] = NS_STREAM_ID_SEP;
    Bytes.toBytes(streamId, streamIdBytes, Long.BYTES + 2);
    return streamIdBytes;
  }

  private final MVCCAsyncStore<byte[], byte[]> store;
  private final StorageContainerPlacementPolicy placementPolicy;
  private final ScheduledExecutorService executor;

  public RootRangeStoreImpl(MVCCAsyncStore<byte[], byte[]> store,
                            StorageContainerPlacementPolicy placementPolicy,
                            ScheduledExecutorService executor) {
    this.store = store;
    this.placementPolicy = placementPolicy;
    this.executor = executor;
  }

  //
  // Namespaces API
  //

  CompletableFuture<KVRecord<byte[], byte[]>> getValue(byte[] key) {
    RangeOp<byte[], byte[]> op = store.getOpFactory().buildRangeOp()
         .isRangeOp(false)
         .key(key)
         .build();

    return store.range(op).thenApplyAsync(kvs -> {
        if (kvs.count() <= 0) {
          return null;
        } else {
          return kvs.kvs().get(0);
        }
    });
  }

  PutOp<byte[], byte[]> newPut(byte[] key, byte[] value) {
    return store.getOpFactory().buildPutOp()
        .key(key)
        .value(value)
        .prevKV(false)
        .build();
  }

  DeleteOp<byte[], byte[]> newDelete(byte[] key) {
    return store.getOpFactory().buildDeleteOp()
        .key(key)
        .isRangeOp(false)
        .build();
  }

  DeleteOp<byte[], byte[]> newDeleteRange(byte[] key, byte[] endKey) {
    return store.getOpFactory().buildDeleteOp()
        .key(key)
        .endKey(endKey)
        .isRangeOp(true)
        .build();
  }

  @Override
  public CompletableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
    if (log.isTraceEnabled()) {
      log.trace("Received CreateNamespace request : {}", request);
    }

    return CreateNamespaceProcessor.of().process(
      this,
      request,
      executor);
  }

  StatusCode verifyCreateNamespaceRequest(CreateNamespaceRequest request) {
    String colName = request.getName();
    StatusCode code = StatusCode.SUCCESS;
    if (!validateNamespaceName(colName)) {
      log.error("Failed to create namespace due to invalid namespace name {}", colName);
      code = StatusCode.INVALID_NAMESPACE_NAME;
    }
    return code;
  }

  CompletableFuture<CreateNamespaceResponse> doProcessCreateNamespaceRequest(CreateNamespaceRequest request) {

    return getValue(NS_ID_KEY)
        .thenCompose(nsIdKv -> {
          long currentNsId, currentNsIdRev;
          if (null == nsIdKv) {
            currentNsId = -1L;
            currentNsIdRev = -1L;
          } else {
            currentNsId = Bytes.toLong(nsIdKv.value(), 0);
            currentNsIdRev = nsIdKv.modifiedRevision();
            nsIdKv.recycle();
          }

          return executeCreateNamespaceTxn(currentNsId, currentNsIdRev, request);
        });
  }

  private CompletableFuture<CreateNamespaceResponse> executeCreateNamespaceTxn(long currentNsId,
                                                                               long currentNsIdRev,
                                                                               CreateNamespaceRequest request) {
    long namespaceId = currentNsId + 1;
    String nsName = request.getName();

    NamespaceMetadata metadata = NamespaceMetadata.newBuilder()
        .setProps(NamespaceProperties.newBuilder()
          .setNamespaceId(namespaceId)
          .setNamespaceName(nsName)
          .setDefaultStreamConf(request.getColConf().getDefaultStreamConf()))
        .build();

    byte[] nsNameKey = getNamespaceNameKey(nsName);
    byte[] nsNameVal = Bytes.toBytes(namespaceId);
    byte[] nsIdKey = getNamespaceIdKey(namespaceId);
    byte[] nsIdVal = metadata.toByteArray();

    TxnOpBuilder<byte[], byte[]> txnBuilder = store.getOpFactory().buildTxnOp()
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.EQUAL, nsNameKey, null))
        .addSuccessOps(
            newPut(nsNameKey, nsNameVal))
        .addSuccessOps(
            newPut(nsIdKey, nsIdVal))
        .addSuccessOps(
            newPut(NS_ID_KEY, Bytes.toBytes(namespaceId)));

    if (currentNsIdRev < 0) {
      txnBuilder = txnBuilder.addCompareOps(
          store.getOpFactory().compareValue(CompareResult.EQUAL, NS_ID_KEY, null));
    } else {
      txnBuilder = txnBuilder.addCompareOps(
          store.getOpFactory().compareModRevision(CompareResult.EQUAL, NS_ID_KEY, currentNsIdRev));
    }

    TxnOp<byte[], byte[]> txn = txnBuilder.build();

    return store.txn(txn)
        .thenApply(txnResult -> {
          CreateNamespaceResponse.Builder respBuilder = CreateNamespaceResponse.newBuilder();
          if (txnResult.isSuccess()) {
            respBuilder.setCode(StatusCode.SUCCESS);
            respBuilder.setColProps(metadata.getProps());
          } else {
            // TODO: differentiate the error code
            respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
          }
          return respBuilder.build();
        });
  }

  @Override
  public CompletableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
    return DeleteNamespaceProcessor.of().process(
      this,
      request,
      executor);
  }

  StatusCode verifyDeleteNamespaceRequest(DeleteNamespaceRequest request) {
    String colName = request.getName();
    StatusCode code = StatusCode.SUCCESS;
    if (!validateNamespaceName(colName)) {
      log.error("Failed to delete namespace due to invalid namespace name {}", colName);
      code = StatusCode.INVALID_NAMESPACE_NAME;
    }
    return code;
  }

  CompletableFuture<DeleteNamespaceResponse> doProcessDeleteNamespaceRequest(DeleteNamespaceRequest request) {

    String nsName = request.getName();

    return getNamespace(nsName)
        .thenCompose(nsMetadata -> deleteNamespace(nsName, nsMetadata));
  }

  CompletableFuture<DeleteNamespaceResponse> deleteNamespace(String nsName, NamespaceMetadata nsMetadata) {
    if (null == nsMetadata) {
      return FutureUtils.value(
          DeleteNamespaceResponse.newBuilder()
            .setCode(StatusCode.NAMESPACE_NOT_FOUND)
            .build());
    }

    byte[] nsNameKey = getNamespaceNameKey(nsName);
    byte[] nsIdKey = getNamespaceIdKey(nsMetadata.getProps().getNamespaceId());
    byte[] nsIdEndKey = getNamespaceIdEndKey(nsMetadata.getProps().getNamespaceId());


    TxnOp<byte[], byte[]> txnOp = store.getOpFactory().buildTxnOp()
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.NOT_EQUAL, nsNameKey, null))
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.NOT_EQUAL, nsIdKey, null))
        .addSuccessOps(
            newDelete(nsNameKey))
        .addSuccessOps(
            newDeleteRange(nsIdKey, nsIdEndKey))
        .build();

    return store.txn(txnOp).thenApply(txnResult -> {
      DeleteNamespaceResponse.Builder respBuilder = DeleteNamespaceResponse.newBuilder();
      if (txnResult.isSuccess()) {
        respBuilder.setCode(StatusCode.SUCCESS);
      } else {
        // TODO: differentiate the error code
        respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
      }
      return respBuilder.build();
    });
  }

  @Override
  public CompletableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
    String nsName = request.getName();
    if (!validateNamespaceName(nsName)) {
      log.error("Failed to get namespace due to invalid namespace name {}", nsName);
      return FutureUtils.value(
          GetNamespaceResponse.newBuilder()
              .setCode(StatusCode.INVALID_NAMESPACE_NAME)
              .build());
    } else {
      return getNamespace(request.getName()).thenApply(nsMetadata -> {
        GetNamespaceResponse.Builder nsRespBuilder = GetNamespaceResponse.newBuilder();
        if (null == nsMetadata) {
          nsRespBuilder.setCode(StatusCode.NAMESPACE_NOT_FOUND);
        } else {
          nsRespBuilder.setCode(StatusCode.SUCCESS);
          nsRespBuilder.setColProps(nsMetadata.getProps());
        }
        return nsRespBuilder.build();
      });
    }
  }

  private CompletableFuture<NamespaceMetadata> getNamespace(long nsId) {
    byte[] nsIdKey = getNamespaceIdKey(nsId);
    return store.get(nsIdKey).thenCompose(nsIdVal -> {
      try {
        return FutureUtils.value(null != nsIdVal ? NamespaceMetadata.parseFrom(nsIdVal) : null);
      } catch (InvalidProtocolBufferException e) {
        return FutureUtils.exception(e);
      }
    });
  }

  private CompletableFuture<NamespaceMetadata> getNamespace(String nsName) {
    byte[] nsNameKey = getNamespaceNameKey(nsName);
    return store.get(nsNameKey)
        .thenCompose(nsIdBytes -> {
          if (null == nsIdBytes) {
            return FutureUtils.value(null);
          } else {
            long nsId = Bytes.toLong(nsIdBytes, 0);
            return getNamespace(nsId);
          }
        });
  }

  //
  // Stream API
  //

  @Override
  public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
    String streamName = request.getName();
    String nsName = request.getColName();

    StatusCode code = verifyStreamRequest(nsName, streamName);
    if (StatusCode.SUCCESS != code) {
      return FutureUtils.value(CreateStreamResponse.newBuilder().setCode(code).build());
    }

    return createStream(nsName, streamName, request.getStreamConf());
  }

  StatusCode verifyStreamRequest(String nsName, String streamName) {
    StatusCode code = StatusCode.SUCCESS;
    if (!validateNamespaceName(nsName)) {
      log.error("Invalid namespace name {}", nsName);
      code = StatusCode.INVALID_NAMESPACE_NAME;
    } else if (!validateStreamName(streamName)) {
      log.error("Invalid stream name {}", streamName);
      code = StatusCode.INVALID_STREAM_NAME;
    }
    return code;
  }

  private CompletableFuture<CreateStreamResponse> createStream(String nsName,
                                                               String streamName,
                                                               StreamConfiguration streamConf) {
    return getNamespace(nsName)
        .thenCompose(nsMetadata -> createStream(
            nsMetadata,
            streamName,
            streamConf
        ))
        .exceptionally(cause -> CreateStreamResponse.newBuilder().setCode(StatusCode.INTERNAL_SERVER_ERROR).build());
  }

  private CompletableFuture<CreateStreamResponse> createStream(NamespaceMetadata nsMetadata,
                                                               String streamName,
                                                               StreamConfiguration streamConf) {
    if (null == nsMetadata) {
      return FutureUtils.value(CreateStreamResponse.newBuilder().setCode(StatusCode.NAMESPACE_NOT_FOUND).build());
    }

    return getValue(STREAM_ID_KEY)
        .thenCompose(streamIdKv -> {

          long currentStreamId = -1L;
          long currentStreamIdRev = -1L;
          if (null != streamIdKv) {
            currentStreamId = Bytes.toLong(streamIdKv.value(), 0);
            currentStreamIdRev = streamIdKv.modifiedRevision();
            streamIdKv.recycle();
          }

          return executeCreateStreamTxn(
              nsMetadata.getProps().getNamespaceId(),
              streamName,
              streamConf,
              currentStreamId,
              currentStreamIdRev);
        });

  }

  private CompletableFuture<CreateStreamResponse> executeCreateStreamTxn(long nsId,
                                                                         String streamName,
                                                                         StreamConfiguration streamConf,
                                                                         long currentStreamId,
                                                                         long currentStreamIdRev) {
    long streamId;
    if (currentStreamId < 0) {
      streamId = MIN_DATA_STREAM_ID;
    } else {
      streamId = currentStreamId + 1;
    }

    long scId = placementPolicy.placeStreamRange(streamId, 0L);

    StreamProperties streamProps = StreamProperties.newBuilder()
        .setStreamId(streamId)
        .setStreamName(streamName)
        .setStorageContainerId(scId)
        .setStreamConf(streamConf)
        .build();

    byte[] nsIdKey = getNamespaceIdKey(nsId);
    byte[] streamNameKey = getStreamNameKey(nsId, streamName);
    byte[] streamNameVal = Bytes.toBytes(streamId);
    byte[] streamIdKey = getStreamIdKey(nsId, streamId);
    byte[] streamIdVal = streamProps.toByteArray();

    TxnOpBuilder<byte[], byte[]> txnBuilder = store.getOpFactory().buildTxnOp()
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.NOT_EQUAL, nsIdKey, null))
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.EQUAL, streamNameKey, null))
        .addSuccessOps(
            newPut(streamNameKey, streamNameVal))
        .addSuccessOps(
            newPut(streamIdKey, streamIdVal))
        .addSuccessOps(
            newPut(STREAM_ID_KEY, Bytes.toBytes(streamId)));

    if (currentStreamIdRev < 0) {
      txnBuilder = txnBuilder.addCompareOps(
          store.getOpFactory().compareValue(CompareResult.EQUAL, STREAM_ID_KEY, null));
    } else {
      txnBuilder = txnBuilder.addCompareOps(
          store.getOpFactory().compareModRevision(CompareResult.EQUAL, STREAM_ID_KEY, currentStreamIdRev));
    }

    TxnOp<byte[], byte[]> txn = txnBuilder.build();

    return store.txn(txn)
        .thenApply(txnResult -> {

          CreateStreamResponse.Builder respBuilder = CreateStreamResponse.newBuilder();
          if (txnResult.isSuccess()) {
            respBuilder.setCode(StatusCode.SUCCESS);
            respBuilder.setStreamProps(streamProps);
          } else {
            // TODO: differentiate the error codes
            respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
          }
          return respBuilder.build();
        })
        .exceptionally(cause ->
            CreateStreamResponse.newBuilder().setCode(StatusCode.INTERNAL_SERVER_ERROR).build());
  }

  @Override
  public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
    String streamName = request.getName();
    String nsName = request.getColName();

    StatusCode code = verifyStreamRequest(nsName, streamName);
    if (StatusCode.SUCCESS != code) {
      return FutureUtils.value(DeleteStreamResponse.newBuilder().setCode(code).build());
    }

    return deleteStream(nsName, streamName);
  }

  private CompletableFuture<DeleteStreamResponse> deleteStream(String nsName,
                                                               String streamName) {
    return getNamespace(nsName)
        .thenCompose(nsMetadata -> deleteStream(nsMetadata, streamName));
  }

  private CompletableFuture<DeleteStreamResponse> deleteStream(NamespaceMetadata nsMetadata,
                                                               String streamName) {

    if (null == nsMetadata) {
      return FutureUtils.value(DeleteStreamResponse.newBuilder().setCode(StatusCode.NAMESPACE_NOT_FOUND).build());
    }

    long nsId = nsMetadata.getProps().getNamespaceId();
    byte[] streamNameKey = getStreamNameKey(nsId, streamName);

    return store.get(streamNameKey).thenCompose(streamIdBytes -> {
      if (null == streamIdBytes) {
        return FutureUtils.value(
            DeleteStreamResponse.newBuilder()
              .setCode(StatusCode.STREAM_NOT_FOUND)
              .build());
      }

      long streamId = Bytes.toLong(streamIdBytes, 0);
      return deleteStream(
          nsId,
          streamId,
          streamName);
    });
  }

  private CompletableFuture<DeleteStreamResponse> deleteStream(long nsId,
                                                               long streamId,
                                                               String streamName) {
    byte[] nsIdKey = getNamespaceIdKey(nsId);
    byte[] streamNameKey = getStreamNameKey(nsId, streamName);
    byte[] streamIdKey = getStreamIdKey(nsId, streamId);

    TxnOp<byte[], byte[]> txnOp = store.getOpFactory().buildTxnOp()
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.NOT_EQUAL, nsIdKey, null))
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.NOT_EQUAL, streamNameKey, null))
        .addCompareOps(
            store.getOpFactory().compareValue(CompareResult.NOT_EQUAL, streamIdKey, null))
        .addSuccessOps(
            newDelete(streamIdKey))
        .addSuccessOps(
            newDelete(streamNameKey))
        .build();

    return store.txn(txnOp).thenApply(txnResult -> {
      DeleteStreamResponse.Builder respBuilder = DeleteStreamResponse.newBuilder();
      if (txnResult.isSuccess()) {
        respBuilder.setCode(StatusCode.SUCCESS);
      } else {
        respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
      }
      return respBuilder.build();
    });
  }

  @Override
  public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
    StreamName streamName = request.getStreamName();

    StatusCode code = verifyStreamRequest(streamName.getColName(), streamName.getStreamName());
    if (StatusCode.SUCCESS != code) {
      return FutureUtils.value(GetStreamResponse.newBuilder().setCode(code).build());
    }

    byte[] nsNameKey = getNamespaceNameKey(streamName.getColName());
    GetStreamResponse.Builder respBuilder = GetStreamResponse.newBuilder();
    return store.get(nsNameKey)
        .thenCompose(nsIdBytes -> {
          if (null == nsIdBytes) {
            return FutureUtils.value(respBuilder.setCode(StatusCode.NAMESPACE_NOT_FOUND).build());
          }

          long nsId = Bytes.toLong(nsIdBytes, 0);
          return getStreamProps(nsId, streamName.getStreamName())
              .thenCompose(streamProps -> {
                if (null == streamProps) {
                  return FutureUtils.value(respBuilder.setCode(StatusCode.STREAM_NOT_FOUND).build());
                } else {
                  return FutureUtils.value(respBuilder
                      .setCode(StatusCode.SUCCESS)
                      .setStreamProps(streamProps)
                      .build());
                }
              })
              .exceptionally(cause -> respBuilder
                  .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                  .build());
        });
  }

  CompletableFuture<StreamProperties> getStreamProps(long nsId, String streamName) {
    byte[] streamNameKey = getStreamNameKey(nsId, streamName);

    return store.get(streamNameKey).thenCompose(streamIdBytes -> {
      if (null == streamIdBytes) {
        return FutureUtils.value(null);
      }

      long streamId = Bytes.toLong(streamIdBytes, 0);
      return getStreamProps(nsId, streamId);
    });
  }

  CompletableFuture<StreamProperties> getStreamProps(long nsId, long streamId) {
    byte[] streamIdKey = getStreamIdKey(nsId, streamId);

    return store.get(streamIdKey).thenCompose(streamPropBytes -> {
      if (null == streamPropBytes) {
        return FutureUtils.value(null);
      } else {
        try {
          return FutureUtils.value(StreamProperties.parseFrom(streamPropBytes));
        } catch (InvalidProtocolBufferException e) {
          return FutureUtils.exception(e);
        }
      }
    });
  }

}
