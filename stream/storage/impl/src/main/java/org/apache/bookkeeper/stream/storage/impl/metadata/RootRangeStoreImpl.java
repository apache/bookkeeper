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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.MIN_DATA_STREAM_ID;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.validateNamespaceName;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.validateStreamName;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Bytes;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stream.proto.NamespaceMetadata;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamName;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest.IdCase;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.metadata.RootRangeStore;

/**
 * A statestore backed implementation of {@link RootRangeStore}.
 */
@Slf4j
public class RootRangeStoreImpl
    implements RootRangeStore {

    private static final byte SYSTEM_TAG = (byte) 0xff;
    private static final byte NS_NAME_TAG = (byte) 0x01;
    private static final byte NS_ID_TAG = (byte) 0x02;

    // separator used for separating streams within a same namespace
    private static final byte NS_STREAM_NAME_SEP = (byte) 0x03;
    private static final byte NS_STREAM_ID_SEP = (byte) 0x04;
    private static final byte STREAM_ID_TAG = (byte) 0x05;
    private static final byte NS_END_SEP = (byte) 0xff;

    static final byte[] NS_ID_KEY = new byte[]{SYSTEM_TAG, 'n', 's', 'i', 'd'};
    static final byte[] STREAM_ID_KEY = new byte[]{SYSTEM_TAG, 's', 't', 'r', 'e', 'a', 'm', 'i', 'd'};

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

    /**
     * stream id: [STREAM_ID_TAG][stream_id].
     */
    static final byte[] getStreamIdKey(long streamId) {
        byte[] streamIdBytes = new byte[Long.BYTES + 1];
        streamIdBytes[0] = STREAM_ID_TAG;
        Bytes.toBytes(streamId, streamIdBytes, 1);
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

    CompletableFuture<KeyValue<byte[], byte[]>> getValue(byte[] key) {
        RangeOp<byte[], byte[]> op = store.getOpFactory().newRange(
            key,
            Options.get());
        return store.range(op).thenApplyAsync(kvs -> {
            try {
                if (kvs.count() <= 0) {
                    return null;
                } else {
                    return kvs.getKvsAndClear().get(0);
                }
            } finally {
                kvs.close();
            }
        }).whenComplete((kv, cause) -> op.close());
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
                    nsIdKv.close();
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
                .setDefaultStreamConf(request.getNsConf().getDefaultStreamConf()))
            .build();

        byte[] nsNameKey = getNamespaceNameKey(nsName);
        byte[] nsNameVal = Bytes.toBytes(namespaceId);
        byte[] nsIdKey = getNamespaceIdKey(namespaceId);
        byte[] nsIdVal = metadata.toByteArray();

        TxnOp<byte[], byte[]> txn = store.newTxn()
            .If(
                store.newCompareValue(CompareResult.EQUAL, nsNameKey, null),
                currentNsIdRev < 0
                    ? store.newCompareValue(CompareResult.EQUAL, NS_ID_KEY, null) :
                    store.newCompareModRevision(CompareResult.EQUAL, NS_ID_KEY, currentNsIdRev)
            )
            .Then(
                store.newPut(nsNameKey, nsNameVal),
                store.newPut(nsIdKey, nsIdVal),
                store.newPut(NS_ID_KEY, Bytes.toBytes(namespaceId)))
            .build();
        return store.txn(txn)
            .thenApply(txnResult -> {
                try {
                    CreateNamespaceResponse.Builder respBuilder = CreateNamespaceResponse.newBuilder();
                    if (txnResult.isSuccess()) {
                        respBuilder.setCode(StatusCode.SUCCESS);
                        respBuilder.setNsProps(metadata.getProps());
                    } else {
                        // TODO: differentiate the error code
                        respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
                    }
                    return respBuilder.build();
                } finally {
                    txnResult.close();
                }
            })
            .whenComplete((resp, cause) -> txn.close());
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


        TxnOp<byte[], byte[]> txnOp = store.newTxn()
            .If(
                store.newCompareValue(CompareResult.NOT_EQUAL, nsNameKey, null),
                store.newCompareValue(CompareResult.NOT_EQUAL, nsIdKey, null)
            )
            .Then(
                store.newDelete(nsNameKey),
                store.newDeleteRange(nsIdKey, nsIdEndKey)
            )
            .build();

        return store.txn(txnOp).thenApply(txnResult -> {
            try {
                DeleteNamespaceResponse.Builder respBuilder = DeleteNamespaceResponse.newBuilder();
                if (txnResult.isSuccess()) {
                    respBuilder.setCode(StatusCode.SUCCESS);
                } else {
                    // TODO: differentiate the error code
                    respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
                }
                return respBuilder.build();
            } finally {
                txnResult.close();
            }
        }).whenComplete((resp, cause) -> txnOp.close());
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
                    nsRespBuilder.setNsProps(nsMetadata.getProps());
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
        String nsName = request.getNsName();

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
            .exceptionally(cause ->
                CreateStreamResponse.newBuilder().setCode(StatusCode.INTERNAL_SERVER_ERROR).build());
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
                    streamIdKv.close();
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
        byte[] streamReverseIndexKey = getStreamIdKey(streamId);
        byte[] streamReverseIndexValue = Bytes.toBytes(nsId);

        TxnOp<byte[], byte[]> txn = store.newTxn()
            .If(
                store.newCompareValue(CompareResult.NOT_EQUAL, nsIdKey, null),
                currentStreamIdRev < 0
                    ? store.newCompareValue(CompareResult.EQUAL, STREAM_ID_KEY, null) :
                    store.newCompareModRevision(CompareResult.EQUAL, STREAM_ID_KEY, currentStreamIdRev),
                store.newCompareValue(CompareResult.EQUAL, streamNameKey, null)
            )
            .Then(
                store.newPut(streamNameKey, streamNameVal),
                store.newPut(streamIdKey, streamIdVal),
                store.newPut(streamReverseIndexKey, streamReverseIndexValue),
                store.newPut(STREAM_ID_KEY, Bytes.toBytes(streamId))
            )
            .build();

        return store.txn(txn)
            .thenApply(txnResult -> {
                try {
                    CreateStreamResponse.Builder respBuilder = CreateStreamResponse.newBuilder();
                    if (txnResult.isSuccess()) {
                        respBuilder.setCode(StatusCode.SUCCESS);
                        respBuilder.setStreamProps(streamProps);
                    } else {
                        // TODO: differentiate the error codes
                        respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
                    }
                    return respBuilder.build();
                } finally {
                    txnResult.close();
                    txn.close();
                }
            })
            .exceptionally(cause -> {
                txn.close();
                return CreateStreamResponse.newBuilder().setCode(StatusCode.INTERNAL_SERVER_ERROR).build();
            });
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
        String streamName = request.getName();
        String nsName = request.getNsName();

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
        byte[] streamReverseIndexKey = getStreamIdKey(streamId);

        TxnOp<byte[], byte[]> txnOp = store.newTxn()
            .If(
                store.newCompareValue(CompareResult.NOT_EQUAL, nsIdKey, null),
                store.newCompareValue(CompareResult.NOT_EQUAL, streamNameKey, null),
                store.newCompareValue(CompareResult.NOT_EQUAL, streamIdKey, null),
                store.newCompareValue(CompareResult.NOT_EQUAL, streamReverseIndexKey, null)
            )
            .Then(
                store.newDelete(streamIdKey),
                store.newDelete(streamNameKey),
                store.newDelete(streamReverseIndexKey)
            )
            .build();

        return store.txn(txnOp).thenApply(txnResult -> {
            try {
                DeleteStreamResponse.Builder respBuilder = DeleteStreamResponse.newBuilder();
                if (txnResult.isSuccess()) {
                    respBuilder.setCode(StatusCode.SUCCESS);
                } else {
                    respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR);
                }
                return respBuilder.build();
            } finally {
                txnResult.close();
            }
        }).whenComplete((resp, cause) -> txnOp.close());
    }

    private CompletableFuture<GetStreamResponse> streamPropertiesToResponse(
        CompletableFuture<StreamProperties> propsFuture
    ) {
        GetStreamResponse.Builder respBuilder = GetStreamResponse.newBuilder();
        return propsFuture.thenCompose(streamProps -> {
            if (null == streamProps) {
                return FutureUtils.value(respBuilder.setCode(StatusCode.STREAM_NOT_FOUND).build());
            } else {
                return FutureUtils.value(respBuilder
                    .setCode(StatusCode.SUCCESS)
                    .setStreamProps(streamProps)
                    .build());
            }
        }).exceptionally(cause ->
            respBuilder
                .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                .build()
        );
    }

    @Override
    public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
        if (IdCase.STREAM_ID == request.getIdCase()) {
            return streamPropertiesToResponse(
                getStreamProps(request.getStreamId()));
        } else if (IdCase.STREAM_NAME == request.getIdCase()) {
            return getStreamProps(request.getStreamName());
        } else {
            return FutureUtils.value(GetStreamResponse.newBuilder()
                .setCode(StatusCode.ILLEGAL_OP)
                .build());
        }
    }

    CompletableFuture<StreamProperties> getStreamProps(long streamId) {
        byte[] streamReverseIndexKey = getStreamIdKey(streamId);

        return store.get(streamReverseIndexKey).thenCompose(nsIdBytes -> {
            if (null == nsIdBytes) {
                return FutureUtils.value(null);
            }

            long nsId = Bytes.toLong(nsIdBytes, 0);
            return getStreamProps(nsId, streamId);
        });
    }

    CompletableFuture<GetStreamResponse> getStreamProps(StreamName streamName) {
        StatusCode code = verifyStreamRequest(
                streamName.getNamespaceName(),
                streamName.getStreamName());
        if (StatusCode.SUCCESS != code) {
            return FutureUtils.value(GetStreamResponse.newBuilder()
                .setCode(code).build());
        }

        byte[] nsNameKey = getNamespaceNameKey(streamName.getNamespaceName());


        return store.get(nsNameKey)
            .thenCompose(nsIdBytes -> {
                if (null == nsIdBytes) {
                    return FutureUtils.value(GetStreamResponse.newBuilder()
                        .setCode(StatusCode.NAMESPACE_NOT_FOUND).build());
                }
                long nsId = Bytes.toLong(nsIdBytes, 0);
                return streamPropertiesToResponse(
                    getStreamProps(nsId, streamName.getStreamName()));
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
