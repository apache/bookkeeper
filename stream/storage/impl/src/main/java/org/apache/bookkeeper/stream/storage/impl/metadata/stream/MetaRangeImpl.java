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

package org.apache.bookkeeper.stream.storage.impl.metadata.stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.MIN_DATA_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.isStreamCreated;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.split;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.Result;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.Bytes;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stream.proto.RangeMetadata;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.RangeState;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamMetadata;
import org.apache.bookkeeper.stream.proto.StreamMetadata.LifecycleState;
import org.apache.bookkeeper.stream.proto.StreamMetadata.ServingState;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.metadata.stream.MetaRange;
import org.apache.bookkeeper.stream.storage.exceptions.DataRangeNotFoundException;

/**
 * The default implementation of {@link MetaRange}.
 */
@Slf4j
public class MetaRangeImpl implements MetaRange {

    private static final byte METADATA_SEP = (byte) 0x1;
    private static final byte RANGE_SEP = (byte) 0x2;
    private static final byte END_SEP = (byte) 0xff;

    public static final byte[] getStreamMetadataKey(long streamId) {
        byte[] metadataKey = new byte[Long.BYTES + 1];
        Bytes.toBytes(streamId, metadataKey, 0);
        metadataKey[Long.BYTES] = METADATA_SEP;
        return metadataKey;
    }

    public static final byte[] getStreamRangeKey(long streamId, long rangeId) {
        byte[] rangeKey = new byte[2 * Long.BYTES + 1];
        Bytes.toBytes(streamId, rangeKey, 0);
        rangeKey[Long.BYTES] = RANGE_SEP;
        Bytes.toBytes(rangeId, rangeKey, Long.BYTES + 1);
        return rangeKey;
    }

    public static final byte[] getStreamMetadataEndKey(long streamId) {
        byte[] metadataKey = new byte[Long.BYTES + 1];
        Bytes.toBytes(streamId, metadataKey, 0);
        metadataKey[Long.BYTES] = END_SEP;
        return metadataKey;
    }

    public static final boolean isMetadataKey(byte[] key) {
        return key.length == (Long.BYTES + 1) && key[Long.BYTES] == METADATA_SEP;
    }

    static final boolean isStreamRangeKey(byte[] key) {
        return key.length == (2 * Long.BYTES + 1) && key[Long.BYTES] == RANGE_SEP;
    }

    private final MVCCAsyncStore<byte[], byte[]> store;
    private final ExecutorService executor;
    private final StorageContainerPlacementPolicy placementPolicy;

    @GuardedBy("this")
    private long streamId;
    @GuardedBy("this")
    private StreamProperties streamProps;
    private LifecycleState lifecycleState = LifecycleState.UNINIT;
    private ServingState servingState = ServingState.WRITABLE;
    private long cTime = 0L;
    private long mTime = 0L;

    private long nextRangeId = MIN_DATA_RANGE_ID;
    /**
     * All the ranges in the stream.
     *
     * <p>The ranges are sorted by range id. It implies that these
     * ranges are also sorted in the increasing order of their creation time.
     */
    private final NavigableMap<Long, RangeMetadata> ranges;
    /**
     * List of ranges that are currently active in the stream.
     */
    private final List<Long> currentRanges;

    // the revision of this meta range in local store.
    private long revision;

    public MetaRangeImpl(MVCCAsyncStore<byte[], byte[]> store,
                         ExecutorService executor,
                         StorageContainerPlacementPolicy placementPolicy) {
        this(
            store,
            executor,
            placementPolicy,
            Maps.newTreeMap(),
            Lists.newArrayList(),
            StreamMetadata
                .newBuilder()
                .setLifecycleState(LifecycleState.UNINIT)
                .setServingState(ServingState.WRITABLE)
                .setNextRangeId(MIN_DATA_RANGE_ID)
                .build(),
            0L,
            0L);
    }

    private MetaRangeImpl(MVCCAsyncStore<byte[], byte[]> store,
                          ExecutorService executor,
                          StorageContainerPlacementPolicy placementPolicy,
                          NavigableMap<Long, RangeMetadata> ranges,
                          List<Long> currentRanges,
                          StreamMetadata meta,
                          long cTime,
                          long mTime) {
        this.store = store;
        this.executor = executor;
        this.placementPolicy = placementPolicy;
        // construct the state
        this.ranges = ranges;
        this.currentRanges = currentRanges;
        this.cTime = cTime;
        this.mTime = mTime;
        this.streamProps = meta.getProps();
        this.streamId = streamProps.getStreamId();
        this.lifecycleState = meta.getLifecycleState();
        this.servingState = meta.getServingState();
        this.nextRangeId = meta.getNextRangeId();
    }

    @VisibleForTesting
    public long unsafeGetCreationTime() {
        return cTime;
    }

    @VisibleForTesting
    public long unsafeGetModificationTime() {
        return mTime;
    }

    @VisibleForTesting
    public LifecycleState unsafeGetLifecycleState() {
        return lifecycleState;
    }

    @VisibleForTesting
    public synchronized StreamProperties unsafeGetStreamProperties() {
        return streamProps;
    }

    @VisibleForTesting
    public synchronized long unsafeGetStreamId() {
        return streamId;
    }

    @VisibleForTesting
    public NavigableMap<Long, RangeMetadata> unsafeGetRanges() {
        return ranges;
    }

    @VisibleForTesting
    private synchronized StreamMetadata toStreamMetadata(LifecycleState state) {
        StreamMetadata.Builder builder = StreamMetadata
            .newBuilder()
            .setProps(streamProps)
            .setLifecycleState(state)
            .setServingState(servingState)
            .setNextRangeId(nextRangeId)
            .setCTime(cTime)
            .setMTime(mTime)
            .addAllCurrentRanges(currentRanges);
        return builder.build();
    }

    private synchronized StreamMetadata toStreamMetadata(ServingState state, long mTime) {
        StreamMetadata.Builder builder = StreamMetadata
            .newBuilder()
            .setProps(streamProps)
            .setLifecycleState(lifecycleState)
            .setServingState(state)
            .setNextRangeId(nextRangeId)
            .setCTime(cTime)
            .setMTime(mTime)
            .addAllCurrentRanges(currentRanges);
        return builder.build();
    }

    @VisibleForTesting
    public List<Long> unsafeGetCurrentRanges() {
        return currentRanges;
    }

    private <T> CompletableFuture<T> checkStreamCreated(Supplier<CompletableFuture<T>> supplier) {
        CompletableFuture<T> future = FutureUtils.createFuture();
        executor.submit(() -> {
            try {
                if (isStreamCreated(lifecycleState)) {
                    supplier.get()
                        .thenApplyAsync(value -> future.complete(value), executor)
                        .exceptionally(cause -> future.completeExceptionally(cause));
                } else {
                    throw new IllegalStateException("Stream isn't created yet.");
                }
            } catch (Throwable cause) {
                future.completeExceptionally(cause);
            }
        });
        return future;
    }

    private void checkLifecycleState(LifecycleState expected) {
        checkState(expected == lifecycleState, "Unexpected state " + lifecycleState + ", expected to be " + expected);
    }

    @Override
    public synchronized String getName() {
        checkState(null != streamProps);
        return streamProps.getStreamName();
    }

    private <T> CompletableFuture<T> executeTask(Consumer<CompletableFuture<T>> consumer) {
        CompletableFuture<T> executeFuture = FutureUtils.createFuture();
        executor.submit(() -> {
            try {
                consumer.accept(executeFuture);
            } catch (Throwable cause) {
                executeFuture.completeExceptionally(cause);
            }
        });
        return executeFuture;
    }

    @Override
    public CompletableFuture<Boolean> create(StreamProperties streamProps) {
        return executeTask((future) -> unsafeCreate(future, streamProps));
    }

    private void unsafeCreate(CompletableFuture<Boolean> createFuture,
                              StreamProperties streamProps) {
        // 1. verify the state
        checkLifecycleState(LifecycleState.UNINIT);
        this.lifecycleState = LifecycleState.CREATING;

        // 2. store the props/configuration
        synchronized (this) {
            this.streamProps = streamProps;
        }
        this.streamId = streamProps.getStreamId();
        this.cTime = this.mTime = System.currentTimeMillis();

        // 3. create the ranges
        List<RangeProperties> propertiesList =
            split(streamId, streamProps.getStreamConf().getInitialNumRanges(), nextRangeId, placementPolicy);
        List<Op<byte[], byte[]>> successOps = Lists.newArrayListWithExpectedSize(
            propertiesList.size() + 1);
        for (RangeProperties props : propertiesList) {
            RangeMetadata meta = RangeMetadata.newBuilder()
                .setProps(props)
                .setCreateTime(cTime)
                .setFenceTime(Long.MAX_VALUE)
                .setState(RangeState.RANGE_ACTIVE)
                .addAllParents(Lists.newArrayList())
                .build();
            ranges.put(props.getRangeId(), meta);
            currentRanges.add(props.getRangeId());

            successOps.add(
                store.newPut(
                    getStreamRangeKey(streamId, props.getRangeId()),
                    meta.toByteArray()));
        }
        nextRangeId += propertiesList.size();

        // serialize the stream metadata
        byte[] streamMetadataKey = getStreamMetadataKey(streamId);
        successOps.add(
            store.newPut(
                streamMetadataKey,
                toStreamMetadata(LifecycleState.CREATED).toByteArray()));

        TxnOp<byte[], byte[]> txn = store.newTxn()
            // create stream only when stream doesn't exists
            .If(store.newCompareValue(CompareResult.EQUAL, streamMetadataKey, null))
            .Then(successOps.toArray(new Op[successOps.size()]))
            .build();

        if (log.isTraceEnabled()) {
            log.trace("Execute create stream metadata range txn {}", streamProps);
        }
        store.txn(txn)
            .thenApplyAsync(txnResult -> {
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("Create stream metadata range txn result = {}", txnResult.isSuccess());
                    }
                    if (txnResult.isSuccess()) {
                        List<Result<byte[], byte[]>> results = txnResult.results();
                        MetaRangeImpl.this.revision = results.get(results.size() - 1).revision();

                        // mark the state to CREATED
                        this.lifecycleState = LifecycleState.CREATED;
                        createFuture.complete(true);
                    } else {
                        createFuture.complete(false);
                    }
                    return null;
                } finally {
                    txnResult.close();
                }
            }, executor)
            .exceptionally(cause -> {
                createFuture.completeExceptionally(cause);
                return null;
            });
    }

    @Override
    public CompletableFuture<MetaRange> load(long streamId) {
        byte[] streamMetadataKey = getStreamMetadataKey(streamId);
        byte[] streamMetadataEndKey = getStreamMetadataEndKey(streamId);

        return store.range(streamMetadataKey, streamMetadataEndKey)
            .thenApplyAsync(kvs -> {
                if (kvs.isEmpty()) {
                    return null;
                } else {
                    loadMetadata(kvs);
                    return MetaRangeImpl.this;
                }
            }, executor);
    }

    private void loadMetadata(List<KeyValue<byte[], byte[]>> kvs) {
        for (KeyValue<byte[], byte[]> kv : kvs) {
            if (isMetadataKey(kv.key())) {
                this.revision = kv.modifiedRevision();
                long streamId = Bytes.toLong(kv.key(), 0);
                loadStreamMetadata(streamId, kv.value());
            } else if (isStreamRangeKey(kv.key())) {
                long streamId = Bytes.toLong(kv.key(), 0);
                long rangeId = Bytes.toLong(kv.key(), Long.BYTES + 1);
                loadRangeMetadata(streamId, rangeId, kv.value());
            }
        }
    }

    private void loadStreamMetadata(long streamId, byte[] streamMetadataBytes) {
        this.streamId = streamId;
        StreamMetadata metadata;
        try {
            metadata = StreamMetadata.parseFrom(streamMetadataBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Invalid stream metadata of stream " + streamId, e);
        }

        this.streamProps = metadata.getProps();
        this.lifecycleState = metadata.getLifecycleState();
        this.servingState = metadata.getServingState();
        this.currentRanges.clear();
        this.currentRanges.addAll(metadata.getCurrentRangesList());
        this.nextRangeId = metadata.getNextRangeId();
        this.cTime = metadata.getCTime();
        this.mTime = metadata.getMTime();
    }

    private void loadRangeMetadata(long streamId, long rangeId, byte[] rangeMetadataBytes) {
        checkArgument(this.streamId == streamId);
        checkArgument(rangeId >= 0L);

        RangeMetadata metadata;
        try {
            metadata = RangeMetadata.parseFrom(rangeMetadataBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Invalid range metadata of range (" + streamId + ", " + rangeId + ")", e);
        }

        this.ranges.put(rangeId, metadata);
    }

    @Override
    public CompletableFuture<Boolean> delete(long streamId) {
        byte[] streamMetadataKey = getStreamMetadataKey(streamId);
        byte[] streamEndKey = getStreamMetadataEndKey(streamId);

        return store.deleteRange(streamMetadataKey, streamEndKey)
            .thenApplyAsync(kvs -> !kvs.isEmpty(), executor);
    }

    @Override
    public CompletableFuture<ServingState> getServingState() {
        return checkStreamCreated(() -> FutureUtils.value(servingState));
    }

    @Override
    public CompletableFuture<ServingState> updateServingState(ServingState state) {
        return checkStreamCreated(() -> {
            long mTime = System.currentTimeMillis();
            byte[] streamMetadataKey = getStreamMetadataKey(streamId);
            byte[] streamMetadata = toStreamMetadata(state, mTime).toByteArray();
            return store.rPut(streamMetadataKey, streamMetadata, revision)
                .thenApplyAsync(newRev -> {
                    this.servingState = state;
                    this.mTime = mTime;
                    this.revision = newRev;
                    return state;
                }, executor);
        });
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return checkStreamCreated(() -> FutureUtils.value(unsafeGetStreamProperties().getStreamConf()));
    }

    private RangeMetadata unsafeGetDataRange(long rangeId) throws DataRangeNotFoundException {
        RangeMetadata rangeMeta = ranges.get(rangeId);
        if (null == rangeMeta) {
            throw new DataRangeNotFoundException(streamId, rangeId);
        }
        return rangeMeta;
    }

    @Override
    public CompletableFuture<List<RangeMetadata>> getActiveRanges() {
        return checkStreamCreated(() -> {
            List<Long> rangesIds = ImmutableList.copyOf(currentRanges);
            List<RangeMetadata> properties = Lists.newArrayListWithExpectedSize(rangesIds.size());
            for (long rangeId : rangesIds) {
                properties.add(unsafeGetDataRange(rangeId));
            }
            return FutureUtils.value(properties);
        });
    }

}
