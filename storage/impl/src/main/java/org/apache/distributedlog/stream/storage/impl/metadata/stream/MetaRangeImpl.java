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

package org.apache.distributedlog.stream.storage.impl.metadata.stream;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.MIN_DATA_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.isStreamCreated;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.split;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import org.apache.bookkeeper.common.annotation.OrderedBy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.stream.proto.RangeMetadata;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.RangeState;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamMetadata;
import org.apache.distributedlog.stream.proto.StreamMetadata.LifecycleState;
import org.apache.distributedlog.stream.proto.StreamMetadata.ServingState;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.metadata.stream.MetaRange;
import org.apache.distributedlog.stream.storage.exceptions.DataRangeNotFoundException;

/**
 * The default implementation of {@link MetaRange}.
 */
public class MetaRangeImpl implements MetaRange {

  private final ExecutorService executor;
  private final StorageContainerPlacementPolicy placementPolicy;

  @GuardedBy("this")
  private long streamId;
  @GuardedBy("this")
  private StreamProperties streamProps;
  @OrderedBy(key = "streamId")
  private LifecycleState lifecycleState = LifecycleState.UNINIT;
  @OrderedBy(key = "streamId")
  private ServingState servingState = ServingState.WRITABLE;
  @OrderedBy(key = "streamId")
  private long cTime = 0L;
  @OrderedBy(key = "streamId")
  private long mTime = 0L;

  @OrderedBy(key = "streamId")
  private long nextRangeId = MIN_DATA_RANGE_ID;
  /**
   * All the ranges in the stream.
   *
   * <p>The ranges are sorted by range id. It implies that these
   * ranges are also sorted in the increasing order of their creation time.
   */
  @OrderedBy(key = "streamId")
  private final NavigableMap<Long, RangeMetadata> ranges;
  /**
   * List of ranges that are currently active in the stream.
   */
  @OrderedBy(key = "streamId")
  private final List<Long> currentRanges;

  public MetaRangeImpl(ExecutorService executor,
                       StorageContainerPlacementPolicy placementPolicy) {
    this(executor,
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

  private MetaRangeImpl(ExecutorService executor,
                        StorageContainerPlacementPolicy placementPolicy,
                        NavigableMap<Long, RangeMetadata> ranges,
                        List<Long> currentRanges,
                        StreamMetadata meta,
                        long cTime,
                        long mTime) {
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
  LifecycleState unsafeGetLifecycleState() {
    return lifecycleState;
  }

  @VisibleForTesting
  public synchronized StreamProperties unsafeGetStreamProperties() {
    return streamProps;
  }

  @VisibleForTesting
  synchronized long unsafeGetStreamId() {
    return streamId;
  }

  @VisibleForTesting
  public NavigableMap<Long, RangeMetadata> unsafeGetRanges() {
    return ranges;
  }

  @VisibleForTesting
  public synchronized StreamMetadata unsafeGetStreamMetadata() {
    StreamMetadata.Builder builder = StreamMetadata
      .newBuilder()
      .setProps(streamProps)
      .setLifecycleState(lifecycleState)
      .setServingState(servingState)
      .setNextRangeId(nextRangeId);

    return builder.build();
  }

  @VisibleForTesting
  public List<Long> unsafeGetCurrentRanges() {
    return currentRanges;
  }

  private <T> CompletableFuture<T> checkStreamCreated(Supplier<T> supplier) {
    CompletableFuture<T> future = FutureUtils.createFuture();
    executor.submit(() -> {
      try {
        if (isStreamCreated(lifecycleState)) {
          future.complete(supplier.get());
        } else {
          throw new IllegalStateException("Stream isn't created yet.");
        }
      } catch (Throwable cause) {
        future.completeExceptionally(cause);
      }
    });
    return future;
  }

  private void unsafeUpdateModificationTime() {
    this.mTime = System.currentTimeMillis();
  }

  @OrderedBy(key = "streamId")
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

  @OrderedBy(key = "streamId")
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
    }
    nextRangeId += propertiesList.size();

    // 4. mark the state to CREATED
    this.lifecycleState = LifecycleState.CREATED;

    createFuture.complete(true);
  }

  @Override
  public CompletableFuture<Boolean> delete() {
    return executeTask((future) -> future.complete(true));
  }

  @Override
  public CompletableFuture<ServingState> getServingState() {
    return checkStreamCreated(() -> servingState);
  }

  @Override
  public CompletableFuture<ServingState> updateServingState(ServingState state) {
    return checkStreamCreated(() -> {
      this.servingState = state;
      unsafeUpdateModificationTime();
      return this.servingState;
    });
  }

  @Override
  public CompletableFuture<StreamConfiguration> getConfiguration() {
    return checkStreamCreated(() -> unsafeGetStreamProperties().getStreamConf());
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
      return properties;
    });
  }

}
