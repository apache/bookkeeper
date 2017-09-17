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

package org.apache.distributedlog.stream.client.impl.view.kv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.ByteBufHashRouter;
import org.apache.distributedlog.stream.api.view.kv.Table;
import org.apache.distributedlog.stream.api.view.kv.options.DeleteOption;
import org.apache.distributedlog.stream.api.view.kv.options.GetOption;
import org.apache.distributedlog.stream.api.view.kv.options.PutOption;
import org.apache.distributedlog.stream.api.view.kv.result.DeleteResult;
import org.apache.distributedlog.stream.api.view.kv.result.GetResult;
import org.apache.distributedlog.stream.api.view.kv.result.PutResult;
import org.apache.distributedlog.stream.client.impl.RangeRouter;
import org.apache.distributedlog.stream.client.internal.api.HashStreamRanges;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * The default implemenation of {@link Table}.
 */
@Slf4j
public class TableImpl implements Table {

  private static class FailRequestKeyValueSpace implements Table {

    static final FailRequestKeyValueSpace INSTANCE =
      new FailRequestKeyValueSpace();

    private static final IllegalStateException CAUSE =
      new IllegalStateException("No range found for a given routing key");

    private FailRequestKeyValueSpace() {}

    @Override
    public CompletableFuture<GetResult> get(ByteBuf pKey,
                                            ByteBuf lKey,
                                            GetOption option) {
      return FutureUtils.exception(CAUSE);
    }

    @Override
    public CompletableFuture<PutResult> put(ByteBuf pKey,
                                            ByteBuf lKey,
                                            ByteBuf value,
                                            PutOption option) {
      return FutureUtils.exception(CAUSE);
    }

    @Override
    public CompletableFuture<DeleteResult> delete(ByteBuf pKey,
                                                  ByteBuf lKey,
                                                  DeleteOption option) {
      return FutureUtils.exception(CAUSE);
    }

    @Override
    public void close() {
      // no-op
    }
  }

  private final String streamName;
  private final StreamProperties props;
  private final RangeServerClientManager clientManager;
  private final ScheduledExecutorService executor;
  private final TableRangeFactory trFactory;

  // States
  private final RangeRouter<ByteBuf> rangeRouter;
  private final ConcurrentMap<Long, Table> tableRanges;


  public TableImpl(String streamName,
                   StreamProperties props,
                   RangeServerClientManager clientManager,
                   ScheduledExecutorService executor) {
    this(
      streamName,
      props,
      clientManager,
      executor,
      (streamProps, rangeProps, scheduler) -> new TableRangeImpl(
        streamProps.getStreamId(),
        rangeProps,
        clientManager.getStorageContainerChannel(streamProps.getStorageContainerId()),
        executor),
      Optional.empty());
  }

  public TableImpl(String streamName,
                   StreamProperties props,
                   RangeServerClientManager clientManager,
                   ScheduledExecutorService executor,
                   TableRangeFactory factory,
                   Optional<RangeRouter<ByteBuf>> rangeRouterOverride) {
    this.streamName = streamName;
    this.props = props;
    this.clientManager = clientManager;
    this.executor = executor;
    this.trFactory = factory;
    this.rangeRouter =
      rangeRouterOverride.orElse(new RangeRouter<ByteBuf>(ByteBufHashRouter.of()));
    this.tableRanges = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  ConcurrentMap<Long, Table> getTableRanges() {
    return tableRanges;
  }

  private Table getTableRange(Long range) {
    Table tRange = tableRanges.get(range);
    // TODO: we need logic to handle scale/repartitioning
    if (null == tRange) {
      return FailRequestKeyValueSpace.INSTANCE;
    }
    return tRange;
  }

  public CompletableFuture<Table> initialize() {
    return this.clientManager
      .openMetaRangeClient(props)
      .getActiveDataRanges()
      .thenComposeAsync((ranges) -> refreshRangeSpaces(ranges), executor);
  }

  CompletableFuture<Table> refreshRangeSpaces(HashStreamRanges newRanges) {
    // compare the ranges to see if it requires an update
    HashStreamRanges oldRanges = rangeRouter.getRanges();
    if (null != oldRanges && oldRanges.getMaxRangeId() >= newRanges.getMaxRangeId()) {
      log.info("No new stream ranges found for stream {}.", streamName);
      return FutureUtils.value(this);
    }
    if (log.isInfoEnabled()) {
      log.info("Updated the active ranges to {}", newRanges);
    }
    rangeRouter.setRanges(newRanges);
    // add new ranges
    Set<Long> activeRanges = Sets.newHashSetWithExpectedSize(newRanges.getRanges().size());
    newRanges.getRanges().forEach((rk, range) -> {
      activeRanges.add(range.getRangeId());
      if (tableRanges.containsKey(range.getRangeId())) {
        return;
      }
      Table tableRange = trFactory.openTableRange(props, range, executor);
      if (log.isInfoEnabled()) {
        log.info("Create table range client for range {}", range.getRangeId());
      }
      this.tableRanges.put(range.getRangeId(), tableRange);
    });
    // remove old ranges
    Iterator<Map.Entry<Long, Table>> rsIter = tableRanges.entrySet().iterator();
    while (rsIter.hasNext()) {
      Map.Entry<Long, Table> entry = rsIter.next();
      Long rid = entry.getKey();
      if (activeRanges.contains(rid)) {
        continue;
      }
      rsIter.remove();
      Table oldRangeSpace = entry.getValue();
      oldRangeSpace.close();
    }
    return FutureUtils.value(this);
  }

  @Override
  public CompletableFuture<GetResult> get(ByteBuf pKey,
                                          ByteBuf lKey,
                                          GetOption option) {
    Long range = rangeRouter.getRange(pKey);
    return getTableRange(range).get(pKey, lKey, option);
  }

  @Override
  public CompletableFuture<PutResult> put(ByteBuf pKey,
                                          ByteBuf lKey,
                                          ByteBuf value,
                                          PutOption option) {
    Long range = rangeRouter.getRange(pKey);
    return getTableRange(range).put(pKey, lKey, value, option);
  }

  @Override
  public CompletableFuture<DeleteResult> delete(ByteBuf pKey,
                                                ByteBuf lKey,
                                                DeleteOption option) {
    Long range = rangeRouter.getRange(pKey);
    return getTableRange(range).delete(pKey, lKey, option);
  }

  @Override
  public void close() {
    tableRanges.values().forEach(Table::close);
  }
}
