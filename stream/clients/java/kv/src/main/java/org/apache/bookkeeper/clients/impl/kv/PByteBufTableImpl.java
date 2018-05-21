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

package org.apache.bookkeeper.clients.impl.kv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Txn;
import org.apache.bookkeeper.api.kv.impl.op.OpFactoryImpl;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueFactory;
import org.apache.bookkeeper.api.kv.impl.result.ResultFactory;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.routing.RangeRouter;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.ByteBufHashRouter;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * The default implemenation of {@link PTable}.
 */
@Slf4j
public class PByteBufTableImpl implements PTable<ByteBuf, ByteBuf> {

    static final IllegalStateException CAUSE =
        new IllegalStateException("No range found for a given routing key");

    private static class FailRequestTxn implements Txn<ByteBuf, ByteBuf> {

        @Override
        public Txn<ByteBuf, ByteBuf> If(CompareOp... cmps) {
            return this;
        }

        @Override
        public Txn<ByteBuf, ByteBuf> Then(Op... ops) {
            return this;
        }

        @Override
        public Txn<ByteBuf, ByteBuf> Else(Op... ops) {
            return this;
        }

        @Override
        public CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commit() {
            return FutureUtils.exception(CAUSE);
        }
    }

    static class FailRequestKeyValueSpace implements PTable<ByteBuf, ByteBuf> {

        private final OpFactory<ByteBuf, ByteBuf> opFactory;
        private final FailRequestTxn txn;

        private FailRequestKeyValueSpace(OpFactory<ByteBuf, ByteBuf> opFactory) {
            this.opFactory = opFactory;
            this.txn = new FailRequestTxn();
        }

        @Override
        public CompletableFuture<RangeResult<ByteBuf, ByteBuf>> get(ByteBuf pKey,
                                                                    ByteBuf lKey,
                                                                    RangeOption<ByteBuf> option) {
            return FutureUtils.exception(CAUSE);
        }

        @Override
        public CompletableFuture<PutResult<ByteBuf, ByteBuf>> put(ByteBuf pKey,
                                                                  ByteBuf lKey,
                                                                  ByteBuf value,
                                                                  PutOption option) {
            return FutureUtils.exception(CAUSE);
        }

        @Override
        public CompletableFuture<DeleteResult<ByteBuf, ByteBuf>> delete(ByteBuf pKey,
                                                                        ByteBuf lKey,
                                                                        DeleteOption<ByteBuf> option) {
            return FutureUtils.exception(CAUSE);
        }

        @Override
        public CompletableFuture<IncrementResult<ByteBuf, ByteBuf>> increment(ByteBuf pKey,
                                                                              ByteBuf lKey,
                                                                              long amount,
                                                                              IncrementOption<ByteBuf> option) {
            return FutureUtils.exception(CAUSE);
        }

        @Override
        public Txn<ByteBuf, ByteBuf> txn(ByteBuf pKey) {
            return txn;
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public OpFactory<ByteBuf, ByteBuf> opFactory() {
            return opFactory;
        }
    }

    private final OpFactory<ByteBuf, ByteBuf> opFactory;
    private final ResultFactory<ByteBuf, ByteBuf> resultFactory;
    private final KeyValueFactory<ByteBuf, ByteBuf> kvFactory;
    private final String streamName;
    private final StreamProperties props;
    private final StorageServerClientManager clientManager;
    private final ScheduledExecutorService executor;
    private final TableRangeFactory<ByteBuf, ByteBuf> trFactory;
    private final PTable<ByteBuf, ByteBuf> failRequestTable;

    // States
    private final RangeRouter<ByteBuf> rangeRouter;
    private final ConcurrentMap<Long, PTable<ByteBuf, ByteBuf>> tableRanges;


    public PByteBufTableImpl(String streamName,
                             StreamProperties props,
                             StorageServerClientManager clientManager,
                             ScheduledExecutorService executor,
                             Backoff.Policy backoffPolicy) {
        this(
            streamName,
            props,
            clientManager,
            executor,
            (streamProps, rangeProps, executorService, opFactory, resultFactory, kvFactory)
                -> new PByteBufTableRangeImpl(
                    streamProps.getStreamId(),
                    rangeProps,
                    clientManager.getStorageContainerChannel(rangeProps.getStorageContainerId()),
                    executorService,
                    opFactory,
                    resultFactory,
                    kvFactory,
                    backoffPolicy),
            Optional.empty());
    }

    public PByteBufTableImpl(String streamName,
                             StreamProperties props,
                             StorageServerClientManager clientManager,
                             ScheduledExecutorService executor,
                             TableRangeFactory<ByteBuf, ByteBuf> factory,
                             Optional<RangeRouter<ByteBuf>> rangeRouterOverride) {
        this.streamName = streamName;
        this.props = props;
        this.clientManager = clientManager;
        this.executor = executor;
        this.trFactory = factory;
        this.rangeRouter =
            rangeRouterOverride.orElse(new RangeRouter<>(ByteBufHashRouter.of()));
        this.tableRanges = new ConcurrentHashMap<>();
        this.opFactory = new OpFactoryImpl<>();
        this.resultFactory = new ResultFactory<>();
        this.kvFactory = new KeyValueFactory<>();
        this.failRequestTable = new FailRequestKeyValueSpace(opFactory);
    }

    @Override
    public OpFactory<ByteBuf, ByteBuf> opFactory() {
        return opFactory;
    }

    @VisibleForTesting
    ConcurrentMap<Long, PTable<ByteBuf, ByteBuf>> getTableRanges() {
        return tableRanges;
    }

    private PTable<ByteBuf, ByteBuf> getTableRange(Long range) {
        PTable<ByteBuf, ByteBuf> tRange = tableRanges.get(range);
        // TODO: we need logic to handle scale/repartitioning
        if (null == tRange) {
            return failRequestTable;
        }
        return tRange;
    }

    public CompletableFuture<PTable<ByteBuf, ByteBuf>> initialize() {
        return this.clientManager
            .openMetaRangeClient(props)
            .getActiveDataRanges()
            .thenComposeAsync((ranges) -> refreshRangeSpaces(ranges), executor);
    }

    CompletableFuture<PTable<ByteBuf, ByteBuf>> refreshRangeSpaces(HashStreamRanges newRanges) {
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
            PTable<ByteBuf, ByteBuf> tableRange =
                trFactory.openTableRange(props, range, executor, opFactory, resultFactory, kvFactory);
            if (log.isInfoEnabled()) {
                log.info("Create table range client for range {}", range.getRangeId());
            }
            this.tableRanges.put(range.getRangeId(), tableRange);
        });
        // remove old ranges
        Iterator<Entry<Long, PTable<ByteBuf, ByteBuf>>> rsIter = tableRanges.entrySet().iterator();
        while (rsIter.hasNext()) {
            Map.Entry<Long, PTable<ByteBuf, ByteBuf>> entry = rsIter.next();
            Long rid = entry.getKey();
            if (activeRanges.contains(rid)) {
                continue;
            }
            rsIter.remove();
            PTable oldRangeSpace = entry.getValue();
            oldRangeSpace.close();
        }
        return FutureUtils.value(this);
    }

    @Override
    public CompletableFuture<RangeResult<ByteBuf, ByteBuf>> get(ByteBuf pKey,
                                                                ByteBuf lKey,
                                                                RangeOption<ByteBuf> option) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRange(range).get(pKey, lKey, option);
    }

    @Override
    public CompletableFuture<PutResult<ByteBuf, ByteBuf>> put(ByteBuf pKey,
                                                              ByteBuf lKey,
                                                              ByteBuf value,
                                                              PutOption<ByteBuf> option) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRange(range).put(pKey, lKey, value, option);
    }

    @Override
    public CompletableFuture<DeleteResult<ByteBuf, ByteBuf>> delete(ByteBuf pKey,
                                                                    ByteBuf lKey,
                                                                    DeleteOption<ByteBuf> option) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRange(range).delete(pKey, lKey, option);
    }

    @Override
    public CompletableFuture<IncrementResult<ByteBuf, ByteBuf>> increment(ByteBuf pKey,
                                                                          ByteBuf lKey,
                                                                          long amount,
                                                                          IncrementOption<ByteBuf> option) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRange(range).increment(pKey, lKey, amount, option);
    }

    @Override
    public Txn<ByteBuf, ByteBuf> txn(ByteBuf pKey) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRange(range).txn(pKey);
    }

    @Override
    public void close() {
        tableRanges.values().forEach(PTable::close);
    }
}
