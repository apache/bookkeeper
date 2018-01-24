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
package org.apache.distributedlog.clients.impl.kv;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.ByteBufHashRouter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.kv.PTableWriter;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.clients.impl.internal.api.HashStreamRanges;
import org.apache.distributedlog.clients.impl.internal.api.StorageServerClientManager;
import org.apache.distributedlog.clients.impl.routing.RangeRouter;
import org.apache.distributedlog.stream.proto.RangeProperties;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * Default implementation of {@link PTableWriter}.
 */
@Slf4j
public class PByteBufTableWriterImpl implements PTableWriter<ByteBuf, ByteBuf> {

    static final IllegalStateException CAUSE =
        new IllegalStateException("No range found for a given routing key");

    static class FailRequestTableRangeWriter implements PTableWriter<ByteBuf, ByteBuf> {

        @Override
        public CompletableFuture<Void> write(long sequenceId, ByteBuf pKey, ByteBuf lKey, ByteBuf value) {
            return FutureUtils.exception(CAUSE);
        }

        @Override
        public CompletableFuture<Void> increment(long sequenceId, ByteBuf pKey, ByteBuf lKey, long amount) {
            return FutureUtils.exception(CAUSE);
        }

        @Override
        public void close() {
            // no-op
        }

    }

    private final String streamName;
    private final StreamProperties props;
    private final StorageServerClientManager clientManager;
    private final ScheduledExecutorService executor;
    private final PTableWriter<ByteBuf, ByteBuf> failRequestWriter;
    private final Namespace namespace;

    // States
    private final RangeRouter<ByteBuf> rangeRouter;
    private final ConcurrentMap<Long, PTableWriter<ByteBuf, ByteBuf>> tableRanges;

    public PByteBufTableWriterImpl(String streamName,
                                   StreamProperties props,
                                   StorageServerClientManager clientManager,
                                   ScheduledExecutorService executor) throws IOException {
        this.streamName = streamName;
        this.props = props;
        this.clientManager = clientManager;
        this.failRequestWriter = new FailRequestTableRangeWriter();
        this.executor = executor;
        this.rangeRouter = new RangeRouter<>(ByteBufHashRouter.of());
        this.tableRanges = new ConcurrentHashMap<>();
        this.namespace = NamespaceBuilder.newBuilder()
            .uri(URI.create(props.getStreamConf().getBackendServiceUrl()))
            .conf(new DistributedLogConfiguration()
                .setWriteLockEnabled(false)
                .setImmediateFlushEnabled(false)
                .setOutputBufferSize(128 * 1024)
                .setPeriodicFlushFrequencyMilliSeconds(2)
                .setExplicitTruncationByApplication(true)
                .setLogSegmentRollingConcurrency(1)
                .setMaxLogSegmentBytes(256 * 1024 * 1024))
            .build();
    }

    private PTableWriter<ByteBuf, ByteBuf> getTableRangeWriter(Long range) {
        PTableWriter<ByteBuf, ByteBuf> tRange = tableRanges.get(range);
        // TODO: we need logic to handle scale/repartitioning
        if (null == tRange) {
            return failRequestWriter;
        }
        return tRange;
    }

    public CompletableFuture<PTableWriter<ByteBuf, ByteBuf>> initialize() {
        return this.clientManager
            .openMetaRangeClient(props)
            .getActiveDataRanges()
            .thenComposeAsync((ranges) -> refreshRangeSpaces(ranges), executor);
    }

    CompletableFuture<PTableWriter<ByteBuf, ByteBuf>> refreshRangeSpaces(HashStreamRanges newRanges) {
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
        try {
            Set<Long> activeRanges = Sets.newHashSetWithExpectedSize(newRanges.getRanges().size());
            for (Map.Entry<Long, RangeProperties> entry : newRanges.getRanges().entrySet()) {
                long rk = entry.getKey();
                RangeProperties range = entry.getValue();
                activeRanges.add(range.getRangeId());
                if (tableRanges.containsKey(range.getRangeId())) {
                    continue;
                }
                PTableWriter<ByteBuf, ByteBuf> rangeWriter =
                    new PByteBufTableRangeWriterImpl(props, range, namespace);
                if (log.isInfoEnabled()) {
                    log.info("Create table range client for range {}", range.getRangeId());
                }
                this.tableRanges.put(range.getRangeId(), rangeWriter);
            }
            // remove old ranges
            Iterator<Entry<Long, PTableWriter<ByteBuf, ByteBuf>>> rsIter = tableRanges.entrySet().iterator();
            while (rsIter.hasNext()) {
                Map.Entry<Long, PTableWriter<ByteBuf, ByteBuf>> entry = rsIter.next();
                Long rid = entry.getKey();
                if (activeRanges.contains(rid)) {
                    continue;
                }
                rsIter.remove();
                PTableWriter<ByteBuf, ByteBuf> oldRangeSpace = entry.getValue();
                oldRangeSpace.close();
            }
            return FutureUtils.value(this);
        } catch (IOException ioe) {
            return FutureUtils.exception(ioe);
        }
    }

    @Override
    public CompletableFuture<Void> write(long sequenceId, ByteBuf pKey, ByteBuf lKey, ByteBuf value) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRangeWriter(range)
            .write(sequenceId, pKey, lKey, value);
    }

    @Override
    public CompletableFuture<Void> increment(long sequenceId, ByteBuf pKey, ByteBuf lKey, long amount) {
        Long range = rangeRouter.getRange(pKey);
        return getTableRangeWriter(range)
            .increment(sequenceId, pKey, lKey, amount);
    }

    @Override
    public void close() {
        tableRanges.values().forEach(PTableWriter::close);
        namespace.close();
    }
}
