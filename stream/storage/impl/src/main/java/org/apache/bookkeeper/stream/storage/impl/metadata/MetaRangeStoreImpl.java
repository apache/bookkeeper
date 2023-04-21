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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stream.proto.RangeMetadata;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.RelatedRanges;
import org.apache.bookkeeper.stream.proto.storage.RelationType;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.metadata.MetaRangeStore;
import org.apache.bookkeeper.stream.storage.api.metadata.stream.MetaRange;
import org.apache.bookkeeper.stream.storage.impl.metadata.stream.MetaRangeImpl;

/**
 * The default implementation of {@link MetaRangeStore}.
 */
@Slf4j
public class MetaRangeStoreImpl
    implements MetaRangeStore {

    private final MVCCAsyncStore<byte[], byte[]> store;
    private final ScheduledExecutorService executor;
    private final StorageContainerPlacementPolicy rangePlacementPolicy;
    private final Map<Long, MetaRangeImpl> streams;
    private final StorageServerClientManager clientManager;

    public MetaRangeStoreImpl(MVCCAsyncStore<byte[], byte[]> store,
                              StorageContainerPlacementPolicy rangePlacementPolicy,
                              ScheduledExecutorService executor,
                              StorageServerClientManager clientManager) {
        this.store = store;
        this.executor = executor;
        this.rangePlacementPolicy = rangePlacementPolicy;
        this.streams = Maps.newHashMap();
        this.clientManager = clientManager;
    }

    //
    // Methods for {@link MetaRangeStore}
    //

    //
    // Stream API
    //

    private CompletableFuture<GetActiveRangesResponse> createStreamIfMissing(long streamId,
                                                                             MetaRangeImpl metaRange,
                                                                             StreamProperties streamProps) {
        if (null == streamProps) {
            return FutureUtils.value(GetActiveRangesResponse.newBuilder()
                .setCode(StatusCode.STREAM_NOT_FOUND)
                .build());
        }

        return metaRange.create(streamProps).thenCompose(created -> {
            if (created) {
                synchronized (streams) {
                    streams.put(streamId, metaRange);
                }
                return getActiveRanges(metaRange);
            } else {
                return FutureUtils.value(GetActiveRangesResponse.newBuilder()
                    .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                    .build());
            }
        });
    }

    @Override
    public CompletableFuture<GetActiveRangesResponse> getActiveRanges(GetActiveRangesRequest request) {
        final long streamId = request.getStreamId();

        MetaRangeImpl metaRange = streams.get(streamId);

        if (null == metaRange) {
            final MetaRangeImpl metaRangeImpl = new MetaRangeImpl(store, executor, rangePlacementPolicy);
            return metaRangeImpl.load(streamId)
                .thenCompose(mr -> {
                    if (null == mr) {
                        // meta range doesn't exist, talk to root range to get the stream props
                        return clientManager.getStreamProperties(streamId)
                            .thenCompose(streamProperties ->
                                createStreamIfMissing(streamId, metaRangeImpl, streamProperties));
                    } else {
                        synchronized (streams) {
                            streams.put(streamId, (MetaRangeImpl) mr);
                        }
                        return getActiveRanges(mr);
                    }
                });
        } else {
            return getActiveRanges(metaRange);
        }
    }

    private CompletableFuture<GetActiveRangesResponse> getActiveRanges(MetaRange metaRange) {
        GetActiveRangesResponse.Builder respBuilder = GetActiveRangesResponse.newBuilder();
        return metaRange.getActiveRanges()
            .thenApplyAsync(ranges -> {
                for (RangeMetadata range : ranges) {
                    RelatedRanges.Builder rrBuilder = RelatedRanges.newBuilder()
                        .setProps(range.getProps())
                        .setType(RelationType.PARENTS)
                        .addAllRelatedRanges(range.getParentsList());
                    respBuilder.addRanges(rrBuilder);
                }
                return respBuilder
                    .setCode(StatusCode.SUCCESS)
                    .build();
            }, executor)
            .exceptionally(cause -> respBuilder.setCode(StatusCode.INTERNAL_SERVER_ERROR).build());
    }

}
