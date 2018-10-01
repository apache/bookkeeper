/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.routing;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.clients.impl.routing.RangeRouter;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.router.BytesHashRouter;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;

/**
 * A default implementation of {@link RangeRoutingTable}.
 */
@Slf4j
public class RangeRoutingTableImpl implements RangeRoutingTable {

    private final StorageServerClientManager manager;
    private final ConcurrentLongHashMap<RangeRouter<byte[]>> ranges;

    // outstanding requests
    private final ConcurrentLongHashMap<CompletableFuture<RangeRouter<byte[]>>> outstandingUpdates;

    public RangeRoutingTableImpl(StorageServerClientManager manager) {
        this.manager = manager;
        this.ranges = new ConcurrentLongHashMap<>();
        this.outstandingUpdates = new ConcurrentLongHashMap<>();
    }

    @VisibleForTesting
    RangeRouter<byte[]> getRangeRouter(long streamId) {
        return ranges.get(streamId);
    }

    @Override
    public RangeProperties getRange(long streamId, byte[] routingKey) {
        RangeRouter<byte[]> router = ranges.get(streamId);
        if (null == router) {
            // trigger to fetch stream metadata, but return `null` since
            // the range router is not ready, let the client backoff and retry.
            fetchStreamRanges(streamId);
            return null;
        } else {
            return router.getRangeProperties(routingKey);
        }
    }

    @VisibleForTesting
    CompletableFuture<RangeRouter<byte[]>> getOutstandingFetchRequest(long streamId) {
        return outstandingUpdates.get(streamId);
    }

    private void fetchStreamRanges(long streamId) {
        if (null != outstandingUpdates.get(streamId)) {
            // there is already an outstanding fetch request, do nothing
            return;
        }
        final CompletableFuture<RangeRouter<byte[]>> newFetchFuture = new CompletableFuture<>();
        if (null != outstandingUpdates.put(streamId, newFetchFuture)) {
            // some one already triggers the fetch
            return;
        }
        FutureUtils.proxyTo(
            manager.openMetaRangeClient(streamId)
                .thenCompose(metaRangeClient -> metaRangeClient.getActiveDataRanges())
                .thenApply(hashStreamRanges -> {
                    RangeRouter<byte[]> router = new RangeRouter<>(BytesHashRouter.of());
                    router.setRanges(hashStreamRanges);
                    return router;
                })
                .whenComplete((router, cause) -> {
                    if (null == cause) {
                        ranges.put(streamId, router);
                    }
                    outstandingUpdates.remove(streamId, newFetchFuture);
                }),
            newFetchFuture
        );
    }
}
