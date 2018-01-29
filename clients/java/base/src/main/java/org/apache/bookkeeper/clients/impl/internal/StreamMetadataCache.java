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

package org.apache.bookkeeper.clients.impl.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.StreamProperties;

/**
 * Build the stream metadata cache.
 */
public class StreamMetadataCache {

    private final RootRangeClient scClient;
    private final ConcurrentMap<Long, StreamProperties> streams;

    public StreamMetadataCache(RootRangeClient scClient) {
        this.scClient = scClient;
        this.streams = Maps.newConcurrentMap();
    }

    @VisibleForTesting
    ConcurrentMap<Long, StreamProperties> getStreams() {
        return streams;
    }

    CompletableFuture<StreamProperties> getStreamProperties(long streamId) {
        StreamProperties props = streams.get(streamId);
        if (null != props) {
            return FutureUtils.value(props);
        }

        return scClient.getStream(streamId).thenApply(propsReturned -> {
            streams.put(streamId, propsReturned);
            return propsReturned;
        });
    }

    boolean putStreamProperties(long streamId, StreamProperties props) {
        return null == streams.putIfAbsent(streamId, props);
    }

}
