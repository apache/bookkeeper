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

package org.apache.bookkeeper.metadata.etcd.helpers;

import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortOrder;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.google.common.primitives.UnsignedBytes;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Read a range of key/value pairs in a streaming way.
 */
@Slf4j
public class KeyStream<T> {

    private final KV kvClient;
    private final ByteSequence startKey;
    private final ByteSequence endKey;
    private final Function<ByteSequence, T> encoder;
    private final int batchSize;
    private ByteSequence nextKey;
    private ByteSequence lastKey = null;
    private boolean hasMore = true;

    public KeyStream(KV kvClient,
                     ByteSequence startKey,
                     ByteSequence endKey,
                     Function<ByteSequence, T> encoder) {
        this(kvClient, startKey, endKey, encoder, 100);
    }

    public KeyStream(KV kvClient,
                     ByteSequence startKey,
                     ByteSequence endKey,
                     Function<ByteSequence, T> encoder,
                     int batchSize) {
        this.kvClient = kvClient;
        this.startKey = startKey;
        this.endKey = endKey;
        this.nextKey = startKey;
        this.encoder = encoder;
        this.batchSize = batchSize;
    }

    public CompletableFuture<List<T>> readNext() {
        ByteSequence beginKey;
        int batchSize = this.batchSize;
        synchronized (this) {
            if (!hasMore) {
                return FutureUtils.value(Collections.emptyList());
            }

            beginKey = nextKey;
            if (null != lastKey) {
                // read one more in since we are including last key.
                batchSize += 1;
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("Read keys between {} and {}", beginKey.toStringUtf8(), endKey.toStringUtf8());
        }
        return kvClient.get(
            beginKey,
            GetOption.newBuilder()
                .withRange(endKey)
                .withKeysOnly(true)
                .withLimit(batchSize)
                .withSortField(SortTarget.KEY)
                .withSortOrder(SortOrder.ASCEND)
                .build()
        ).thenApply(getResp -> {
            List<KeyValue> kvs = getResp.getKvs();
            ByteSequence lkey;
            synchronized (KeyStream.this) {
                hasMore = getResp.isMore();
                lkey = lastKey;
                if (kvs.size() > 0) {
                    lastKey = nextKey = kvs.get(kvs.size() - 1).getKey();
                }
            }
            if (null != lkey
                && kvs.size() > 0
                && UnsignedBytes.lexicographicalComparator().compare(
                    lkey.getBytes(),
                    kvs.get(0).getKey().getBytes()) == 0) {
                kvs.remove(0);
            }
            return kvs.stream()
                .map(kv -> encoder.apply(kv.getKey()))
                .collect(Collectors.toList());
        });
    }

}
