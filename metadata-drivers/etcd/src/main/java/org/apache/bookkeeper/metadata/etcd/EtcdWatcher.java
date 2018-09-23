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

package org.apache.bookkeeper.metadata.etcd;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchResponse;
import com.coreos.jetcd.watch.WatchResponseWithError;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Watcher class holds watcher information.
 */
@Slf4j
public class EtcdWatcher implements AutoCloseable {

    private final ScheduledExecutorService executor;
    @Getter
    private final WatchOption watchOption;
    @Getter
    private final ByteSequence key;
    // watch listener
    private final CopyOnWriteArraySet<BiConsumer<WatchResponse, Throwable>> consumers;
    @Getter
    @Setter
    private long watchID;
    // the revision to watch on.
    @Getter
    @Setter
    private long revision;
    private boolean closed = false;
    // the client owns this watcher
    private final EtcdWatchClient owner;

    EtcdWatcher(ByteSequence key,
                WatchOption watchOption,
                ScheduledExecutorService executor,
                EtcdWatchClient owner) {
        this.key = key;
        this.watchOption = watchOption;
        this.executor = executor;
        this.owner = owner;
        this.consumers = new CopyOnWriteArraySet<>();
    }

    public void addConsumer(BiConsumer<WatchResponse, Throwable> consumer) {
        this.consumers.add(consumer);
    }

    synchronized boolean isClosed() {
        return closed;
    }

    void notifyWatchResponse(WatchResponseWithError watchResponse) {
        synchronized (this) {
            if (closed) {
                return;
            }
        }

        this.executor.submit(() -> consumers.forEach(c -> {
            if (watchResponse.getException() != null) {
                c.accept(null, watchResponse.getException());
            } else {
                c.accept(
                    new WatchResponse(watchResponse.getWatchResponse()),
                    null);
            }
        }));
    }

    public CompletableFuture<Void> closeAsync() {
        return owner.unwatch(this);
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        try {
            FutureUtils.result(closeAsync());
        } catch (Exception e) {
            log.warn("Encountered error on removing watcher '{}' from watch client : {}",
                watchID, e.getMessage());
        }
        consumers.clear();
    }
}
