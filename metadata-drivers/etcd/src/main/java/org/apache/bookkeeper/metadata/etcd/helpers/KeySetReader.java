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

import com.coreos.jetcd.Client;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.metadata.etcd.EtcdWatchClient;
import org.apache.bookkeeper.metadata.etcd.EtcdWatcher;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A helper class to read a set of keys and watch them.
 */
@Slf4j
public class KeySetReader<T> implements BiConsumer<WatchResponse, Throwable>, AutoCloseable {

    private final Client client;
    private final boolean ownWatchClient;
    private final EtcdWatchClient watchClient;
    private final Function<ByteSequence, T> encoder;
    private final ByteSequence beginKey;
    private final ByteSequence endKey;
    private final Set<ByteSequence> keys;
    private final CopyOnWriteArraySet<Consumer<Versioned<Set<T>>>> consumers =
        new CopyOnWriteArraySet<>();
    private volatile long revision = -1L;
    private CompletableFuture<EtcdWatcher> watchFuture = null;
    private CompletableFuture<Void> closeFuture = null;

    public KeySetReader(Client client,
                        Function<ByteSequence, T> encoder,
                        ByteSequence beginKey,
                        ByteSequence endKey) {
        this(client, new EtcdWatchClient(client), encoder, beginKey, endKey);
    }

    public KeySetReader(Client client,
                        EtcdWatchClient watchClient,
                        Function<ByteSequence, T> encoder,
                        ByteSequence beginKey,
                        ByteSequence endKey) {
        this.client = client;
        this.watchClient = watchClient;
        this.ownWatchClient = false;
        this.encoder = encoder;
        this.beginKey = beginKey;
        this.endKey = endKey;
        this.keys = Collections.synchronizedSet(Sets.newHashSet());
    }

    public CompletableFuture<Versioned<Set<T>>> read() {
        GetOption.Builder optionBuilder = GetOption.newBuilder()
            .withKeysOnly(true);
        if (null != endKey) {
            optionBuilder.withRange(endKey);
        }
        return client.getKVClient().get(
            beginKey,
            optionBuilder.build()
        ).thenApply(getResp -> {
            boolean updated = updateLocalValue(getResp);
            Versioned<Set<T>> localValue = getLocalValue();
            try {
                return localValue;
            } finally {
                if (updated) {
                    notifyConsumers(localValue);
                }
            }
        });
    }

    @VisibleForTesting
    long getRevision() {
        return revision;
    }

    private void notifyConsumers(Versioned<Set<T>> localValue) {
        consumers.forEach(consumer -> consumer.accept(localValue));
    }

    private synchronized boolean updateLocalValue(GetResponse response) {
        if (revision < response.getHeader().getRevision()) {
            revision = response.getHeader().getRevision();
            keys.clear();
            for (KeyValue kv : response.getKvs()) {
                ByteSequence key = kv.getKey();
                keys.add(key);
            }
            return true;
        } else {
            return false;
        }
    }

    private synchronized Versioned<Set<T>> processWatchResponse(WatchResponse response) {
        if (null != closeFuture) {
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug("Received watch response : revision = {}, {} events = {}",
                response.getHeader().getRevision(), response.getEvents().size(), response.getEvents());
        }

        if (response.getHeader().getRevision() <= revision) {
            return null;
        }
        revision = response.getHeader().getRevision();
        response.getEvents().forEach(event -> {
            switch (event.getEventType()) {
                case PUT:
                    keys.add(event.getKeyValue().getKey());
                    break;
                case DELETE:
                    keys.remove(event.getKeyValue().getKey());
                    break;
                default:
                    // ignore
                    break;
            }

        });
        return getLocalValue();
    }

    @VisibleForTesting
    synchronized Versioned<Set<T>> getLocalValue() {
        return new Versioned<>(
            keys.stream().map(encoder).collect(Collectors.toSet()),
            new LongVersion(revision)
        );
    }

    private CompletableFuture<Versioned<Set<T>>> getOrRead() {
        boolean shouldRead = false;
        synchronized (this) {
            if (revision < 0L) {
                // the value is never cached.
                shouldRead = true;
            }
        }
        if (shouldRead) {
            return read();
        } else {
            return FutureUtils.value(getLocalValue());
        }
    }

    @VisibleForTesting
    synchronized boolean isWatcherSet() {
        return null != watchFuture;
    }

    public CompletableFuture<Versioned<Set<T>>> readAndWatch(Consumer<Versioned<Set<T>>> consumer) {
        if (!consumers.add(consumer) || isWatcherSet()) {
            return getOrRead();
        }

        return getOrRead()
            .thenCompose(versionedKeys -> {
                long revision = ((LongVersion) versionedKeys.getVersion()).getLongVersion();
                return watch(revision).thenApply(ignored -> versionedKeys);
            });
    }

    public CompletableFuture<Void> unwatch(Consumer<Versioned<Set<T>>> consumer) {
        if (consumers.remove(consumer) && consumers.isEmpty()) {
            return closeOrRewatch(false);
        } else {
            return FutureUtils.Void();
        }
    }

    private synchronized CompletableFuture<EtcdWatcher> watch(long revision) {
        if (null != watchFuture) {
            return watchFuture;
        }

        WatchOption.Builder optionBuilder = WatchOption.newBuilder()
            .withRevision(revision);
        if (null != endKey) {
            optionBuilder.withRange(endKey);
        }
        watchFuture = watchClient.watch(beginKey, optionBuilder.build(), this);
        return watchFuture.whenComplete((watcher, cause) -> {
            if (null != cause) {
                synchronized (KeySetReader.this) {
                    watchFuture = null;
                }
            }
        });
    }

    private CompletableFuture<Void> closeOrRewatch(boolean rewatch) {
        CompletableFuture<EtcdWatcher> oldWatcherFuture;
        synchronized (this) {
            oldWatcherFuture = watchFuture;
            if (rewatch && null == closeFuture) {
                watchFuture = watch(revision);
            } else {
                watchFuture = null;
            }
        }
        if (null != oldWatcherFuture) {
            return oldWatcherFuture.thenCompose(EtcdWatcher::closeAsync);
        } else {
            return FutureUtils.Void();
        }
    }

    @Override
    public void accept(WatchResponse watchResponse, Throwable throwable) {
        if (null == throwable) {
            Versioned<Set<T>> localValue = processWatchResponse(watchResponse);
            if (null != localValue) {
                notifyConsumers(localValue);
            }
        } else {
            closeOrRewatch(true);
        }
    }

    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> future;
        synchronized (this) {
            if (null == closeFuture) {
                closeFuture = closeOrRewatch(false).thenCompose(ignored -> {
                    if (ownWatchClient) {
                        return watchClient.closeAsync();
                    } else {
                        return FutureUtils.Void();
                    }
                });
            }
            future = closeFuture;
        }
        return future;
    }

    @Override
    public void close() {
        try {
            FutureUtils.result(closeAsync());
        } catch (Exception e) {
            log.warn("Encountered exceptions on closing key reader : {}", e.getMessage());
        }
    }
}
