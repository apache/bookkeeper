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

package org.apache.bookkeeper.metadata.etcd.helpers;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.common.exception.ClosedClientException;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchResponse;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.metadata.etcd.EtcdWatchClient;
import org.apache.bookkeeper.metadata.etcd.EtcdWatcher;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A helper class to read the stream of values of a given key.
 */
@Slf4j
public class ValueStream<T> implements BiConsumer<WatchResponse, Throwable>, AutoCloseable {

    private final Client client;
    private final boolean ownWatchClient;
    private final EtcdWatchClient watchClient;
    private final Function<ByteSequence, T> encoder;
    private final ByteSequence key;
    private final Map<Consumer<Versioned<T>>, RevisionedConsumer<T>> consumers =
        new HashMap<>();
    private volatile T localValue = null;
    private volatile long revision = -1L;
    private CompletableFuture<EtcdWatcher> watchFuture = null;
    private CompletableFuture<Void> closeFuture = null;

    public ValueStream(Client client,
                       Function<ByteSequence, T> encoder,
                       ByteSequence key) {
        this(client, new EtcdWatchClient(client), encoder, key);
    }

    public ValueStream(Client client,
                       EtcdWatchClient watchClient,
                       Function<ByteSequence, T> encoder,
                       ByteSequence key) {
        this.client = client;
        this.watchClient = watchClient;
        this.ownWatchClient = false;
        this.encoder = encoder;
        this.key = key;
    }

    public CompletableFuture<Versioned<T>> read() {
        return client.getKVClient().get(
            key
        ).thenApply(getResp -> {
            boolean updated = updateLocalValue(getResp);
            Versioned<T> localValue = getLocalValue();
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
    public int getNumConsumers() {
        synchronized (consumers) {
            return consumers.size();
        }
    }

    private void notifyConsumers(Versioned<T> localValue) {
        synchronized (consumers) {
            consumers.values().forEach(consumer -> consumer.accept(localValue));
        }
    }

    private synchronized boolean updateLocalValue(GetResponse response) {
        if (revision < response.getHeader().getRevision()) {
            revision = response.getHeader().getRevision();
            if (response.getCount() > 0) {
                localValue = encoder.apply(response.getKvs().get(0).getValue());
            } else {
                localValue = null;
            }
            return true;
        } else {
            return false;
        }
    }

    private synchronized Versioned<T> processWatchResponse(WatchResponse response) {
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
                    this.localValue = encoder.apply(event.getKeyValue().getValue());
                    break;
                case DELETE:
                    this.localValue = null;
                    break;
                default:
                    // ignore
                    break;
            }

        });
        return getLocalValue();
    }

    @VisibleForTesting
    synchronized Versioned<T> getLocalValue() {
        return new Versioned<>(
            localValue,
            new LongVersion(revision)
        );
    }

    private CompletableFuture<Versioned<T>> getOrRead() {
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

    private synchronized CompletableFuture<EtcdWatcher> getWatchFuture() {
        return this.watchFuture;
    }

    @VisibleForTesting
    public CompletableFuture<EtcdWatcher> waitUntilWatched() {
        CompletableFuture<EtcdWatcher> wf;
        while ((wf = getWatchFuture()) == null) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Interrupted at waiting until the key is watched", e);
                }
            }
        }
        return wf;
    }

    public CompletableFuture<Versioned<T>> readAndWatch(Consumer<Versioned<T>> consumer) {
        final RevisionedConsumer<T> revisionedConsumer = new RevisionedConsumer<>(consumer);
        final boolean consumerExisted;
        synchronized (consumers) {
            consumerExisted = (null != consumers.put(consumer, revisionedConsumer));
        }
        if (consumerExisted) {
            return getOrRead();
        }

        return getOrRead()
            .thenCompose(versionedVal -> {
                long revision = ((LongVersion) versionedVal.getVersion()).getLongVersion();
                synchronized (this) {
                    notifyConsumers(versionedVal);
                }
                return watchIfNeeded(revision).thenApply(ignored -> versionedVal);
            });
    }

    public CompletableFuture<Boolean> unwatch(Consumer<Versioned<T>> consumer) {
        boolean lastConsumer;
        synchronized (consumers) {
            lastConsumer = (null != consumers.remove(consumer) && consumers.isEmpty());
        }
        if (lastConsumer) {
            return closeOrRewatch(false).thenApply(ignored -> true);
        } else {
            return FutureUtils.value(false);
        }
    }

    private synchronized CompletableFuture<EtcdWatcher> watchIfNeeded(long revision) {
        if (null != watchFuture) {
            return watchFuture;
        }
        watchFuture = watch(revision);
        return watchFuture;
    }

    private CompletableFuture<EtcdWatcher> watch(long revision) {
        WatchOption.Builder optionBuilder = WatchOption.newBuilder()
            .withRevision(revision);
        return watchClient.watch(key, optionBuilder.build(), this)
            .whenComplete((watcher, cause) -> {
                if (null != cause) {
                    synchronized (ValueStream.this) {
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
            if (log.isDebugEnabled()) {
                log.debug("Received watch response : revision = {}, {} events = {}",
                    watchResponse.getHeader().getRevision(),
                    watchResponse.getEvents().size(),
                    watchResponse.getEvents());
            }

            synchronized (this) {
                Versioned<T> localValue = processWatchResponse(watchResponse);
                if (null != localValue) {
                    notifyConsumers(localValue);
                }
            }
        } else {
            // rewatch if it is not a `ClosedClientException`
            closeOrRewatch(!(throwable instanceof ClosedClientException));
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
