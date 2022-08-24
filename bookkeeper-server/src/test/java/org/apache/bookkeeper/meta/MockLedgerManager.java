/**
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
 *
 */
package org.apache.bookkeeper.meta;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock implementation of Ledger Manager.
 */
public class MockLedgerManager implements LedgerManager {
    static final Logger LOG = LoggerFactory.getLogger(MockLedgerManager.class);

    /**
     * Hook for injecting errors or delays.
     */
    public interface Hook {
        CompletableFuture<Void> runHook(long ledgerId, LedgerMetadata metadata);
    }

    final Map<Long, Pair<LongVersion, byte[]>> metadataMap;
    final ExecutorService executor;
    final boolean ownsExecutor;
    final LedgerMetadataSerDe serDe;
    private Hook preWriteHook = (ledgerId, metadata) -> FutureUtils.value(null);

    public MockLedgerManager() {
        this(new ConcurrentHashMap<>(),
             Executors.newSingleThreadExecutor((r) -> new Thread(r, "MockLedgerManager")), true);
    }

    private MockLedgerManager(Map<Long, Pair<LongVersion, byte[]>> metadataMap,
                              ExecutorService executor, boolean ownsExecutor) {
        this.metadataMap = metadataMap;
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
        this.serDe = new LedgerMetadataSerDe();
    }

    public MockLedgerManager newClient() {
        return new MockLedgerManager(metadataMap, executor, false);
    }

    private Versioned<LedgerMetadata> readMetadata(long ledgerId) throws Exception {
        Pair<LongVersion, byte[]> pair = metadataMap.get(ledgerId);
        if (pair == null) {
            return null;
        } else {
            return new Versioned<>(serDe.parseConfig(pair.getRight(), ledgerId, Optional.empty()), pair.getLeft());
        }
    }

    public void setPreWriteHook(Hook hook) {
        this.preWriteHook = hook;
    }

    public void executeCallback(Runnable r) {
        r.run();
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId, LedgerMetadata metadata) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        executor.submit(() -> {
                if (metadataMap.containsKey(ledgerId)) {
                    executeCallback(() -> promise.completeExceptionally(new BKException.BKLedgerExistException()));
                } else {
                    try {
                        metadataMap.put(ledgerId, Pair.of(new LongVersion(0L), serDe.serialize(metadata)));
                        Versioned<LedgerMetadata> readBack = readMetadata(ledgerId);
                        executeCallback(() -> promise.complete(readBack));
                    } catch (Exception e) {
                        LOG.error("Error reading back written metadata", e);
                        executeCallback(() -> promise.completeExceptionally(new BKException.MetaStoreException()));
                    }
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        executor.submit(() -> {
                try {
                    Versioned<LedgerMetadata> metadata = readMetadata(ledgerId);
                    if (metadata == null) {
                        executeCallback(() -> promise.completeExceptionally(
                                                new BKException.BKNoSuchLedgerExistsOnMetadataServerException()));
                    } else {
                        executeCallback(() -> promise.complete(metadata));
                    }
                } catch (Exception e) {
                    LOG.error("Error reading metadata", e);
                    executeCallback(() -> promise.completeExceptionally(new BKException.MetaStoreException()));
                }
            });
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                            Version currentVersion) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        preWriteHook.runHook(ledgerId, metadata)
            .thenComposeAsync((ignore) -> {
                    try {
                        Versioned<LedgerMetadata> oldMetadata = readMetadata(ledgerId);
                        if (oldMetadata == null) {
                            return FutureUtils.exception(
                                    new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                        } else if (!oldMetadata.getVersion().equals(currentVersion)) {
                            return FutureUtils.exception(new BKException.BKMetadataVersionException());
                        } else {
                            LongVersion oldVersion = (LongVersion) oldMetadata.getVersion();
                            metadataMap.put(ledgerId, Pair.of(new LongVersion(oldVersion.getLongVersion() + 1),
                                                              serDe.serialize(metadata)));
                            Versioned<LedgerMetadata> readBack = readMetadata(ledgerId);
                            return FutureUtils.value(readBack);
                        }
                    } catch (Exception e) {
                        LOG.error("Error writing metadata", e);
                        return FutureUtils.exception(e);
                    }
                }, executor)
            .whenComplete((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
                        executeCallback(() -> promise.completeExceptionally(cause));
                    } else {
                        executeCallback(() -> promise.complete(res));
                    }
                });
        return promise;
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {}

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {}

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, AsyncCallback.VoidCallback finalCb,
                                    Object context, int successRc, int failureRc) {
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
        List<Long> ledgerIds = new ArrayList<>(metadataMap.keySet());
        ledgerIds.sort(Comparator.naturalOrder());
        List<List<Long>> partitions = Lists.partition(ledgerIds, 100);
        return new LedgerRangeIterator() {
            int i = 0;
            @Override
            public boolean hasNext() {
                if (i >= partitions.size()) {
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public LedgerRange next() {
                return new LedgerRange(new HashSet<>(partitions.get(i++)));
            }
        };
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdownNow();
        }
    }

}
