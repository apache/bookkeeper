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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
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
    private Hook preWriteHook = (ledgerId, metadata) -> FutureUtils.value(null);

    public MockLedgerManager() {
        this(new HashMap<>(),
             Executors.newSingleThreadExecutor((r) -> new Thread(r, "MockLedgerManager")), true);
    }

    private MockLedgerManager(Map<Long, Pair<LongVersion, byte[]>> metadataMap,
                              ExecutorService executor, boolean ownsExecutor) {
        this.metadataMap = metadataMap;
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
    }

    public MockLedgerManager newClient() {
        return new MockLedgerManager(metadataMap, executor, false);
    }

    private Versioned<LedgerMetadata> readMetadata(long ledgerId) throws Exception {
        Pair<LongVersion, byte[]> pair = metadataMap.get(ledgerId);
        if (pair == null) {
            return null;
        } else {
            return new Versioned<>(LedgerMetadata.parseConfig(pair.getRight(), Optional.empty()), pair.getLeft());
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
                    metadataMap.put(ledgerId, Pair.of(new LongVersion(0L), metadata.serialize()));
                    try {
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
                                                new BKException.BKNoSuchLedgerExistsException()));
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
                            return FutureUtils.exception(new BKException.BKNoSuchLedgerExistsException());
                        } else if (!oldMetadata.getVersion().equals(currentVersion)) {
                            return FutureUtils.exception(new BKException.BKMetadataVersionException());
                        } else {
                            LongVersion oldVersion = (LongVersion) oldMetadata.getVersion();
                            metadataMap.put(ledgerId, Pair.of(new LongVersion(oldVersion.getLongVersion() + 1),
                                                              metadata.serialize()));
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
    public LedgerRangeIterator getLedgerRanges() {
        return null;
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdownNow();
        }
    }

}
