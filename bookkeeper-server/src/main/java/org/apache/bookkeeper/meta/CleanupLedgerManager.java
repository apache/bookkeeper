/**
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
package org.apache.bookkeeper.meta;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;

/**
 * A ledger manager that cleans up resources upon closing.
 */
public class CleanupLedgerManager implements LedgerManager {

    private class CleanupGenericCallback<T> implements GenericCallback<T> {

        private final GenericCallback<T> cb;

        CleanupGenericCallback(GenericCallback<T> cb) {
            this.cb = cb;
            addCallback(cb);
        }

        @Override
        public void operationComplete(int rc, T result) {
            closeLock.readLock().lock();
            try {
                if (!closed && null != removeCallback(cb)) {
                    cb.operationComplete(rc, result);
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
    }

    private static class ClosedLedgerRangeIterator implements LedgerRangeIterator {

        @Override
        public boolean hasNext() throws IOException {
            throw new IOException("Ledger manager is closed.");
        }

        @Override
        public LedgerRange next() throws IOException {
            throw new IOException("Ledger manager is closed.");
        }
    }

    private final LedgerManager underlying;
    private final ConcurrentMap<GenericCallback, GenericCallback> callbacks =
        new ConcurrentHashMap<GenericCallback, GenericCallback>();
    private boolean closed = false;
    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final Set<CompletableFuture<?>> futures = ConcurrentHashMap.newKeySet();

    public CleanupLedgerManager(LedgerManager lm) {
        this.underlying = lm;
    }

    @VisibleForTesting
    public LedgerManager getUnderlying() {
        return underlying;
    }

    private void addCallback(GenericCallback callback) {
        callbacks.put(callback, callback);
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        underlying.registerLedgerMetadataListener(ledgerId, listener);
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        underlying.unregisterLedgerMetadataListener(ledgerId, listener);
    }

    private GenericCallback removeCallback(GenericCallback callback) {
        return callbacks.remove(callback);
    }

    private void recordPromise(CompletableFuture<?> promise) {
        futures.add(promise);
        promise.thenRun(() -> futures.remove(promise));
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long lid, LedgerMetadata metadata) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return closedPromise();
            } else {
                CompletableFuture<Versioned<LedgerMetadata>> promise = underlying.createLedgerMetadata(lid, metadata);
                recordPromise(promise);
                return promise;
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return closedPromise();
            }
            CompletableFuture<Void> promise = underlying.removeLedgerMetadata(ledgerId, version);
            recordPromise(promise);
            return promise;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return closedPromise();
            }
            CompletableFuture<Versioned<LedgerMetadata>> promise = underlying.readLedgerMetadata(ledgerId);
            recordPromise(promise);
            return promise;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                            Version currentVersion) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return closedPromise();
            }
            CompletableFuture<Versioned<LedgerMetadata>> promise =
                underlying.writeLedgerMetadata(ledgerId, metadata, currentVersion);
            recordPromise(promise);
            return promise;
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor,
                                    final AsyncCallback.VoidCallback finalCb, final Object context,
                                    final int successRc, final int failureRc) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                finalCb.processResult(failureRc, null, context);
                return;
            }
            final GenericCallback<Void> stub = new GenericCallback<Void>() {
                @Override
                public void operationComplete(int rc, Void result) {
                    finalCb.processResult(failureRc, null, context);
                }
            };
            addCallback(stub);
            underlying.asyncProcessLedgers(processor, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (null != removeCallback(stub)) {
                        finalCb.processResult(rc, path, ctx);
                    }
                }
            }, context, successRc, failureRc);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return new ClosedLedgerRangeIterator();
            }
            return underlying.getLedgerRanges(zkOpTimeoutMs);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void close() throws IOException {
        Set<GenericCallback> keys;
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            keys = new HashSet<GenericCallback>(callbacks.keySet());
        } finally {
            closeLock.writeLock().unlock();
        }
        for (GenericCallback key : keys) {
            GenericCallback callback = callbacks.remove(key);
            if (null != callback) {
                callback.operationComplete(BKException.Code.ClientClosedException, null);
            }
        }
        BKException exception = new BKException.BKClientClosedException();
        futures.forEach((f) -> f.completeExceptionally(exception));
        futures.clear();
        underlying.close();
    }

    private static <T> CompletableFuture<T> closedPromise() {
        return FutureUtils.exception(new BKException.BKClientClosedException());
    }
}
