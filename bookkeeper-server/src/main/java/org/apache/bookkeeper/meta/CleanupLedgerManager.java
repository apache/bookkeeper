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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
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

    @Override
    public void createLedgerMetadata(long lid, LedgerMetadata metadata,
                                     GenericCallback<Versioned<LedgerMetadata>> cb) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.operationComplete(BKException.Code.ClientClosedException, null);
                return;
            }
            underlying.createLedgerMetadata(lid, metadata, new CleanupGenericCallback<>(cb));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void removeLedgerMetadata(long ledgerId, Version version,
                                     GenericCallback<Void> vb) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                vb.operationComplete(BKException.Code.ClientClosedException, null);
                return;
            }
            underlying.removeLedgerMetadata(ledgerId, version,
                    new CleanupGenericCallback<Void>(vb));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void readLedgerMetadata(long ledgerId,
                                   GenericCallback<Versioned<LedgerMetadata>> readCb) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                readCb.operationComplete(BKException.Code.ClientClosedException, null);
                return;
            }
            underlying.readLedgerMetadata(ledgerId, new CleanupGenericCallback<>(readCb));
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                    Version currentVersion,
                                    GenericCallback<Versioned<LedgerMetadata>> cb) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.operationComplete(BKException.Code.ClientClosedException, null);
                return;
            }
            underlying.writeLedgerMetadata(ledgerId, metadata, currentVersion,
                                           new CleanupGenericCallback<>(cb));
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
    public LedgerRangeIterator getLedgerRanges() {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return new ClosedLedgerRangeIterator();
            }
            return underlying.getLedgerRanges();
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
        underlying.close();
    }
}
