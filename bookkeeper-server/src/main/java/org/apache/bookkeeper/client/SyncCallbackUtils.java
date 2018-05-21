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
package org.apache.bookkeeper.client;

import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.impl.LastConfirmedAndEntryImpl;

/**
 * Utility for callbacks.
 *
 */
@Slf4j
class SyncCallbackUtils {

    /**
     * Wait for a result. This is convenience method to implement callbacks
     *
     * @param <T>
     * @param future
     * @return
     * @throws InterruptedException
     * @throws BKException
     */
    public static <T> T waitForResult(CompletableFuture<T> future) throws InterruptedException, BKException {
        try {
            try {
                /*
                 * CompletableFuture.get() in JDK8 spins before blocking and wastes CPU time.
                 * CompletableFuture.get(long, TimeUnit) blocks immediately (if the result is
                 * not yet available). While the implementation of get() has changed in JDK9
                 * (not spinning any more), using CompletableFuture.get(long, TimeUnit) allows
                 * us to avoid spinning for all current JDK versions.
                 */
                return future.get(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (TimeoutException eignore) {
                // it's ok to return null if we timeout after 292 years (2^63 nanos)
                return null;
            }
        } catch (ExecutionException err) {
            if (err.getCause() instanceof BKException) {
                throw (BKException) err.getCause();
            } else {
                BKException unexpectedConditionException =
                    BKException.create(BKException.Code.UnexpectedConditionException);
                unexpectedConditionException.initCause(err.getCause());
                throw unexpectedConditionException;
            }

        }
    }

    /**
     * Handle the Response Code and transform it to a BKException.
     *
     * @param <T>
     * @param rc
     * @param result
     * @param future
     */
    public static <T> void finish(int rc, T result, CompletableFuture<? super T> future) {
        if (rc != BKException.Code.OK) {
            future.completeExceptionally(BKException.create(rc).fillInStackTrace());
        } else {
            future.complete(result);
        }
    }

    static class SyncCreateCallback implements AsyncCallback.CreateCallback {

        private final CompletableFuture<? super LedgerHandle> future;

        public SyncCreateCallback(CompletableFuture<? super LedgerHandle> future) {
            this.future = future;
        }

        /**
         * Create callback implementation for synchronous create call.
         *
         * @param rc return code
         * @param lh ledger handle object
         * @param ctx optional control object
         */
        @Override
        public void createComplete(int rc, LedgerHandle lh, Object ctx) {
            finish(rc, lh, future);
        }

    }

    static class SyncCreateAdvCallback implements AsyncCallback.CreateCallback {

        private final CompletableFuture<? super LedgerHandleAdv> future;

        public SyncCreateAdvCallback(CompletableFuture<? super LedgerHandleAdv> future) {
            this.future = future;
        }

        /**
         * Create callback implementation for synchronous create call.
         *
         * @param rc return code
         * @param lh ledger handle object
         * @param ctx optional control object
         */
        @Override
        public void createComplete(int rc, LedgerHandle lh, Object ctx) {
            if (lh == null || lh instanceof LedgerHandleAdv) {
                finish(rc, (LedgerHandleAdv) lh, future);
            } else {
                finish(BKException.Code.UnexpectedConditionException, null, future);
            }
        }

    }

    static class SyncOpenCallback implements AsyncCallback.OpenCallback {

        private final CompletableFuture<? super LedgerHandle> future;

        public SyncOpenCallback(CompletableFuture<? super LedgerHandle> future) {
            this.future = future;
        }

        /**
         * Callback method for synchronous open operation.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param ctx
         *          optional control object
         */
        @Override
        public void openComplete(int rc, LedgerHandle lh, Object ctx) {
            finish(rc, lh, future);
        }
    }

    static class SyncDeleteCallback implements AsyncCallback.DeleteCallback {

        private final CompletableFuture<Void> future;

        public SyncDeleteCallback(CompletableFuture<Void> future) {
            this.future = future;
        }


        /**
         * Delete callback implementation for synchronous delete call.
         *
         * @param rc
         *            return code
         * @param ctx
         *            optional control object
         */
        @Override
        public void deleteComplete(int rc, Object ctx) {
            finish(rc, null, future);
        }
    }

    static class LastAddConfirmedCallback implements AsyncCallback.AddLacCallback {
        static final LastAddConfirmedCallback INSTANCE = new LastAddConfirmedCallback();
        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger identifier
         * @param ctx
         *          control object
         */
        @Override
        public void addLacComplete(int rc, LedgerHandle lh, Object ctx) {
            if (rc != BKException.Code.OK) {
                log.warn("LastAddConfirmedUpdate failed: {} ", BKException.getMessage(rc));
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Callback LAC Updated for: {} ", lh.getId());
                }
            }
        }
    }

    static class SyncReadCallback implements AsyncCallback.ReadCallback {

        private final CompletableFuture<Enumeration<LedgerEntry>> future;

        public SyncReadCallback(CompletableFuture<Enumeration<LedgerEntry>> future) {
            this.future = future;
        }

        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param seq
         *          sequence of entries
         * @param ctx
         *          control object
         */
        @Override
        public void readComplete(int rc, LedgerHandle lh,
                                 Enumeration<LedgerEntry> seq, Object ctx) {
            finish(rc, seq, future);
        }
    }

    static class SyncAddCallback extends CompletableFuture<Long> implements AsyncCallback.AddCallback {

        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param entry
         *          entry identifier
         * @param ctx
         *          control object
         */
        @Override
        public void addComplete(int rc, LedgerHandle lh, long entry, Object ctx) {
            finish(rc, entry, this);
        }
    }

    static class FutureReadLastConfirmed extends CompletableFuture<Long>
        implements AsyncCallback.ReadLastConfirmedCallback {

        @Override
        public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
            finish(rc, lastConfirmed, this);
        }

    }

    static class SyncReadLastConfirmedCallback implements AsyncCallback.ReadLastConfirmedCallback {
        /**
         * Implementation of  callback interface for synchronous read last confirmed method.
         */
        @Override
        public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
            LedgerHandle.LastConfirmedCtx lcCtx = (LedgerHandle.LastConfirmedCtx) ctx;

            synchronized (lcCtx) {
                lcCtx.setRC(rc);
                lcCtx.setLastConfirmed(lastConfirmed);
                lcCtx.notify();
            }
        }
    }

    static class SyncCloseCallback implements AsyncCallback.CloseCallback {

        private final CompletableFuture<Void> future;

        public SyncCloseCallback(CompletableFuture<Void> future) {
            this.future = future;
        }

        /**
         * Close callback method.
         *
         * @param rc
         * @param lh
         * @param ctx
         */
        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            finish(rc, null, future);
        }
    }

    static class FutureReadLastConfirmedAndEntry
        extends CompletableFuture<LastConfirmedAndEntry> implements AsyncCallback.ReadLastConfirmedAndEntryCallback {

        @Override
        public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
            LastConfirmedAndEntry result = LastConfirmedAndEntryImpl.create(lastConfirmed, entry);
            finish(rc, result, this);
        }
    }

}
