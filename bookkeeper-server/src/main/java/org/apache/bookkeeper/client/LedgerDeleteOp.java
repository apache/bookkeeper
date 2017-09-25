/*
 *
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

package org.apache.bookkeeper.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncDeleteCallback;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.apache.bookkeeper.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates asynchronous ledger delete operation
 *
 */
class LedgerDeleteOp extends OrderedSafeGenericCallback<Void> {

    static final Logger LOG = LoggerFactory.getLogger(LedgerDeleteOp.class);

    BookKeeper bk;
    long ledgerId;
    DeleteCallback cb;
    Object ctx;
    long startTime;
    OpStatsLogger deleteOpLogger;

    /**
     * Constructor
     *
     * @param bk
     *            BookKeeper object
     * @param ledgerId
     *            ledger Id
     * @param cb
     *            callback implementation
     * @param ctx
     *            optional control object
     */
    LedgerDeleteOp(BookKeeper bk, long ledgerId, DeleteCallback cb, Object ctx) {
        super(bk.getMainWorkerPool(), ledgerId);
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.cb = cb;
        this.ctx = ctx;
        this.startTime = MathUtils.nowInNano();
        this.deleteOpLogger = bk.getDeleteOpLogger();
    }

    /**
     * Initiates the operation
     */
    public void initiate() {
        // Asynchronously delete the ledger from meta manager
        // When this completes, it will invoke the callback method below.
        bk.getLedgerManager().removeLedgerMetadata(ledgerId, Version.ANY, this);
    }

    /**
     * Implements Delete Callback.
     */
    @Override
    public void safeOperationComplete(int rc, Void result) {
        if (BKException.Code.OK != rc) {
            deleteOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        } else {
            deleteOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        }
        cb.deleteComplete(rc, this.ctx);
    }

    @Override
    public String toString() {
        return String.format("LedgerDeleteOp(%d)", ledgerId);
    }

    public static class DeleteBuilderImpl  implements DeleteBuilder {

        private long builderLedgerId = -1;
        private final BookKeeper bk;

        public DeleteBuilderImpl(BookKeeper bk) {
            this.bk = bk;
        }

        @Override
        public DeleteBuilder withLedgerId(long ledgerId) {
            this.builderLedgerId = ledgerId;
            return this;
        }

        @Override
        public CompletableFuture<?> execute() {
            CompletableFuture<Void> counter = new CompletableFuture<>();
            delete(builderLedgerId, new SyncDeleteCallback(), counter);
            return counter;
        }

        private void delete(long ledgerId, AsyncCallback.DeleteCallback cb, Object ctx) {
            LedgerDeleteOp op = new LedgerDeleteOp(bk, ledgerId, cb, ctx);
            bk.getCloseLock().readLock().lock();
            try {
                if (bk.isClosed()) {
                    cb.deleteComplete(BKException.Code.ClientClosedException, ctx);
                    return;
                }
                op.initiate();
            } finally {
                bk.getCloseLock().readLock().unlock();
            }
        }
    }

}
