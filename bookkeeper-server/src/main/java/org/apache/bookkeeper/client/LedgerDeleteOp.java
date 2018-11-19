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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncDeleteCallback;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates asynchronous ledger delete operation.
 *
 */
class LedgerDeleteOp {

    static final Logger LOG = LoggerFactory.getLogger(LedgerDeleteOp.class);

    final BookKeeper bk;
    final long ledgerId;
    final DeleteCallback cb;
    final Object ctx;
    final long startTime;
    final OpStatsLogger deleteOpLogger;

    /**
     * Constructor.
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
    LedgerDeleteOp(BookKeeper bk, BookKeeperClientStats clientStats,
                   long ledgerId, DeleteCallback cb, Object ctx) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.cb = cb;
        this.ctx = ctx;
        this.startTime = MathUtils.nowInNano();
        this.deleteOpLogger = clientStats.getDeleteOpLogger();
    }

    /**
     * Initiates the operation.
     */
    public void initiate() {
        // Asynchronously delete the ledger from meta manager
        // When this completes, it will invoke the callback method below.
        bk.getLedgerManager().removeLedgerMetadata(ledgerId, Version.ANY)
            .whenCompleteAsync((ignore, exception) -> {
                    if (exception != null) {
                        deleteOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                    } else {
                        deleteOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                    }
                    cb.deleteComplete(BKException.getExceptionCode(exception), this.ctx);
                }, bk.getMainWorkerPool().chooseThread(ledgerId));
    }

    @Override
    public String toString() {
        return String.format("LedgerDeleteOp(%d)", ledgerId);
    }

    static class DeleteBuilderImpl  implements DeleteBuilder {

        private Long builderLedgerId;
        private final BookKeeper bk;

        DeleteBuilderImpl(BookKeeper bk) {
            this.bk = bk;
        }

        @Override
        public DeleteBuilder withLedgerId(long ledgerId) {
            this.builderLedgerId = ledgerId;
            return this;
        }

        @Override
        public CompletableFuture<Void> execute() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            SyncDeleteCallback result = new SyncDeleteCallback(future);
            delete(builderLedgerId, result);
            return future;
        }

        private boolean validate() {
            if (builderLedgerId == null || builderLedgerId < 0) {
                LOG.error("invalid ledgerId {} < 0", builderLedgerId);
                return false;
            }
            return true;
        }

        private void delete(Long ledgerId, AsyncCallback.DeleteCallback cb) {
            if (!validate()) {
                cb.deleteComplete(BKException.Code.IncorrectParameterException, null);
                return;
            }
            LedgerDeleteOp op = new LedgerDeleteOp(bk, bk.getClientCtx().getClientStats(), ledgerId, cb, null);
            ReentrantReadWriteLock closeLock = bk.getCloseLock();
            closeLock.readLock().lock();
            try {
                if (bk.isClosed()) {
                    cb.deleteComplete(BKException.Code.ClientClosedException, null);
                    return;
                }
                op.initiate();
            } finally {
                closeLock.readLock().unlock();
            }
        }
    }

}
