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

import static org.apache.bookkeeper.client.BookKeeper.DigestType.fromApiDigestType;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncOpenCallback;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.OpenBuilderBase;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedGenericCallback;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the ledger open operation.
 *
 */
class LedgerOpenOp {
    static final Logger LOG = LoggerFactory.getLogger(LedgerOpenOp.class);

    final BookKeeper bk;
    final long ledgerId;
    final OpenCallback cb;
    final Object ctx;
    ReadOnlyLedgerHandle lh;
    final byte[] passwd;
    boolean doRecovery = true;
    // The ledger metadata may be modified even if it has been closed, because the auto-recovery component may rewrite
    // the ledger's metadata. Keep receiving a notification from ZK to avoid the following issue: an opened ledger
    // handle in memory still accesses to a BK instance who has been decommissioned. The issue that solved happens as
    // follows:
    // 1. Client service open a readonly ledger handle, which has been closed.
    // 2. All BKs that relates to the ledger have been decommissioned.
    // 3. Auto recovery component moved the data into other BK instances who is alive.
    // 4. The ledger handle in the client memory keeps connects to the BKs who in the original ensemble set, and the
    //    connection will always fail.
    // For minimum modification, to add a new configuration named "keepUpdateMetadata", users can use the
    // new API to create a readonly ledger handle that will auto-updates metadata.
    boolean keepUpdateMetadata = false;
    boolean administrativeOpen = false;
    long startTime;
    final OpStatsLogger openOpLogger;

    final DigestType suggestedDigestType;
    final boolean enableDigestAutodetection;

    /**
     * Constructor.
     *
     * @param bk
     * @param ledgerId
     * @param digestType Ignored if conf.getEnableDigestTypeAutodetection() is true
     * @param passwd
     * @param cb
     * @param ctx
     */
    public LedgerOpenOp(BookKeeper bk, BookKeeperClientStats clientStats,
                        long ledgerId, DigestType digestType, byte[] passwd,
                        OpenCallback cb, Object ctx) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.passwd = passwd;
        this.cb = cb;
        this.ctx = ctx;
        this.enableDigestAutodetection = bk.getConf().getEnableDigestTypeAutodetection();
        this.suggestedDigestType = digestType;
        this.openOpLogger = clientStats.getOpenOpLogger();
    }

    public LedgerOpenOp(BookKeeper bk, BookKeeperClientStats clientStats,
                        long ledgerId, OpenCallback cb, Object ctx) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.cb = cb;
        this.ctx = ctx;

        this.passwd = bk.getConf().getBookieRecoveryPasswd();
        this.administrativeOpen = true;
        this.enableDigestAutodetection = false;
        this.suggestedDigestType = bk.conf.getBookieRecoveryDigestType();
        this.openOpLogger = clientStats.getOpenOpLogger();
    }

    /**
     * Inititates the ledger open operation.
     */
    public void initiate() {
        startTime = MathUtils.nowInNano();

        /**
         * Asynchronously read the ledger metadata node.
         */
        bk.getLedgerManager().readLedgerMetadata(ledgerId)
                .thenAcceptAsync(this::openWithMetadata, bk.getScheduler().chooseThread(ledgerId))
                .exceptionally(exception -> {
                    openComplete(BKException.getExceptionCode(exception), null);
                    return null;
                });
    }

    /**
     * Inititates the ledger open operation without recovery.
     */
    public void initiateWithoutRecovery() {
        this.doRecovery = false;
        initiate();
    }

    /**
     * Different with {@link #initiate()}, the method keep update metadata once the auto-recover component modified
     * the ensemble.
     */
    public void initiateWithKeepUpdateMetadata() {
        this.keepUpdateMetadata = true;
        initiate();
    }

    private CompletableFuture<Void> closeLedgerHandleAsync() {
        if (lh != null) {
            return lh.closeAsync();
        }
        return CompletableFuture.completedFuture(null);
    }

    private void openWithMetadata(Versioned<LedgerMetadata> versionedMetadata) {
        LedgerMetadata metadata = versionedMetadata.getValue();

        final byte[] passwd;

        // we should use digest type from metadata *ONLY* when:
        // 1) digest type is stored in metadata
        // 2) `autodetection` is enabled
        DigestType digestType;
        if (enableDigestAutodetection && metadata.hasPassword()) {
            digestType = fromApiDigestType(metadata.getDigestType());
        } else {
            digestType = suggestedDigestType;
        }

        /* For an administrative open, the default passwords
         * are read from the configuration, but if the metadata
         * already contains passwords, use these instead. */
        if (administrativeOpen && metadata.hasPassword()) {
            passwd = metadata.getPassword();
            digestType = fromApiDigestType(metadata.getDigestType());
        } else {
            passwd = this.passwd;

            if (metadata.hasPassword()) {
                if (!Arrays.equals(passwd, metadata.getPassword())) {
                    LOG.error("Provided passwd does not match that in metadata");
                    openComplete(BKException.Code.UnauthorizedAccessException, null);
                    return;
                }
                // if `digest auto detection` is enabled, ignore the suggested digest type, this allows digest type
                // changes. e.g. moving from `crc32` to `crc32c`.
                if (suggestedDigestType != fromApiDigestType(metadata.getDigestType()) && !enableDigestAutodetection) {
                    LOG.error("Provided digest does not match that in metadata");
                    openComplete(BKException.Code.DigestMatchException, null);
                    return;
                }
            }
        }

        // get the ledger metadata back
        // The cases that need to register listener immediately are:
        // 1. The ledger is not in recovery opening, which is the original case.
        // 2. The ledger is closed and need to keep update metadata. There is other cases that do not need to
        //   register listener. e.g. The ledger is opening by Auto-Recovery component.
        final boolean watchImmediately = !doRecovery || (keepUpdateMetadata && metadata.isClosed());
        try {
            // The ledger metadata may be modified even if it has been closed, because the auto-recovery component may
            // rewrite the ledger's metadata. Keep receiving a notification from ZK to avoid the following issue: an
            // opened ledger handle in memory still accesses to a BK instance who has been decommissioned. The issue
            // that solved happens as follows:
            // 1. Client service open a readonly ledger handle, which has been closed.
            // 2. All BKs that relates to the ledger have been decommissioned.
            // 3. Auto recovery component moved the data into other BK instances who is alive.
            // 4. The ledger handle in the client memory keeps connects to the BKs who in the original ensemble set,
            //    and the connection will always fail.
            // Therefore, if a user needs to the feature that update metadata automatically, he will set
            // "keepUpdateMetadata" to "true",
            lh = new ReadOnlyLedgerHandle(bk.getClientCtx(), ledgerId, versionedMetadata, digestType,
                                          passwd, watchImmediately);
        } catch (GeneralSecurityException e) {
            LOG.error("Security exception while opening ledger: " + ledgerId, e);
            openComplete(BKException.Code.DigestNotInitializedException, null);
            return;
        } catch (NumberFormatException e) {
            LOG.error("Incorrectly entered parameter throttle: " + bk.getConf().getThrottleValue(), e);
            openComplete(BKException.Code.IncorrectParameterException, null);
            return;
        }

        if (metadata.isClosed()) {
            // Ledger was closed properly
            openComplete(BKException.Code.OK, lh);
            return;
        }

        if (doRecovery) {
            lh.recover(new OrderedGenericCallback<Void>(bk.getMainWorkerPool(), ledgerId) {
                @Override
                public void safeOperationComplete(int rc, Void result) {
                    if (rc == BKException.Code.OK) {
                        openComplete(BKException.Code.OK, lh);
                        if (!watchImmediately && keepUpdateMetadata) {
                            lh.registerLedgerMetadataListener();
                        }
                    } else {
                        closeLedgerHandleAsync().whenComplete((ignore, ex) -> {
                            if (ex != null) {
                                LOG.error("Ledger {} close failed", ledgerId, ex);
                            }
                            if (rc == BKException.Code.UnauthorizedAccessException
                                    || rc == BKException.Code.TimeoutException) {
                                openComplete(bk.getReturnRc(rc), null);
                            } else {
                                openComplete(bk.getReturnRc(BKException.Code.LedgerRecoveryException), null);
                            }
                        });
                    }
                }
                @Override
                public String toString() {
                    return String.format("Recover(%d)", ledgerId);
                }
            });
        } else {
            lh.asyncReadLastConfirmed(new ReadLastConfirmedCallback() {
                @Override
                public void readLastConfirmedComplete(int rc,
                        long lastConfirmed, Object ctx) {
                    if (rc == BKException.Code.TimeoutException) {
                        closeLedgerHandleAsync().whenComplete((r, ex) -> {
                            if (ex != null) {
                                LOG.error("Ledger {} close failed", ledgerId, ex);
                            }
                            openComplete(bk.getReturnRc(rc), null);
                        });
                    } else if (rc != BKException.Code.OK) {
                        closeLedgerHandleAsync().whenComplete((r, ex) -> {
                            if (ex != null) {
                                LOG.error("Ledger {} close failed", ledgerId, ex);
                            }
                            openComplete(bk.getReturnRc(BKException.Code.ReadException), null);
                        });
                    } else {
                        lh.lastAddConfirmed = lh.lastAddPushed = lastConfirmed;
                        openComplete(BKException.Code.OK, lh);
                    }
                }
            }, null);

        }
    }

    void openComplete(int rc, LedgerHandle lh) {
        if (BKException.Code.OK != rc) {
            openOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        } else {
            openOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        }

        if (lh != null) { // lh is null in case of errors
            lh.executeOrdered(() -> cb.openComplete(rc, lh, ctx));
        } else {
            cb.openComplete(rc, null, ctx);
        }
    }

    static final class OpenBuilderImpl extends OpenBuilderBase {

        private final BookKeeper bk;

        OpenBuilderImpl(BookKeeper bookkeeper) {
            this.bk = bookkeeper;
        }

        @Override
        public CompletableFuture<ReadHandle> execute() {
            CompletableFuture<ReadHandle> future = new CompletableFuture<>();
            SyncOpenCallback callback = new SyncOpenCallback(future);
            open(callback);
            return future;
        }

        private void open(OpenCallback cb) {
            final int validateRc = validate();
            if (Code.OK != validateRc) {
                cb.openComplete(validateRc, null, null);
                return;
            }

            LedgerOpenOp op = new LedgerOpenOp(bk, bk.getClientCtx().getClientStats(),
                                               ledgerId, fromApiDigestType(digestType),
                                               password, cb, null);
            ReentrantReadWriteLock closeLock = bk.getCloseLock();
            closeLock.readLock().lock();
            try {
                if (bk.isClosed()) {
                    cb.openComplete(BKException.Code.ClientClosedException, null, null);
                    return;
                }
                if (recovery) {
                    op.initiate();
                } else {
                    op.initiateWithoutRecovery();
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
    }

}
