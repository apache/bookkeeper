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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.security.GeneralSecurityException;

import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the ledger open operation
 *
 */
class LedgerOpenOp implements GenericCallback<LedgerMetadata> {
    static final Logger LOG = LoggerFactory.getLogger(LedgerOpenOp.class);

    final BookKeeper bk;
    final long ledgerId;
    final OpenCallback cb;
    final Object ctx;
    LedgerHandle lh;
    final byte[] passwd;
    final DigestType digestType;
    boolean doRecovery = true;
    boolean administrativeOpen = false;
    long startTime;
    OpStatsLogger openOpLogger;

    /**
     * Constructor.
     *
     * @param bk
     * @param ledgerId
     * @param digestType
     * @param passwd
     * @param cb
     * @param ctx
     */
    public LedgerOpenOp(BookKeeper bk, long ledgerId, DigestType digestType, byte[] passwd,
                        OpenCallback cb, Object ctx) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.passwd = passwd;
        this.cb = cb;
        this.ctx = ctx;
        this.digestType = digestType;
    }

    public LedgerOpenOp(BookKeeper bk, long ledgerId, OpenCallback cb, Object ctx) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.cb = cb;
        this.ctx = ctx;

        this.passwd = bk.getConf().getBookieRecoveryPasswd();
        this.digestType = bk.getConf().getBookieRecoveryDigestType();
        this.administrativeOpen = true;
    }

    /**
     * Inititates the ledger open operation
     */
    public void initiate() {
        startTime = MathUtils.nowInNano();

        openOpLogger = bk.getOpenOpLogger();

        /**
         * Asynchronously read the ledger metadata node.
         */
        bk.getLedgerManager().readLedgerMetadata(ledgerId, this);
    }

    /**
     * Inititates the ledger open operation without recovery
     */
    public void initiateWithoutRecovery() {
        this.doRecovery = false;
        initiate();
    }

    /**
     * Implements Open Ledger Callback.
     */
    @Override
    public void operationComplete(int rc, LedgerMetadata metadata) {
        if (BKException.Code.OK != rc) {
            // open ledger failed.
            openComplete(rc, null);
            return;
        }

        final byte[] passwd;
        final DigestType digestType;

        /* For an administrative open, the default passwords
         * are read from the configuration, but if the metadata
         * already contains passwords, use these instead. */
        if (administrativeOpen && metadata.hasPassword()) {
            passwd = metadata.getPassword();
            digestType = metadata.getDigestType();
        } else {
            passwd = this.passwd;
            digestType = this.digestType;

            if (metadata.hasPassword()) {
                if (!Arrays.equals(passwd, metadata.getPassword())) {
                    LOG.error("Provided passwd does not match that in metadata");
                    openComplete(BKException.Code.UnauthorizedAccessException, null);
                    return;
                }
                if (digestType != metadata.getDigestType()) {
                    LOG.error("Provided digest does not match that in metadata");
                    openComplete(BKException.Code.DigestMatchException, null);
                    return;
                }
            }
        }

        // get the ledger metadata back
        try {
            lh = new ReadOnlyLedgerHandle(bk, ledgerId, metadata, digestType, passwd, !doRecovery);
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
            lh.recover(new OrderedSafeGenericCallback<Void>(bk.mainWorkerPool, ledgerId) {
                @Override
                public void safeOperationComplete(int rc, Void result) {
                    if (rc == BKException.Code.OK) {
                        openComplete(BKException.Code.OK, lh);
                    } else if (rc == BKException.Code.UnauthorizedAccessException) {
                        openComplete(BKException.Code.UnauthorizedAccessException, null);
                    } else {
                        openComplete(bk.getReturnRc(BKException.Code.LedgerRecoveryException), null);
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
                    if (rc != BKException.Code.OK) {
                        openComplete(bk.getReturnRc(BKException.Code.ReadException), null);
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
        cb.openComplete(rc, lh, ctx);
    }
}
