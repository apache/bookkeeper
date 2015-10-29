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

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates asynchronous ledger create operation
 *
 */
class LedgerCreateOp implements GenericCallback<Void> {

    static final Logger LOG = LoggerFactory.getLogger(LedgerCreateOp.class);

    CreateCallback cb;
    LedgerMetadata metadata;
    LedgerHandle lh;
    Long ledgerId;
    Object ctx;
    byte[] passwd;
    BookKeeper bk;
    DigestType digestType;
    long startTime;
    OpStatsLogger createOpLogger;
    boolean adv = false;

    /**
     * Constructor
     *
     * @param bk
     *       BookKeeper object
     * @param ensembleSize
     *       ensemble size
     * @param writeQuorumSize
     *       write quorum size
     * @param ackQuorumSize
     *       ack quorum size
     * @param digestType
     *       digest type, either MAC or CRC32
     * @param passwd
     *       password
     * @param cb
     *       callback implementation
     * @param ctx
     *       optional control object
     */

    LedgerCreateOp(BookKeeper bk, int ensembleSize,
                   int writeQuorumSize, int ackQuorumSize,
                   DigestType digestType,
                   byte[] passwd, CreateCallback cb, Object ctx) {
        this.bk = bk;
        this.metadata = new LedgerMetadata(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, passwd);
        this.digestType = digestType;
        this.passwd = passwd;
        this.cb = cb;
        this.ctx = ctx;
        this.startTime = MathUtils.nowInNano();
        this.createOpLogger = bk.getCreateOpLogger();
    }

    /**
     * Initiates the operation
     */
    public void initiate() {
        // allocate ensemble first

        /*
         * Adding bookies to ledger handle
         */

        ArrayList<BookieSocketAddress> ensemble;
        try {
            ensemble = bk.bookieWatcher
                    .newEnsemble(metadata.getEnsembleSize(), metadata.getWriteQuorumSize());
        } catch (BKNotEnoughBookiesException e) {
            LOG.error("Not enough bookies to create ledger");
            createComplete(e.getCode(), null);
            return;
        }

        /*
         * Add ensemble to the configuration
         */
        metadata.addEnsemble(0L, ensemble);

        createLedger();
    }

    void createLedger() {
        // generate a ledger id and then create the ledger with metadata
        final LedgerIdGenerator ledgerIdGenerator = bk.getLedgerIdGenerator();
        ledgerIdGenerator.generateLedgerId(new GenericCallback<Long>() {
            @Override
            public void operationComplete(int rc, Long ledgerId) {
                if (BKException.Code.OK != rc) {
                    createComplete(rc, null);
                    return;
                }

                LedgerCreateOp.this.ledgerId = ledgerId;
                // create a ledger with metadata
                bk.getLedgerManager().createLedgerMetadata(ledgerId, metadata, LedgerCreateOp.this);
            }
        });
    }

    /**
     * Initiates the operation to return LedgerHandleAdv.
     */
    public void initiateAdv() {
        this.adv = true;
        initiate();
    }

    /**
     * Callback when created ledger.
     */
    @Override
    public void operationComplete(int rc, Void result) {
        if (BKException.Code.LedgerExistException == rc) {
            // retry to generate a new ledger id
            createLedger();
            return;
        } else if (BKException.Code.OK != rc) {
            createComplete(rc, null);
            return;
        }

        try {
            if (adv) {
                lh = new LedgerHandleAdv(bk, ledgerId, metadata, digestType, passwd);
            } else {
                lh = new LedgerHandle(bk, ledgerId, metadata, digestType, passwd);
            }
        } catch (GeneralSecurityException e) {
            LOG.error("Security exception while creating ledger: " + ledgerId, e);
            createComplete(BKException.Code.DigestNotInitializedException, null);
            return;
        } catch (NumberFormatException e) {
            LOG.error("Incorrectly entered parameter throttle: " + bk.getConf().getThrottleValue(), e);
            createComplete(BKException.Code.IncorrectParameterException, null);
            return;
        }
        // return the ledger handle back
        createComplete(BKException.Code.OK, lh);
    }

    private void createComplete(int rc, LedgerHandle lh) {
        // Opened a new ledger
        if (BKException.Code.OK != rc) {
            createOpLogger.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        } else {
            createOpLogger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        }
        cb.createComplete(rc, lh, ctx);
    }

}
