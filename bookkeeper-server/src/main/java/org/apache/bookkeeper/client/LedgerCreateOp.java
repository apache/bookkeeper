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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCreateCallback;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteHandle;

/**
 * Encapsulates asynchronous ledger create operation
 *
 */
class LedgerCreateOp implements GenericCallback<Void>  {

    static final Logger LOG = LoggerFactory.getLogger(LedgerCreateOp.class);

    CreateCallback cb;
    LedgerMetadata metadata;
    LedgerHandle lh;
    Long ledgerId = -1L;
    Object ctx;
    byte[] passwd;
    BookKeeper bk;
    DigestType digestType;
    long startTime;
    OpStatsLogger createOpLogger;
    boolean adv = false;
    boolean generateLedgerId = true;

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
     * @param customMetadata
     *       A map of user specified custom metadata about the ledger to be persisted; will not try to
     *       preserve the order(e.g. sortedMap) upon later retireval.
     */
    LedgerCreateOp(BookKeeper bk, int ensembleSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
            byte[] passwd, CreateCallback cb, Object ctx, final Map<String, byte[]> customMetadata) {
        this.bk = bk;
        this.metadata = new LedgerMetadata(ensembleSize, writeQuorumSize, ackQuorumSize, digestType, passwd, customMetadata);
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
            ensemble = bk.getBookieWatcher()
                    .newEnsemble(metadata.getEnsembleSize(),
                            metadata.getWriteQuorumSize(),
                            metadata.getAckQuorumSize(),
                            metadata.getCustomMetadata());
        } catch (BKNotEnoughBookiesException e) {
            LOG.error("Not enough bookies to create ledger");
            createComplete(e.getCode(), null);
            return;
        }

        /*
         * Add ensemble to the configuration
         */
        metadata.addEnsemble(0L, ensemble);
        if (this.generateLedgerId) {
            generateLedgerIdAndCreateLedger();
        } else {
            // Create ledger with supplied ledgerId
            bk.getLedgerManager().createLedgerMetadata(ledgerId, metadata, LedgerCreateOp.this);
        }
    }

    void generateLedgerIdAndCreateLedger() {
        // generate a ledgerId
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
    public void initiateAdv(final long ledgerId) {
        this.adv = true;
        this.ledgerId = ledgerId;
        if (this.ledgerId != -1L) {
            this.generateLedgerId = false;
        }
        initiate();
    }

    /**
     * Callback when created ledger.
     */
    @Override
    public void operationComplete(int rc, Void result) {
        if (this.generateLedgerId && (BKException.Code.LedgerExistException == rc)) {
            // retry to generate a new ledger id
            generateLedgerIdAndCreateLedger();
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

    private static final byte[] EMPTY_PASSWORD = new byte[0];

    public static class CreateBuilderImpl implements CreateBuilder {

        private final BookKeeper bk;
        private int builderEnsembleSize = 3;
        private int builderAckQuorumSize = 2;
        private int builderWriteQuorumSize = 2;
        private byte[] builderPassword = EMPTY_PASSWORD;
        private org.apache.bookkeeper.client.api.DigestType builderDigestType
            = org.apache.bookkeeper.client.api.DigestType.CRC32;
        private Map<String, byte[]> builderCustomMetadata = Collections.emptyMap();
        private CreateAdvBuilder advBuilder;

        public CreateBuilderImpl(BookKeeper bk) {
            this.bk = bk;
        }

        @Override
        public CreateBuilder withEnsembleSize(int ensembleSize) {
            this.builderEnsembleSize = ensembleSize;
            return this;
        }

        @Override
        public CreateBuilder withWriteQuorumSize(int writeQuorumSize) {
            this.builderWriteQuorumSize = writeQuorumSize;
            return this;
        }

        @Override
        public CreateBuilder withAckQuorumSize(int ackQuorumSize) {
            this.builderAckQuorumSize = ackQuorumSize;
            return this;
        }

        @Override
        public CreateBuilder withPassword(byte[] password) {
            this.builderPassword = password;
            return this;
        }

        @Override
        public CreateBuilder withCustomMetadata(Map<String, byte[]> customMetadata) {
            this.builderCustomMetadata = customMetadata;
            return this;
        }

        @Override
        public CreateBuilder withDigestType(org.apache.bookkeeper.client.api.DigestType digestType) {
            this.builderDigestType = digestType;
            return this;
        }

        @Override
        public CreateAdvBuilder makeAdv() {
            if(advBuilder == null) {
                advBuilder = new CreateAdvBuilderImpl(this);
            }
            return advBuilder;
        }

        private boolean validate(CreateCallback cb, Object ctx) {
            if (builderWriteQuorumSize > builderEnsembleSize) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderAckQuorumSize > builderWriteQuorumSize) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderAckQuorumSize <= 0) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderPassword == null) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderDigestType == null) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderCustomMetadata == null) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            return true;
        }

        @Override
        public CompletableFuture<WriteHandle> execute() {
            CompletableFuture<WriteHandle> counter = new CompletableFuture<>();
            create(new SyncCreateCallback(), counter);
            return counter;
        }

        private void create(CreateCallback cb, Object ctx) {
            if (!validate(cb, ctx)) {
                return;
            }
            LedgerCreateOp op = new LedgerCreateOp(bk, builderEnsembleSize,
                builderWriteQuorumSize, builderAckQuorumSize, DigestType.fromApiDigestType(builderDigestType),
                builderPassword, cb, ctx, builderCustomMetadata);
            ReentrantReadWriteLock closeLock = bk.getCloseLock();
            closeLock.readLock().lock();
            try {
                if (bk.isClosed()) {
                    cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                    return;
                }
                op.initiate();
            } finally {
                closeLock.readLock().unlock();
            }
        }
    }

    private static class CreateAdvBuilderImpl implements CreateAdvBuilder {

        private Long builderLedgerId;
        private final int builderEnsembleSize;
        private final int builderAckQuorumSize;
        private final int builderWriteQuorumSize;
        private final byte[] builderPassword;
        private final Map<String, byte[]> builderCustomMetadata;
        private final org.apache.bookkeeper.client.api.DigestType builderDigestType;
        private final BookKeeper bk;

         private CreateAdvBuilderImpl(CreateBuilderImpl other) {
            this.builderEnsembleSize = other.builderEnsembleSize;
            this.builderAckQuorumSize = other.builderAckQuorumSize;
            this.builderWriteQuorumSize = other.builderWriteQuorumSize;
            this.builderPassword = other.builderPassword;
            this.builderDigestType = other.builderDigestType;
            this.builderCustomMetadata = other.builderCustomMetadata;
            this.bk = other.bk;
        }

        @Override
        public CreateAdvBuilder withLedgerId(long ledgerId) {
            builderLedgerId = ledgerId;
            return this;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> execute() {
            CompletableFuture<WriteAdvHandle> counter = new CompletableFuture<>();
            create(new SyncCreateCallback(), counter);
            return counter;
        }

        private boolean validateAdv(CreateCallback cb, Object ctx) {
             if (builderLedgerId != null && builderLedgerId < 0) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderWriteQuorumSize > builderEnsembleSize) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderAckQuorumSize > builderWriteQuorumSize) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderAckQuorumSize <= 0) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderPassword == null) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderDigestType == null) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            if (builderCustomMetadata == null) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, ctx);
                return false;
            }

            return true;
        }

        private void create(CreateCallback cb, Object ctx) {
            if (!validateAdv(cb, ctx)) {
                return;
            }
            LedgerCreateOp op = new LedgerCreateOp(bk, builderEnsembleSize,
                    builderWriteQuorumSize, builderAckQuorumSize, DigestType.fromApiDigestType(builderDigestType),
                    builderPassword, cb, ctx, builderCustomMetadata);
            bk.getCloseLock().readLock().lock();
            try {
                if (bk.isClosed()) {
                    cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                    return;
                }
                op.initiateAdv(builderLedgerId == null ? -1L : builderLedgerId);
            } finally {
                bk.getCloseLock().readLock().unlock();
            }
        }
    }
}
