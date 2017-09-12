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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.WriteAdvHandler;
import org.apache.bookkeeper.client.api.WriteHandler;
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
class LedgerCreateOp implements GenericCallback<Void>, CreateBuilder {

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
     * // for CreateLedgerBuilder interface
     * @param bk
     */
    LedgerCreateOp(BookKeeper bk) {
        this.bk=bk;
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

    // Builder interface methods

    private static final byte[] EMPTY_PASSWORD = new byte[0];
    private int builderEnsembleSize;
    private int builderAckQuorumSize;
    private int builderWriteQuorumSize;
    private long builderLedgerId = -1L;
    private byte[] builderPassword = EMPTY_PASSWORD;
    private DigestType builderDigestType = DigestType.CRC32;
    private Map<String, byte[]> builderCustomMetadata;

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
    public CreateBuilder withDigestType(DigestType digestType) {
        this.builderDigestType = digestType;
        return this;
    }

    @Override
    public CreateAdvBuilder makeAdv() {
        if(advBuilder == null) {
            advBuilder = new CreateAdvBuilderImpl();
        }
        return advBuilder;
    }

    private CreateAdvBuilder advBuilder;

    @Override
    public WriteHandler create() throws BKException, InterruptedException {
        CompletableFuture<LedgerHandle> counter = new CompletableFuture<>();

        create(new BookKeeper.SyncCreateCallback(), counter);

        LedgerHandle lh = SynchCallbackUtils.waitForResult(counter);
        if (lh == null) {
            LOG.error("Unexpected condition : no ledger handle returned for a success ledger creation");
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }
        return lh;
    }

    @Override
    public CompletableFuture<WriteHandler> execute() {
        CompletableFuture<WriteHandler> counter = new CompletableFuture<>();
        create(new BookKeeper.SyncCreateCallback(), counter);
        return counter;
    }

    private void applyDefaults() {
        if (builderEnsembleSize == 0){
            builderEnsembleSize = 3;
        }
        if (builderWriteQuorumSize == 0) {
            builderWriteQuorumSize = builderEnsembleSize;
        }
        if (builderAckQuorumSize == 0) {
            builderAckQuorumSize = builderWriteQuorumSize;
        }
    }

    @Override
    public void create(CreateCallback cb, Object ctx) {
        applyDefaults();
        if (builderWriteQuorumSize < builderAckQuorumSize) {
            throw new IllegalArgumentException("Write quorum must be larger than ack quorum");
        }
        bk.closeLock.readLock().lock();
        try {
            if (bk.closed) {
                cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                return;
            }

            this.metadata = new LedgerMetadata(builderEnsembleSize,
                builderWriteQuorumSize,
                builderAckQuorumSize,
                builderDigestType,
                builderPassword,
                builderCustomMetadata);
            this.digestType = builderDigestType;
            this.passwd = builderPassword;
            this.cb = cb;
            this.ctx = ctx;
            this.startTime = MathUtils.nowInNano();
            initiate();
        } finally {
            bk.closeLock.readLock().unlock();
        }
    }

    private class CreateAdvBuilderImpl implements CreateAdvBuilder {

        @Override
        public CreateAdvBuilder withLedgerId(int ledgerId) {
            builderLedgerId = ledgerId;
            return this;
        }

        @Override
        public WriteAdvHandler create() throws BKException, InterruptedException {
            CompletableFuture<LedgerHandleAdv> counter = new CompletableFuture<>();

            create(new BookKeeper.SyncCreateCallback(), counter);

            LedgerHandleAdv lh = SynchCallbackUtils.waitForResult(counter);
            if (lh == null) {
                LOG.error("Unexpected condition : no ledger handle returned for a success ledger creation");
                throw BKException.create(BKException.Code.UnexpectedConditionException);
            }
            return lh;
        }

        @Override
        public CompletableFuture<WriteAdvHandler> execute() {
            CompletableFuture<WriteAdvHandler> counter = new CompletableFuture<>();
            create(new BookKeeper.SyncCreateCallback(), counter);
            return counter;
        }

        @Override
        public void create(CreateCallback cb, Object ctx) {
            applyDefaults();
            if (builderWriteQuorumSize < builderAckQuorumSize) {
                throw new IllegalArgumentException("Write quorum must be larger than ack quorum");
            }
            bk.closeLock.readLock().lock();
            try {
                if (bk.closed) {
                    cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                    return;
                }
                LedgerCreateOp.this.metadata = new LedgerMetadata(builderEnsembleSize,
                    builderWriteQuorumSize,
                    builderAckQuorumSize,
                    builderDigestType,
                    builderPassword,
                    builderCustomMetadata);
                LedgerCreateOp.this.digestType = builderDigestType;
                LedgerCreateOp.this.passwd = builderPassword;
                LedgerCreateOp.this.cb = cb;
                LedgerCreateOp.this.ctx = ctx;
                LedgerCreateOp.this.startTime = MathUtils.nowInNano();
                initiateAdv(LedgerCreateOp.this.builderLedgerId);
            } finally {
                bk.closeLock.readLock().unlock();
            }
        }
    }

}
