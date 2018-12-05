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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCreateAdvCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCreateCallback;
import org.apache.bookkeeper.client.api.CreateAdvBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates asynchronous ledger create operation.
 *
 */
class LedgerCreateOp {

    static final Logger LOG = LoggerFactory.getLogger(LedgerCreateOp.class);

    final CreateCallback cb;
    LedgerMetadata metadata;
    LedgerHandle lh;
    long ledgerId = -1L;
    final Object ctx;
    final int ensembleSize;
    final int writeQuorumSize;
    final int ackQuorumSize;
    final Map<String, byte[]> customMetadata;
    final byte[] passwd;
    final BookKeeper bk;
    final DigestType digestType;
    final EnumSet<WriteFlag> writeFlags;
    final long startTime;
    final OpStatsLogger createOpLogger;
    final BookKeeperClientStats clientStats;
    boolean adv = false;
    boolean generateLedgerId = true;

    /**
     * Constructor.
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
    LedgerCreateOp(
            BookKeeper bk, int ensembleSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
            byte[] passwd, CreateCallback cb, Object ctx, final Map<String, byte[]> customMetadata,
            EnumSet<WriteFlag> writeFlags,
            BookKeeperClientStats clientStats) {
        this.bk = bk;
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.customMetadata = customMetadata;
        this.writeFlags = writeFlags;
        this.passwd = passwd;
        this.cb = cb;
        this.ctx = ctx;
        this.startTime = MathUtils.nowInNano();
        this.createOpLogger = clientStats.getCreateOpLogger();
        this.clientStats = clientStats;
    }

    /**
     * Initiates the operation.
     */
    public void initiate() {
        LedgerMetadataBuilder metadataBuilder = LedgerMetadataBuilder.create()
            .withEnsembleSize(ensembleSize).withWriteQuorumSize(writeQuorumSize).withAckQuorumSize(ackQuorumSize)
            .withDigestType(digestType.toApiDigestType()).withPassword(passwd);
        if (customMetadata != null) {
            metadataBuilder.withCustomMetadata(customMetadata);
        }
        if (bk.getConf().getStoreSystemtimeAsLedgerCreationTime()) {
            metadataBuilder.withCreationTime(System.currentTimeMillis()).storingCreationTime(true);
        }

        // select bookies for first ensemble
        try {
            List<BookieSocketAddress> ensemble = bk.getBookieWatcher()
                .newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, customMetadata);
            metadataBuilder.newEnsembleEntry(0L, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            LOG.error("Not enough bookies to create ledger");
            createComplete(e.getCode(), null);
            return;
        }


        this.metadata = metadataBuilder.build();
        if (this.generateLedgerId) {
            generateLedgerIdAndCreateLedger();
        } else {
            // Create ledger with supplied ledgerId
            bk.getLedgerManager().createLedgerMetadata(ledgerId, metadata)
                .whenComplete((written, exception) -> metadataCallback(written, exception));
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
                bk.getLedgerManager().createLedgerMetadata(ledgerId, metadata)
                    .whenComplete((written, exception) -> metadataCallback(written, exception));
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
     * Callback when metadata store has responded.
     */
    private void metadataCallback(Versioned<LedgerMetadata> writtenMetadata, Throwable exception) {
        if (exception != null) {
            if (this.generateLedgerId
                && (BKException.getExceptionCode(exception) == BKException.Code.LedgerExistException)) {
                // retry to generate a new ledger id
                generateLedgerIdAndCreateLedger();
            } else {
                createComplete(BKException.getExceptionCode(exception), null);
            }
        } else {
            try {
                if (adv) {
                    lh = new LedgerHandleAdv(bk.getClientCtx(), ledgerId, writtenMetadata,
                                             digestType, passwd, writeFlags);
                } else {
                    lh = new LedgerHandle(bk.getClientCtx(), ledgerId, writtenMetadata, digestType, passwd, writeFlags);
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

            List<BookieSocketAddress> curEns = lh.getLedgerMetadata().getEnsembleAt(0L);
            LOG.info("Ensemble: {} for ledger: {}", curEns, lh.getId());

            for (BookieSocketAddress bsa : curEns) {
                clientStats.getEnsembleBookieDistributionCounter(bsa.toString()).inc();
            }

            // return the ledger handle back
            createComplete(BKException.Code.OK, lh);
        }
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

    public static class CreateBuilderImpl implements CreateBuilder {

        private final BookKeeper bk;
        private int builderEnsembleSize = 3;
        private int builderAckQuorumSize = 2;
        private int builderWriteQuorumSize = 2;
        private byte[] builderPassword;
        private EnumSet<WriteFlag> builderWriteFlags = WriteFlag.NONE;
        private org.apache.bookkeeper.client.api.DigestType builderDigestType =
            org.apache.bookkeeper.client.api.DigestType.CRC32;
        private Map<String, byte[]> builderCustomMetadata = Collections.emptyMap();

        CreateBuilderImpl(BookKeeper bk) {
            this.bk = bk;
        }

        @Override
        public CreateBuilder withEnsembleSize(int ensembleSize) {
            this.builderEnsembleSize = ensembleSize;
            return this;
        }

        @Override
        public CreateBuilder withWriteFlags(EnumSet<WriteFlag> writeFlags) {
            this.builderWriteFlags = writeFlags;
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

        @SuppressFBWarnings("EI_EXPOSE_REP2")
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
            return new CreateAdvBuilderImpl(this);
        }

        private boolean validate() {
            if (builderWriteFlags == null) {
                LOG.error("invalid null writeFlags");
                return false;
            }

            if (builderWriteQuorumSize > builderEnsembleSize) {
                LOG.error("invalid writeQuorumSize {} > ensembleSize {}", builderWriteQuorumSize, builderEnsembleSize);
                return false;
            }

            if (builderAckQuorumSize > builderWriteQuorumSize) {
                LOG.error("invalid ackQuorumSize {} > writeQuorumSize {}", builderAckQuorumSize,
                        builderWriteQuorumSize);
                return false;
            }

            if (builderAckQuorumSize <= 0) {
                LOG.error("invalid ackQuorumSize {} <= 0", builderAckQuorumSize);
                return false;
            }

            if (builderPassword == null) {
                LOG.error("invalid null password");
                return false;
            }

            if (builderDigestType == null) {
                LOG.error("invalid null digestType");
                return false;
            }

            if (builderCustomMetadata == null) {
                LOG.error("invalid null customMetadata");
                return false;
            }

            return true;
        }

        @Override
        public CompletableFuture<WriteHandle> execute() {
            CompletableFuture<WriteHandle> future = new CompletableFuture<>();
            SyncCreateCallback callback = new SyncCreateCallback(future);
            create(callback);
            return future;
        }

        private void create(CreateCallback cb) {
            if (!validate()) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, null);
                return;
            }
            LedgerCreateOp op = new LedgerCreateOp(bk, builderEnsembleSize,
                builderWriteQuorumSize, builderAckQuorumSize, DigestType.fromApiDigestType(builderDigestType),
                builderPassword, cb, null, builderCustomMetadata, builderWriteFlags,
                bk.getClientCtx().getClientStats());
            ReentrantReadWriteLock closeLock = bk.getCloseLock();
            closeLock.readLock().lock();
            try {
                if (bk.isClosed()) {
                    cb.createComplete(BKException.Code.ClientClosedException, null, null);
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
        private final CreateBuilderImpl parent;

         private CreateAdvBuilderImpl(CreateBuilderImpl parent) {
            this.parent = parent;
        }

        @Override
        public CreateAdvBuilder withLedgerId(long ledgerId) {
            builderLedgerId = ledgerId;
            return this;
        }

        @Override
        public CompletableFuture<WriteAdvHandle> execute() {
            CompletableFuture<WriteAdvHandle> future = new CompletableFuture<>();
            SyncCreateAdvCallback callback = new SyncCreateAdvCallback(future);
            create(callback);
            return future;
        }

        private boolean validate() {
            if (!parent.validate()) {
                return false;
            }
            if (builderLedgerId != null && builderLedgerId < 0) {
                LOG.error("invalid ledgerId {} < 0. Do not set en explicit value if you want automatic generation",
                        builderLedgerId);
                return false;
            }
            return true;
        }

        private void create(CreateCallback cb) {
            if (!validate()) {
                cb.createComplete(BKException.Code.IncorrectParameterException, null, null);
                return;
            }
            LedgerCreateOp op = new LedgerCreateOp(parent.bk, parent.builderEnsembleSize,
                    parent.builderWriteQuorumSize, parent.builderAckQuorumSize,
                    DigestType.fromApiDigestType(parent.builderDigestType),
                    parent.builderPassword, cb, null, parent.builderCustomMetadata,
                    parent.builderWriteFlags,
                    parent.bk.getClientCtx().getClientStats());
            ReentrantReadWriteLock closeLock = parent.bk.getCloseLock();
            closeLock.readLock().lock();
            try {
                if (parent.bk.isClosed()) {
                    cb.createComplete(BKException.Code.ClientClosedException, null, null);
                    return;
                }
                op.initiateAdv(builderLedgerId == null ? -1L : builderLedgerId);
            } finally {
                closeLock.readLock().unlock();
            }
        }
    }
}
