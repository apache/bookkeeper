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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;

import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.TimedGenericCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read only ledger handle. This ledger handle allows you to
 * read from a ledger but not to write to it. It overrides all
 * the public write operations from LedgerHandle.
 * It should be returned for BookKeeper#openLedger operations.
 */
class ReadOnlyLedgerHandle extends LedgerHandle implements LedgerMetadataListener {
    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyLedgerHandle.class);

    private Object metadataLock = new Object();
    private final NavigableMap<Long, List<BookieSocketAddress>> newEnsemblesFromRecovery = new TreeMap<>();

    class MetadataUpdater extends SafeRunnable {

        final Versioned<LedgerMetadata> newMetadata;

        MetadataUpdater(Versioned<LedgerMetadata> metadata) {
            this.newMetadata = metadata;
        }

        @Override
        public void safeRun() {
            while (true) {
                Versioned<LedgerMetadata> currentMetadata = getVersionedLedgerMetadata();
                Version.Occurred occurred = currentMetadata.getVersion().compare(newMetadata.getVersion());
                if (Version.Occurred.BEFORE == occurred) {
                    synchronized (ReadOnlyLedgerHandle.this) {
                        if (setLedgerMetadata(currentMetadata, newMetadata)) {
                            LOG.info("Updated ledger metadata for ledger {} to {}, version {}.",
                                     ledgerId, newMetadata.getValue().toSafeString(), newMetadata.getVersion());
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }

        @Override
        public String toString() {
            return String.format("MetadataUpdater(%d)", ledgerId);
        }
    }

    ReadOnlyLedgerHandle(ClientContext clientCtx,
                         long ledgerId, Versioned<LedgerMetadata> metadata,
                         BookKeeper.DigestType digestType, byte[] password,
                         boolean watch)
            throws GeneralSecurityException, NumberFormatException {
        super(clientCtx, ledgerId, metadata, digestType, password, WriteFlag.NONE);
        if (watch) {
            clientCtx.getLedgerManager().registerLedgerMetadataListener(ledgerId, this);
        }
    }

    @Override
    public void close()
            throws InterruptedException, BKException {
        clientCtx.getLedgerManager().unregisterLedgerMetadataListener(ledgerId, this);
    }

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        clientCtx.getLedgerManager().unregisterLedgerMetadataListener(ledgerId, this);
        cb.closeComplete(BKException.Code.OK, this, ctx);
    }

    @Override
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }

    @Override
    public long addEntry(byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        LOG.error("Tried to add entry on a Read-Only ledger handle, ledgerid=" + ledgerId);
        throw BKException.create(BKException.Code.IllegalOpException);
    }

    @Override
    public void asyncAddEntry(final byte[] data, final AddCallback cb,
                              final Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    @Override
    public void asyncAddEntry(final byte[] data, final int offset, final int length,
                              final AddCallback cb, final Object ctx) {
        LOG.error("Tried to add entry on a Read-Only ledger handle, ledgerid=" + ledgerId);
        cb.addComplete(BKException.Code.IllegalOpException, this,
                       LedgerHandle.INVALID_ENTRY_ID, ctx);
    }

    @Override
    public void onChanged(long lid, Versioned<LedgerMetadata> newMetadata) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received ledger metadata update on {} : {}", lid, newMetadata);
        }
        if (this.ledgerId != lid) {
            return;
        }
        if (null == newMetadata) {
            return;
        }
        Versioned<LedgerMetadata> currentMetadata = getVersionedLedgerMetadata();
        Version.Occurred occurred = currentMetadata.getVersion().compare(newMetadata.getVersion());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to update metadata from {} to {} : {}",
                      currentMetadata, newMetadata, occurred);
        }
        if (Version.Occurred.BEFORE == occurred) { // the metadata is updated
            try {
                clientCtx.getMainWorkerPool().executeOrdered(ledgerId, new MetadataUpdater(newMetadata));
            } catch (RejectedExecutionException ree) {
                LOG.error("Failed on submitting updater to update ledger metadata on ledger {} : {}",
                        ledgerId, newMetadata);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("ReadOnlyLedgerHandle(lid = %d, id = %d)", ledgerId, super.hashCode());
    }

    @Override
    protected void initializeWriteHandleState() {
        // Essentially a noop, we don't want to set up write handle state here for a ReadOnlyLedgerHandle
        explicitLacFlushPolicy = ExplicitLacFlushPolicy.VOID_EXPLICITLAC_FLUSH_POLICY;
    }

    @Override
    public void asyncReadLastEntry(ReadCallback cb, Object ctx) {
        asyncReadLastConfirmed(new ReadLastConfirmedCallback() {
            @Override
            public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
                if (rc == BKException.Code.OK) {
                    if (lastConfirmed < 0) {
                        // Ledger was empty, so there is no last entry to read
                        cb.readComplete(BKException.Code.NoSuchEntryException, ReadOnlyLedgerHandle.this, null, ctx);
                    } else {
                        asyncReadEntriesInternal(lastConfirmed, lastConfirmed, cb, ctx, false);
                    }
                } else {
                    LOG.error("ReadException in asyncReadLastEntry, ledgerId: {}, lac: {}, rc:{}",
                        lastConfirmed, ledgerId, rc);
                    cb.readComplete(rc, ReadOnlyLedgerHandle.this, null, ctx);
                }
            }
        }, ctx);
    }

    /**
     * For a read only ledger handle, this method will only ever be called during recovery,
     * when we are reading forward from LAC and writing back those entries. As such,
     * unlike with LedgerHandle, we do not want to persist changes to the metadata as they occur,
     * but rather, we want to defer the persistence until recovery has completed, and do it all
     * on the close.
     */
    @Override
    void handleBookieFailure(final Map<Integer, BookieSocketAddress> failedBookies) {
        // handleBookieFailure should always run in the ordered executor thread for this
        // ledger, so this synchronized should be unnecessary, but putting it here now
        // just in case (can be removed when we validate threads)
        synchronized (metadataLock) {
            String logContext = String.format("[RecoveryEnsembleChange(ledger:%d)]", ledgerId);

            long lac = getLastAddConfirmed();
            LedgerMetadata metadata = getLedgerMetadata();
            List<BookieSocketAddress> currentEnsemble = getCurrentEnsemble();
            try {
                List<BookieSocketAddress> newEnsemble = EnsembleUtils.replaceBookiesInEnsemble(
                        clientCtx.getBookieWatcher(), metadata, currentEnsemble, failedBookies, logContext);
                Set<Integer> replaced = EnsembleUtils.diffEnsemble(currentEnsemble, newEnsemble);
                if (!replaced.isEmpty()) {
                    newEnsemblesFromRecovery.put(lac + 1, newEnsemble);
                    unsetSuccessAndSendWriteRequest(newEnsemble, replaced);
                }
            } catch (BKException.BKNotEnoughBookiesException e) {
                LOG.error("Could not get additional bookie to remake ensemble, closing ledger: {}", ledgerId);

                handleUnrecoverableErrorDuringAdd(e.getCode());
                return;
            }
        }
    }

    @Override
    void handleUnrecoverableErrorDuringAdd(int rc) {
        errorOutPendingAdds(rc);
    }

    void recover(GenericCallback<Void> finalCb) {
        recover(finalCb, null, false);
    }

    /**
     * Recover the ledger.
     *
     * @param finalCb
     *          callback after recovery is done.
     * @param listener
     *          read entry listener on recovery reads.
     * @param forceRecovery
     *          force the recovery procedure even the ledger metadata shows the ledger is closed.
     */
    void recover(GenericCallback<Void> finalCb,
                 final @VisibleForTesting ReadEntryListener listener,
                 final boolean forceRecovery) {
        final GenericCallback<Void> cb = new TimedGenericCallback<Void>(
            finalCb,
            BKException.Code.OK,
            clientCtx.getClientStats().getRecoverOpLogger());

        MetadataUpdateLoop.NeedsUpdatePredicate needsUpdate =
            (metadata) -> metadata.getState() == LedgerMetadata.State.OPEN;
        if (forceRecovery) {
            // in the force recovery case, we want to update the metadata
            // to IN_RECOVERY, even if the ledger is already closed
            needsUpdate = (metadata) -> metadata.getState() != LedgerMetadata.State.IN_RECOVERY;
        }
        new MetadataUpdateLoop(
                clientCtx.getLedgerManager(), getId(),
                this::getVersionedLedgerMetadata,
                needsUpdate,
                (metadata) -> LedgerMetadataBuilder.from(metadata).withInRecoveryState().build(),
                this::setLedgerMetadata)
            .run()
            .thenCompose((metadata) -> {
                    if (metadata.getValue().isClosed()) {
                        return CompletableFuture.completedFuture(ReadOnlyLedgerHandle.this);
                    } else {
                        return new LedgerRecoveryOp(ReadOnlyLedgerHandle.this, clientCtx)
                            .setEntryListener(listener)
                            .initiate();
                    }
            })
            .thenCompose((ignore) -> closeRecovered())
            .whenComplete((ignore, ex) -> {
                    if (ex != null) {
                        cb.operationComplete(
                                BKException.getExceptionCode(ex, BKException.Code.UnexpectedConditionException), null);
                    } else {
                        cb.operationComplete(BKException.Code.OK, null);
                    }
            });
    }

    CompletableFuture<Versioned<LedgerMetadata>> closeRecovered() {
        long lac, len;
        synchronized (this) {
            lac = lastAddConfirmed;
            len = length;
        }
        LOG.info("Closing recovered ledger {} at entry {}", getId(), lac);
        CompletableFuture<Versioned<LedgerMetadata>> f = new MetadataUpdateLoop(
                clientCtx.getLedgerManager(), getId(),
                this::getVersionedLedgerMetadata,
                (metadata) -> metadata.getState() == LedgerMetadata.State.IN_RECOVERY,
                (metadata) -> {
                    LedgerMetadataBuilder builder = LedgerMetadataBuilder.from(metadata);
                    Long lastEnsembleKey = LedgerMetadataUtils.getLastEnsembleKey(metadata);
                    synchronized (metadataLock) {
                        newEnsemblesFromRecovery.entrySet().forEach(
                                (e) -> {
                                    checkState(e.getKey() >= lastEnsembleKey,
                                               "Once a ledger is in recovery, noone can add ensembles without closing");
                                    // Occurs when a bookie need to be replaced at very start of recovery
                                    if (lastEnsembleKey.equals(e.getKey())) {
                                        builder.replaceEnsembleEntry(e.getKey(), e.getValue());
                                    } else {
                                        builder.newEnsembleEntry(e.getKey(), e.getValue());
                                    }
                                });
                    }
                    return builder.withClosedState().withLastEntryId(lac).withLength(len).build();
                },
                this::setLedgerMetadata).run();
        f.thenRun(() -> {
                synchronized (metadataLock) {
                    newEnsemblesFromRecovery.clear();
                }
            });
        return f;
    }

    @Override
    List<BookieSocketAddress> getCurrentEnsemble() {
        synchronized (metadataLock) {
            if (!newEnsemblesFromRecovery.isEmpty()) {
                return newEnsemblesFromRecovery.lastEntry().getValue();
            } else {
                return super.getCurrentEnsemble();
            }
        }
    }

}
