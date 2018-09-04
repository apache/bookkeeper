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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.TimedGenericCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Version;

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

        final LedgerMetadata newMetadata;

        MetadataUpdater(LedgerMetadata metadata) {
            this.newMetadata = metadata;
        }

        @Override
        public void safeRun() {
            while (true) {
                LedgerMetadata currentMetadata = getLedgerMetadata();
                Version.Occurred occurred = currentMetadata.getVersion().compare(newMetadata.getVersion());
                if (Version.Occurred.BEFORE == occurred) {
                    LOG.info("Updated ledger metadata for ledger {} to {}.", ledgerId, newMetadata.toSafeString());
                    synchronized (ReadOnlyLedgerHandle.this) {
                        if (newMetadata.isClosed()) {
                            ReadOnlyLedgerHandle.this.lastAddConfirmed = newMetadata.getLastEntryId();
                            ReadOnlyLedgerHandle.this.length = newMetadata.getLength();
                        }
                        if (setLedgerMetadata(currentMetadata, newMetadata)) {
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
                         long ledgerId, LedgerMetadata metadata,
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
    public void onChanged(long lid, LedgerMetadata newMetadata) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received ledger metadata update on {} : {}", lid, newMetadata);
        }
        if (this.ledgerId != lid) {
            return;
        }
        if (null == newMetadata) {
            return;
        }
        LedgerMetadata currentMetadata = getLedgerMetadata();
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

    List<BookieSocketAddress> replaceBookiesInEnsemble(LedgerMetadata metadata,
                                                       List<BookieSocketAddress> oldEnsemble,
                                                       Map<Integer, BookieSocketAddress> failedBookies)
            throws BKException.BKNotEnoughBookiesException {
        List<BookieSocketAddress> newEnsemble = new ArrayList<>(oldEnsemble);

        int ensembleSize = metadata.getEnsembleSize();
        int writeQ = metadata.getWriteQuorumSize();
        int ackQ = metadata.getAckQuorumSize();
        Map<String, byte[]> customMetadata = metadata.getCustomMetadata();

        Set<BookieSocketAddress> exclude = new HashSet<>(failedBookies.values());

        int replaced = 0;
        for (Map.Entry<Integer, BookieSocketAddress> entry : failedBookies.entrySet()) {
            int idx = entry.getKey();
            BookieSocketAddress addr = entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("[EnsembleChange-L{}] replacing bookie: {} index: {}", getId(), addr, idx);
            }

            if (!newEnsemble.get(idx).equals(addr)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[EnsembleChange-L{}] Not changing failed bookie {} at index {}, already changed to {}",
                              getId(), addr, idx, newEnsemble.get(idx));
                }
                continue;
            }
            try {
                BookieSocketAddress newBookie = clientCtx.getBookieWatcher().replaceBookie(
                        ensembleSize, writeQ, ackQ, customMetadata, newEnsemble, idx, exclude);
                newEnsemble.set(idx, newBookie);

                replaced++;
            } catch (BKException.BKNotEnoughBookiesException e) {
                // if there is no bookie replaced, we throw not enough bookie exception
                if (replaced <= 0) {
                    throw e;
                } else {
                    break;
                }
            }
        }
        return newEnsemble;
    }

    private static Set<Integer> diffEnsemble(List<BookieSocketAddress> e1,
                                             List<BookieSocketAddress> e2) {
        checkArgument(e1.size() == e2.size(), "Ensembles must be of same size");
        Set<Integer> diff = new HashSet<>();
        for (int i = 0; i < e1.size(); i++) {
            if (!e1.get(i).equals(e2.get(i))) {
                diff.add(i);
            }
        }
        return diff;
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
        blockAddCompletions.incrementAndGet();

        // handleBookieFailure should always run in the ordered executor thread for this
        // ledger, so this synchronized should be unnecessary, but putting it here now
        // just in case (can be removed when we validate threads)
        synchronized (metadataLock) {
            long lac = getLastAddConfirmed();
            LedgerMetadata metadata = getLedgerMetadata();
            List<BookieSocketAddress> currentEnsemble = getCurrentEnsemble();
            try {
                List<BookieSocketAddress> newEnsemble = replaceBookiesInEnsemble(metadata, currentEnsemble,
                                                                                 failedBookies);

                Set<Integer> replaced = diffEnsemble(currentEnsemble, newEnsemble);
                blockAddCompletions.decrementAndGet();
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
            (metadata) -> !(metadata.isClosed() || metadata.isInRecovery());
        if (forceRecovery) {
            // in the force recovery case, we want to update the metadata
            // to IN_RECOVERY, even if the ledger is already closed
            needsUpdate = (metadata) -> !metadata.isInRecovery();
        }
        new MetadataUpdateLoop(
                clientCtx.getLedgerManager(), getId(),
                this::getLedgerMetadata,
                needsUpdate,
                (metadata) -> LedgerMetadataBuilder.from(metadata).withInRecoveryState().build(),
                this::setLedgerMetadata)
            .run()
            .thenCompose((metadata) -> {
                    if (metadata.isClosed()) {
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

    CompletableFuture<LedgerMetadata> closeRecovered() {
        long lac, len;
        synchronized (this) {
            lac = lastAddConfirmed;
            len = length;
        }
        LOG.info("Closing recovered ledger {} at entry {}", getId(), lac);
        CompletableFuture<LedgerMetadata> f = new MetadataUpdateLoop(
                clientCtx.getLedgerManager(), getId(),
                this::getLedgerMetadata,
                (metadata) -> metadata.isInRecovery(),
                (metadata) -> {
                    LedgerMetadataBuilder builder = LedgerMetadataBuilder.from(metadata);
                    Optional<Long> lastEnsembleKey = metadata.getLastEnsembleKey();
                    checkState(lastEnsembleKey.isPresent(),
                               "Metadata shouldn't have been created without at least one ensemble");
                    synchronized (metadataLock) {
                        newEnsemblesFromRecovery.entrySet().forEach(
                                (e) -> {
                                    checkState(e.getKey() >= lastEnsembleKey.get(),
                                               "Once a ledger is in recovery, noone can add ensembles without closing");
                                    // Occurs when a bookie need to be replaced at very start of recovery
                                    if (lastEnsembleKey.get().equals(e.getKey())) {
                                        builder.replaceEnsembleEntry(e.getKey(), e.getValue());
                                    } else {
                                        builder.newEnsembleEntry(e.getKey(), e.getValue());
                                    }
                                });
                    }
                    return builder.closingAt(lac, len).build();
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
