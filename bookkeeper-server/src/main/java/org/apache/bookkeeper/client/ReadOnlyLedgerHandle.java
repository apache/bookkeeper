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
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Version;

/**
 * Read only ledger handle. This ledger handle allows you to
 * read from a ledger but not to write to it. It overrides all
 * the public write operations from LedgerHandle.
 * It should be returned for BookKeeper#openLedger operations.
 */
class ReadOnlyLedgerHandle extends LedgerHandle implements LedgerMetadataListener {

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
    void handleBookieFailure(final Map<Integer, BookieSocketAddress> failedBookies) {
        blockAddCompletions.incrementAndGet();
        synchronized (getLedgerMetadata()) {
            try {
                EnsembleInfo ensembleInfo = replaceBookieInMetadata(failedBookies,
                        numEnsembleChanges.incrementAndGet());
                if (ensembleInfo.replacedBookies.isEmpty()) {
                    blockAddCompletions.decrementAndGet();
                    return;
                }
                blockAddCompletions.decrementAndGet();
                // the failed bookie has been replaced
                unsetSuccessAndSendWriteRequest(ensembleInfo.newEnsemble, ensembleInfo.replacedBookies);
            } catch (BKException.BKNotEnoughBookiesException e) {
                LOG.error("Could not get additional bookie to "
                          + "remake ensemble, closing ledger: " + ledgerId);
                handleUnrecoverableErrorDuringAdd(e.getCode());
                return;
            }
        }
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
}
