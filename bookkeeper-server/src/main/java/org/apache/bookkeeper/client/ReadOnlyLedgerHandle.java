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
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
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

        final LedgerMetadata m;

        MetadataUpdater(LedgerMetadata metadata) {
            this.m = metadata;
        }

        @Override
        public void safeRun() {
            Version.Occurred occurred =
                    ReadOnlyLedgerHandle.this.metadata.getVersion().compare(this.m.getVersion());
            if (Version.Occurred.BEFORE == occurred) {
                LOG.info("Updated ledger metadata for ledger {} to {}.", ledgerId, this.m);
                synchronized (ReadOnlyLedgerHandle.this) {
                    if (this.m.isClosed()) {
                            ReadOnlyLedgerHandle.this.lastAddConfirmed = this.m.getLastEntryId();
                            ReadOnlyLedgerHandle.this.length = this.m.getLength();
                    }
                    ReadOnlyLedgerHandle.this.metadata = this.m;
                }
            }
        }

        @Override
        public String toString() {
            return String.format("MetadataUpdater(%d)", ledgerId);
        }
    }

    ReadOnlyLedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
                         DigestType digestType, byte[] password, boolean watch)
            throws GeneralSecurityException, NumberFormatException {
        super(bk, ledgerId, metadata, digestType, password, EnumSet.noneOf(WriteFlag.class));
        if (watch) {
            bk.getLedgerManager().registerLedgerMetadataListener(ledgerId, this);
        }
    }

    @Override
    public void close()
            throws InterruptedException, BKException {
        bk.getLedgerManager().unregisterLedgerMetadataListener(ledgerId, this);
    }

    @Override
    public void asyncClose(CloseCallback cb, Object ctx) {
        bk.getLedgerManager().unregisterLedgerMetadataListener(ledgerId, this);
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
        synchronized (metadata) {
            try {
                EnsembleInfo ensembleInfo = replaceBookieInMetadata(failedBookies,
                        numEnsembleChanges.incrementAndGet());
                if (ensembleInfo.replacedBookies.isEmpty()) {
                    blockAddCompletions.decrementAndGet();
                    return;
                }
                blockAddCompletions.decrementAndGet();
                // the failed bookie has been replaced
                unsetSuccessAndSendWriteRequest(ensembleInfo.replacedBookies);
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
        Version.Occurred occurred =
                this.metadata.getVersion().compare(newMetadata.getVersion());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to update metadata from {} to {} : {}",
                    this.metadata, newMetadata, occurred);
        }
        if (Version.Occurred.BEFORE == occurred) { // the metadata is updated
            try {
                bk.getMainWorkerPool().submitOrdered(ledgerId, new MetadataUpdater(newMetadata));
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
    protected void initializeExplicitLacFlushPolicy() {
        explicitLacFlushPolicy = ExplicitLacFlushPolicy.VOID_EXPLICITLAC_FLUSH_POLICY;
    }
}
