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

package org.apache.bookkeeper.bookie;

import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Arrays;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.common.util.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a ledger inside a bookie. In particular, it implements operations
 * to write entries to a ledger and read entries from a ledger.
 */
public class LedgerDescriptorImpl extends LedgerDescriptor {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerDescriptorImpl.class);
    final LedgerStorage ledgerStorage;
    private long ledgerId;
    final byte[] masterKey;

    private AtomicBoolean fenceEntryPersisted = new AtomicBoolean();
    private SettableFuture<Boolean> logFenceResult = null;

    LedgerDescriptorImpl(byte[] masterKey,
                         long ledgerId,
                         LedgerStorage ledgerStorage) {
        this.masterKey = masterKey;
        this.ledgerId = ledgerId;
        this.ledgerStorage = ledgerStorage;
    }

    @Override
    void checkAccess(byte[] masterKey) throws BookieException, IOException {
        if (!Arrays.equals(this.masterKey, masterKey)) {
            LOG.error("[{}] Requested master key {} does not match the cached master key {}",
                    this.ledgerId, Arrays.toString(masterKey), Arrays.toString(this.masterKey));
            throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
        }
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    boolean setFenced() throws IOException {
        return ledgerStorage.setFenced(ledgerId);
    }

    @Override
    boolean isFenced() throws IOException {
        return ledgerStorage.isFenced(ledgerId);
    }

    @Override
    void setExplicitLac(ByteBuf lac) throws IOException {
        ledgerStorage.setExplicitlac(ledgerId, lac);
    }

    @Override
    ByteBuf getExplicitLac() {
        return ledgerStorage.getExplicitLac(ledgerId);
    }

    @Override
    synchronized SettableFuture<Boolean> fenceAndLogInJournal(Journal journal) throws IOException {
        boolean success = this.setFenced();
        if (success) {
            // fenced for first time, we should add the key to journal ensure we can rebuild.
            return logFenceEntryInJournal(journal);
        } else {
            // If we reach here, it means the fence state in FileInfo has been set (may not be persisted yet).
            // However, writing the fence log entry to the journal might still be in progress. This can happen
            // when a bookie receives two fence requests almost at the same time. The subsequent logic is used
            // to check the fencing progress.
            if (logFenceResult == null || fenceEntryPersisted.get()){
                // Either ledger's fenced state is recovered from Journal
                // Or Log fence entry in Journal succeed
                SettableFuture<Boolean> result = SettableFuture.create();
                result.set(true);
                return result;
            } else if (logFenceResult.isDone()) {
                // We failed to log fence entry in Journal, try again.
                return logFenceEntryInJournal(journal);
            }
            // Fencing is in progress
            return logFenceResult;
        }
    }

    /**
     * Log the fence ledger entry in Journal so that we can rebuild the state.
     * @param journal log the fence entry in the Journal
     * @return A future which will be satisfied when add entry to journal complete
     */
    private SettableFuture<Boolean> logFenceEntryInJournal(Journal journal) {
        SettableFuture<Boolean> result;
        synchronized (this) {
            result = logFenceResult = SettableFuture.create();
        }
        ByteBuf entry = createLedgerFenceEntry(ledgerId);
        try {
            journal.logAddEntry(entry, false /* ackBeforeSync */, (rc, ledgerId, entryId, addr, ctx) -> {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Record fenced state for ledger {} in journal with rc {}",
                            ledgerId, BKException.codeLogger(rc));
                }
                if (rc == 0) {
                    fenceEntryPersisted.compareAndSet(false, true);
                    result.set(true);
                } else {
                    result.set(false);
                }
            }, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.setException(e);
        }
        return result;
    }

    @Override
    long addEntry(ByteBuf entry) throws IOException, BookieException {
        long ledgerId = entry.getLong(entry.readerIndex());

        if (ledgerId != this.ledgerId) {
            throw new IOException("Entry for ledger " + ledgerId + " was sent to " + this.ledgerId);
        }

        return ledgerStorage.addEntry(entry);
    }

    @Override
    ByteBuf readEntry(long entryId) throws IOException {
        return ledgerStorage.getEntry(ledgerId, entryId);
    }

    @Override
    long getLastAddConfirmed() throws IOException {
        return ledgerStorage.getLastAddConfirmed(ledgerId);
    }

    @Override
    boolean waitForLastAddConfirmedUpdate(long previousLAC,
                                          Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        return ledgerStorage.waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    void cancelWaitForLastAddConfirmedUpdate(Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        ledgerStorage.cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
    }

    @Override
    OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        return ledgerStorage.getListOfEntriesOfLedger(ledgerId);
    }
}
