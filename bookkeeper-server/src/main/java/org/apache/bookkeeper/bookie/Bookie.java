/*
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
 */
package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;

/**
 * Interface for the bookie.
 */
public interface Bookie {

    void start();
    void join() throws InterruptedException;
    boolean isRunning();
    int getExitCode();
    int shutdown();

    boolean isAvailableForHighPriorityWrites();
    boolean isReadOnly();

    // TODO: replace callback with futures
    // TODO: replace ackBeforeSync with flags
    void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException, InterruptedException;
    void recoveryAddEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException, InterruptedException;
    void forceLedger(long ledgerId, WriteCallback cb, Object ctx);
    void setExplicitLac(ByteBuf entry, WriteCallback writeCallback, Object ctx, byte[] masterKey)
            throws IOException, InterruptedException, BookieException;
    ByteBuf getExplicitLac(long ledgerId) throws IOException, NoLedgerException;

    // these can probably be moved out and called directly on ledgerdirmanager
    long getTotalDiskSpace() throws IOException;
    long getTotalFreeSpace() throws IOException;

    // TODO: Shouldn't this be async?
    ByteBuf readEntry(long ledgerId, long entryId)
            throws IOException, NoLedgerException;
    long readLastAddConfirmed(long ledgerId) throws IOException;
    PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException, NoLedgerException;

    /**
     * Fences a ledger. From this point on, clients will be unable to
     * write to this ledger. Only recoveryAddEntry will be
     * able to add entries to the ledger.
     * This method is idempotent. Once a ledger is fenced, it can
     * never be unfenced. Fencing a fenced ledger has no effect.
     * @return
     */
    CompletableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey)
            throws IOException, BookieException;

    // TODO: Replace Watcher with a completableFuture (cancellable)
    boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                          long previousLAC,
                                          Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException;
    void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                             Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException;

    // TODO: StateManager should be passed as a parameter to Bookie
    StateManager getStateManager();

    // TODO: Should be constructed and passed in as a parameter
    LedgerStorage getLedgerStorage();

    // TODO: Move this exceptions somewhere else
    /**
     * Exception is thrown when no such a ledger is found in this bookie.
     */
    class NoLedgerException extends IOException {
        private static final long serialVersionUID = 1L;
        private final long ledgerId;
        public NoLedgerException(long ledgerId) {
            super("Ledger " + ledgerId + " not found");
            this.ledgerId = ledgerId;
        }
        public long getLedgerId() {
            return ledgerId;
        }
    }

    /**
     * Exception is thrown when no such an entry is found in this bookie.
     */
    class NoEntryException extends IOException {
        private static final long serialVersionUID = 1L;
        private final long ledgerId;
        private final long entryId;
        public NoEntryException(long ledgerId, long entryId) {
            this("Entry " + entryId + " not found in " + ledgerId, ledgerId, entryId);
        }

        public NoEntryException(String msg, long ledgerId, long entryId) {
            super(msg);
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        public long getLedger() {
            return ledgerId;
        }
        public long getEntry() {
            return entryId;
        }
    }

}