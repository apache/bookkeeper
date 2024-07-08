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

import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Interface for storing ledger data on persistent storage.
 */
public interface LedgerStorage {

    /**
     * Initialize the LedgerStorage implementation.
     *
     * @param conf
     * @param ledgerManager
     * @param ledgerDirsManager
     */
    void initialize(ServerConfiguration conf,
                    LedgerManager ledgerManager,
                    LedgerDirsManager ledgerDirsManager,
                    LedgerDirsManager indexDirsManager,
                    StatsLogger statsLogger,
                    ByteBufAllocator allocator)
            throws IOException;

    void setStateManager(StateManager stateManager);
    void setCheckpointSource(CheckpointSource checkpointSource);
    void setCheckpointer(Checkpointer checkpointer);

    /**
     * Start any background threads belonging to the storage system. For example, garbage collection.
     */
    void start();

    /**
     * Cleanup and free any resources being used by the storage system.
     */
    void shutdown() throws InterruptedException;

    /**
     * Whether a ledger exists.
     */
    boolean ledgerExists(long ledgerId) throws IOException;

    /**
     * Whether an entry exists.
     */
    boolean entryExists(long ledgerId, long entryId) throws IOException, BookieException;

    /**
     * Fenced the ledger id in ledger storage.
     *
     * @param ledgerId Ledger Id.
     * @throws IOException when failed to fence the ledger.
     */
    boolean setFenced(long ledgerId) throws IOException;

    /**
     * Check whether the ledger is fenced in ledger storage or not.
     *
     * @param ledgerId Ledger ID.
     * @throws IOException
     */
    boolean isFenced(long ledgerId) throws IOException, BookieException;

    /**
     * Set a ledger to limbo state.
     * When a ledger is in limbo state, we cannot answer any requests about it.
     * For example, if a client asks for an entry, we cannot say we don't have it because
     * it may have been written to us in the past, but we are waiting for data integrity checks
     * to copy it over.
     */
    void setLimboState(long ledgerId) throws IOException;

    /**
     * Check whether a ledger is in limbo state.
     * @see #setLimboState(long)
     */
    boolean hasLimboState(long ledgerId) throws IOException;

    /**
     * Clear the limbo state of a ledger.
     * @see #setLimboState(long)
     */
    void clearLimboState(long ledgerId) throws IOException;

    /**
     * Set the master key for a ledger.
     */
    void setMasterKey(long ledgerId, byte[] masterKey) throws IOException;

    /**
     * Get the master key for a ledger.
     *
     * @throws IOException if there is an error reading the from the ledger
     * @throws BookieException if no such ledger exists
     */
    byte[] readMasterKey(long ledgerId) throws IOException, BookieException;

    /**
     * Add an entry to the storage.
     *
     * @return the entry id of the entry added
     */
    long addEntry(ByteBuf entry) throws IOException, BookieException;

    /**
     * Read an entry from storage.
     */
    ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException;

    /**
     * Get last add confirmed.
     *
     * @param ledgerId ledger id.
     * @return last add confirmed.
     * @throws IOException
     */
    long getLastAddConfirmed(long ledgerId) throws IOException, BookieException;

    /**
     * Wait for last add confirmed update.
     *
     * @param previousLAC - The threshold beyond which we would wait for the update
     * @param watcher  - Watcher to notify on update
     * @return
     * @throws IOException
     */
    boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                          long previousLAC,
                                          Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException;

    /**
     * Cancel a previous wait for last add confirmed update.
     *
     * @param ledgerId The ledger being watched.
     * @param watcher The watcher to cancel.
     * @throws IOException
     */
    void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException;

    /**
     * Flushes all data in the storage. Once this is called,
     * add data written to the LedgerStorage up until this point
     * has been persisted to perminant storage
     */
    void flush() throws IOException;

    /**
     * Ask the ledger storage to sync data until the given <i>checkpoint</i>.
     * The ledger storage implementation do checkpoint and return the real checkpoint
     * that it finished. The returned the checkpoint indicates that all entries added
     * before that point already persist.
     *
     * @param checkpoint Check Point that {@link Checkpoint} proposed.
     * @throws IOException
     */
    void checkpoint(Checkpoint checkpoint) throws IOException;

    /**
     * @param ledgerId
     * @throws IOException
     */
    void deleteLedger(long ledgerId) throws IOException;

    /**
     * Signals that a ledger is deleted by the garbage collection thread.
     */
    interface LedgerDeletionListener {
        void ledgerDeleted(long ledgerId);
    }

    /**
     * Register a listener for ledgers deletion notifications.
     *
     * @param listener object that will be notified every time a ledger is deleted
     */
    void registerLedgerDeletionListener(LedgerDeletionListener listener);

    void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException;

    ByteBuf getExplicitLac(long ledgerId) throws IOException, BookieException;

    // for testability
    default LedgerStorage getUnderlyingLedgerStorage() {
        return this;
    }

    /**
     * Force trigger Garbage Collection.
     */
    default void forceGC() {
        return;
    }

    /**
     * Force trigger Garbage Collection with forceMajor or forceMinor parameter.
     */
    default void forceGC(boolean forceMajor, boolean forceMinor) {
        return;
    }

    default void suspendMinorGC() {
        return;
    }

    default void suspendMajorGC() {
        return;
    }

    default void resumeMinorGC() {
        return;
    }

    default void resumeMajorGC() {
        return;
    }

    default boolean isMajorGcSuspended() {
        return false;
    }

    default boolean isMinorGcSuspended() {
        return false;
    }

    default void entryLocationCompact() {
        return;
    }

    default void entryLocationCompact(List<String> locations) {
        return;
    }

    default boolean isEntryLocationCompacting() {
        return false;
    }

    default Map<String, Boolean> isEntryLocationCompacting(List<String> locations) {
        return Collections.emptyMap();
    }

    default List<String> getEntryLocationDBPath() {
        return Collections.emptyList();
    }

    /**
     * Class for describing location of a generic inconsistency.  Implementations should
     * ensure that detail is populated with an exception which adequately describes the
     * nature of the problem.
     */
    class DetectedInconsistency {
        private long ledgerId;
        private long entryId;
        private Exception detail;

        DetectedInconsistency(long ledgerId, long entryId, Exception detail) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.detail = detail;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public long getEntryId() {
            return entryId;
        }

        public Exception getException() {
            return detail;
        }
    }

    /**
     * Performs internal check of local storage logging any inconsistencies.
     * @param rateLimiter Provide to rate of entry checking.  null for unlimited.
     * @return List of inconsistencies detected
     * @throws IOException
     */
    default List<DetectedInconsistency> localConsistencyCheck(Optional<RateLimiter> rateLimiter) throws IOException {
        return new ArrayList<>();
    }

    /**
     * Whether force triggered Garbage Collection is running or not.
     *
     * @return
     *      true  -- force triggered Garbage Collection is running,
     *      false -- force triggered Garbage Collection is not running
     */
    default boolean isInForceGC() {
        return false;
    }


    /**
     * Get Garbage Collection status.
     * Since DbLedgerStorage is a list of storage instances, we should return a list.
     */
    default List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return Collections.emptyList();
    }

    /**
     * Returns the primitive long iterator for entries of the ledger, stored in
     * this LedgerStorage. The returned iterator provide weakly consistent state
     * of the ledger. It is guaranteed that entries of the ledger added to this
     * LedgerStorage by the time this method is called will be available but
     * modifications made after method invocation may not be available.
     *
     * @param ledgerId
     *            - id of the ledger
     * @return the list of entries of the ledger available in this
     *         ledgerstorage.
     * @throws Exception
     */
    PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException;

    /**
     * Get the storage state flags currently set for the storage instance.
     */
    EnumSet<StorageState> getStorageStateFlags() throws IOException;

    /**
     * Set a storage state flag for the storage instance.
     * Implementations must ensure this method is atomic, and the flag
     * is persisted to storage when the method returns.
     */
    void setStorageStateFlag(StorageState flags) throws IOException;

    /**
     * Clear a storage state flag for the storage instance.
     * Implementations must ensure this method is atomic, and the flag
     * is persisted to storage when the method returns.
     */
    void clearStorageStateFlag(StorageState flags) throws IOException;

    /**
     * StorageState flags.
     */
    enum StorageState {
        NEEDS_INTEGRITY_CHECK
    }
}
