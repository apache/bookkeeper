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

package org.apache.bookkeeper.bookie.datainteg;

enum Events {
    /**
     * Data integrity service has started
     * It runs at an interval to check if a full integrity check is needed,
     * and if so runs it.
     */
    DATA_INTEG_SERVICE_START,
    /**
     * Data integrity service has been stopped.
     */
    DATA_INTEG_SERVICE_STOP,
    /**
     * An exception was thrown on the data integrity service executor
     * and never caught. This is a programmer error and should be reported
     * as such.
     */
    DATA_INTEG_SERVICE_UNCAUGHT_ERROR,
    /**
     * Data integrity service thread interrupted.
     * This is non-fatal and indicates that the bookie is shutting down.
     * The full check will resume once the bookie is started again.
     */
    DATA_INTEG_SERVICE_INTERRUPTED,
    /**
     * An error occurred in the in the data integrity service loop.
     * This normally indicates that an error occurred in the full check.
     * The full check will be tried again.
     * It could also indicate an error checking the NEEDS_INTEGRITY_CHECK
     * flag, which indicates disk issues.
     */
    DATA_INTEG_SERVICE_ERROR,

    /**
     * Mark a ledger as in-limbo. In limbo ledgers are ledgers for whose
     * entries we cannot safely answer queries positively or negatively.
     * These are ledgers which have not been closed and where this bookie
     * appears in the final ensemble.

     * We may have had an entry in the past, but due to disk failures or
     * configuration changes it may not currently exist locally. However,
     * we cannot tell clients that the entry doesn't exist, because the client
     * would understand that to mean that it never existed, and this would
     * break consistency in the ledger recovery protocol.

     * For limbo ledgers, all entry level queries should throw an exception.

     * We also mark the ledger as fenced at this point, as it may have been set
     * on this ledger previously. This means no more writes for this ledger
     * can come to this bookie.
     */
    MARK_LIMBO,

    /**
     * An error occurred marking the ledger as fenced or as in-limbo.
     * The most likely cause is a bad disk.
     * This is a fatal error, as we cannot safely serve entries if we cannot
     * set limbo and fence flags.
     */
    LIMBO_OR_FENCE_ERROR,

    /**
     * Start the preboot check. The preboot check runs when some configuration
     * has changed regarding the disk configuration. This may be simply a disk
     * being added, or it could be the disks being wiped. The preboot check
     * needs to check which ledgers we are supposed to store according to
     * ledger metadata. Any unclosed ledgers which contain this bookie in its last
     * ensemble must be marked as in-limbo, as we don't know if entries from that
     * ledger have previously existed on this bookie.

     * The preboot check doesn't copy any data. That is left up to the full check
     * which can run in the background while the bookie is serving data for non-limbo
     * ledgers.

     * The preboot check has a runId associated which can be used to pull together
     * all the events from the same run.
     * The preboot check will set the NEEDS_INTEGRITY_CHECK flag on storage to
     * trigger a full check after the bookie has booted.
     */
    PREBOOT_START,
    /**
     * The preboot check has completed successfully. The event contains the number
     * of ledgers that have been processed.
     */
    PREBOOT_END,
    /**
     * An error occurred during the preboot check. This is a fatal error as we cannot
     * safely serve data if the correct ledgers have not been marked as in-limbo. The
     * error could be due to problems accessing the metadata store, or due to disk
     * issues.
     */
    PREBOOT_ERROR,
    /**
     * Preboot found an invalid ledger metadata. All ledger metadata must have at least
     * one ensemble but the process found one with none.
     */
    INVALID_METADATA,
    /**
     * Preboot must create a ledger that the bookie does not have but that metadata says
     * the bookie should have. This can happen due to things like ensemble changes and
     * when a ledger is closed. If the ledger cannot be created on the bookie then
     * this error will cause preboot to fail.
     */
    ENSURE_LEDGER_ERROR,
    /**
     * Initialized the full check. If we have cached metadata from a previous run, or
     * the preboot check, then we use that. Otherwise we read the metadata from the
     * metadata store.

     * The full check goes through each ledger for which this bookie is supposed to
     * store entries and checks that these entries exist on the bookie. If they do not
     * exist, they are copied from another bookie.

     * Each full check has a runId associated which can be used to find all events from
     * the check.
     */
    FULL_CHECK_INIT,
    /**
     * The full check has completed.
     */
    FULL_CHECK_COMPLETE,
    /**
     * Start iterating through the ledger that should be on this bookie.
     * The event is annotated with the number of ledgers which will be checked,
     * which may be fewer that the total number of ledgers on the bookie as
     * a previous run may have verified that some ledgers are ok and don't need
     * to be checked.
     */
    FULL_CHECK_START,
    /**
     * The full check has completed. This can be an info event or an error event.
     * The event is annotated with the number of ledgers which were checked and found
     * to be ok, the number that were found to be missing and the number for which
     * errors occurred during the check. The missing ledgers have been deleted on
     * the cluster, so don't need to be processed again. If there is a non-zero of
     * ledgers with errors, the whole event is an error.

     * An error for this event is non-fatal. Any ledgers which finished with error
     * will be processed again the next time the full check runs. The full check
     * continues retrying until there are no errors.
     */
    FULL_CHECK_END,
    /**
     * An error occurred during the full check, but not while processing ledgers.
     * This error could occur while flushing the ledger storage or clearing the
     * full check flag.
     */
    FULL_CHECK_ERROR,

    /**
     * The full check will use cached metadata.
     */
    USE_CACHED_METADATA,
    /**
     * The full check will read the metadata from the metadata store.
     */
    REFRESH_METADATA,

    /**
     * The NEEDS_INTEGRITY_CHECK will be cleared from the ledger storage.
     * This signifies that the ledger storage contains everything it should
     * and the full check does not need to be retried, even after reboot.
     */
    CLEAR_INTEGCHECK_FLAG,

    /**
     * An error occurred while clearing the limbo flag for a ledger.
     * This is generally a disk error. This error is non-fatal and the operation
     * will be tried again on the next full check.
     */
    CLEAR_LIMBO_ERROR,
    /**
     * Recover a ledger that has been marked as in limbo. This runs the ledger
     * recovery algorithm to find the last entry of the ledger and mark the ledger
     * as closed. As the ledger is marked as in-limbo locally, the current bookie
     * not take part in the recovery process apart from initializing it.

     * Once recovery completes successfully, the limbo flag can be cleared for the
     * ledger.
     */
    RECOVER_LIMBO_LEDGER,
    /**
     * The ledger has been deleted from the ledger metadata store, so we don't need
     * to continue any processing on it.
     */
    RECOVER_LIMBO_LEDGER_MISSING,
    /**
     * An error occurred during recovery. This could be due to not having enough
     * bookies available to recover the ledger.
     * The error is non-fatal. The recovery will be tried again on the next run of
     * ledger recovery.
     */
    RECOVER_LIMBO_LEDGER_ERROR,
    /**
     * An error occurred when trying to close the ledger handle of a recovered ledger.
     * This shouldn't happen, as closing a recovered ledger should not involve any I/O.
     * This error is non-fatal and the event is registered for informational purposes
     * only.
     */
    RECOVER_LIMBO_LEDGER_CLOSE_ERROR,

    /**
     * Start checking whether the entries for a ledger exist locally, and copying them
     * if they do not.
     */
    LEDGER_CHECK_AND_COPY_START,
    /**
     * Checking and copying has completed for a ledger. If any entry failed to copy
     * this is a warning event. The ledger will be retried on the next run of the full
     * check.
     * This event is annotated with the number of entries copied, the number of errors
     * and the total number of bytes copied for the ledger.
     */
    LEDGER_CHECK_AND_COPY_END
}
