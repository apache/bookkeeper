/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException;

/**
 * Interface for marking ledgers which need to be rereplicated
 */
public interface LedgerUnderreplicationManager {
    /**
     * Mark a ledger as underreplicated. The replication should
     * then check which fragments are underreplicated and rereplicate them
     */
    void markLedgerUnderreplicated(long ledgerId, String missingReplica)
            throws ReplicationException.UnavailableException;

    /**
     * Mark a ledger as fully replicated. If the ledger is not
     * already marked as underreplicated, this is a noop.
     */
    void markLedgerReplicated(long ledgerId)
            throws ReplicationException.UnavailableException;

    /**
     * Acquire a underreplicated ledger for rereplication. The ledger
     * should be locked, so that no other agent will receive the ledger
     * from this call.
     * The ledger should remain locked until either #markLedgerComplete
     * or #releaseLedger are called.
     * This call is blocking, so will not return until a ledger is
     * available for rereplication.
     */
    long getLedgerToRereplicate()
            throws ReplicationException.UnavailableException;

    /**
     * Poll for a underreplicated ledger to rereplicate.
     * @see #getLedgerToRereplicate
     * @return the ledgerId, or -1 if none are available
     */
    long pollLedgerToRereplicate()
            throws ReplicationException.UnavailableException;


    /**
     * Release a previously acquired ledger. This allows others to acquire
     * the ledger
     */
    void releaseUnderreplicatedLedger(long ledgerId)
            throws ReplicationException.UnavailableException;

    /**
     * Release all resources held by the ledger underreplication manager
     */
    void close()
            throws ReplicationException.UnavailableException;

    /**
     * Stop ledger replication. Currently running ledger rereplication tasks
     * will be continued and will be stopped from next task. This will block
     * ledger replication {@link #Auditor} and {@link #getLedgerToRereplicate()}
     * tasks
     */
    void disableLedgerReplication()
            throws ReplicationException.UnavailableException;

    /**
     * Resuming ledger replication. This will allow ledger replication
     * {@link #Auditor} and {@link #getLedgerToRereplicate()} tasks to continue
     */
    void enableLedgerReplication()
            throws ReplicationException.UnavailableException;

    /**
     * Check whether the ledger replication is enabled or not. This will return
     * true if the ledger replication is enabled, otherwise return false
     * 
     * @return - return true if it is enabled otherwise return false
     */
    boolean isLedgerReplicationEnabled()
            throws ReplicationException.UnavailableException;

    /**
     * Receive notification asynchronously when the ledger replication process
     * is enabled
     * 
     * @param cb
     *            - callback implementation to receive the notification
     */
    void notifyLedgerReplicationEnabled(GenericCallback<Void> cb)
            throws ReplicationException.UnavailableException;
}
