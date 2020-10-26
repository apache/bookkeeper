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

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException;

/**
 * Interface for marking ledgers which need to be rereplicated.
 */
public interface LedgerUnderreplicationManager extends AutoCloseable {

    /**
     * Mark a ledger as underreplicated. The replication should
     * then check which fragments are underreplicated and rereplicate them
     */
    default void markLedgerUnderreplicated(long ledgerId, String missingReplica) throws ReplicationException {
        FutureUtils.result(
            markLedgerUnderreplicatedAsync(
                ledgerId, Lists.newArrayList(missingReplica)), ReplicationException.EXCEPTION_HANDLER);
    }

    /**
     * Mark a ledger as underreplicated with missing bookies. The replication should then
     * check which fragements are underreplicated and rereplicate them.
     *
     * @param ledgerId ledger id
     * @param missingReplicas missing replicas
     * @return a future presents the mark result.
     */
    CompletableFuture<Void> markLedgerUnderreplicatedAsync(long ledgerId, Collection<String> missingReplicas);

    /**
     * Mark a ledger as fully replicated. If the ledger is not
     * already marked as underreplicated, this is a noop.
     */
    void markLedgerReplicated(long ledgerId)
            throws ReplicationException.UnavailableException;

    /**
     * Get the UnderreplicatedLedger info if this ledger is marked
     * underreplicated otherwise it returns null.
     *
     * @param ledgerId
     *            ledger id
     * @return the UnderreplicatedLedger info instance if this ledger is marked
     *         underreplicated otherwise it returns null.
     * @throws ReplicationException.UnavailableException
     */
    UnderreplicatedLedger getLedgerUnreplicationInfo(long ledgerId) throws ReplicationException.UnavailableException;

    /**
     * Get a list of all the underreplicated ledgers which have been
     * marked for rereplication, filtered by the predicate on the missing replicas list.
     *
     * <p>Missing replicas list of an underreplicated ledger is the list of the bookies which are part of
     * the ensemble of this ledger and are currently unavailable/down.
     *
     * @param predicate filter to use while listing under replicated ledgers. 'null' if filtering is not required
     * @return an iterator which returns underreplicated ledgers.
     */
    Iterator<UnderreplicatedLedger> listLedgersToRereplicate(Predicate<List<String>> predicate);

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
     * Release a previously acquired ledger. This allows others to acquire the ledger.
     */
    void releaseUnderreplicatedLedger(long ledgerId)
            throws ReplicationException.UnavailableException;

    /**
     * Release all resources held by the ledger underreplication manager.
     */
    @Override
    void close()
            throws ReplicationException.UnavailableException;

    /**
     * Stop ledger replication. Currently running ledger rereplication tasks
     * will be continued and will be stopped from next task. This will block
     * ledger replication {@link #Auditor} and {@link #getLedgerToRereplicate()}
     * tasks.
     */
    void disableLedgerReplication()
            throws ReplicationException.UnavailableException;

    /**
     * Resuming ledger replication. This will allow ledger replication
     * {@link #Auditor} and {@link #getLedgerToRereplicate()} tasks to continue.
     */
    void enableLedgerReplication()
            throws ReplicationException.UnavailableException;

    /**
     * Check whether the ledger replication is enabled or not. This will return
     * true if the ledger replication is enabled, otherwise return false.
     *
     * @return - return true if it is enabled otherwise return false
     */
    boolean isLedgerReplicationEnabled()
            throws ReplicationException.UnavailableException;

    /**
     * Receive notification asynchronously when the ledger replication process
     * is enabled.
     *
     * @param cb
     *            - callback implementation to receive the notification
     */
    void notifyLedgerReplicationEnabled(GenericCallback<Void> cb)
            throws ReplicationException.UnavailableException;

    /**
     * Creates the zNode for lostBookieRecoveryDelay with the specified value and returns true.
     * If the node is already existing, then it returns false.
     *
     * @param lostBookieRecoveryDelay
     * @return
     *      true if it succeeds in creating zNode for lostBookieRecoveryDelay, false if it is already existing
     * @throws ReplicationException.UnavailableException
     */
    boolean initializeLostBookieRecoveryDelay(int lostBookieRecoveryDelay)
            throws ReplicationException.UnavailableException;

    /**
     * Setter for the lostBookieRecoveryDelay znode.
     *
     * @param lostBookieRecoveryDelay
     * @throws ReplicationException.UnavailableException
     */
    void setLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws ReplicationException.UnavailableException;

    /**
     * Getter for the lostBookieRecoveryDelay.
     *
     * @return the int value of lostBookieRecoveryDelay
     * @throws ReplicationException.UnavailableException
     */
    int getLostBookieRecoveryDelay() throws ReplicationException.UnavailableException;

    /**
     * Setter for the CheckAllLedgers last executed ctime.
     *
     * @param checkAllLedgersCTime
     * @throws ReplicationException.UnavailableException
     */
    void setCheckAllLedgersCTime(long checkAllLedgersCTime) throws ReplicationException.UnavailableException;

    /**
     * Getter for the CheckAllLedgers last executed ctime.
     *
     * @return the long value of checkAllLedgersCTime
     * @throws ReplicationException.UnavailableException
     */
    long getCheckAllLedgersCTime() throws ReplicationException.UnavailableException;

    /**
     * Setter for the PlacementPolicyCheck last executed ctime.
     *
     * @param placementPolicyCheckCTime
     * @throws ReplicationException.UnavailableException
     */
    void setPlacementPolicyCheckCTime(long placementPolicyCheckCTime) throws ReplicationException.UnavailableException;

    /**
     * Getter for the PlacementPolicyCheck last executed ctime.
     *
     * @return the long value of placementPolicyCheckCTime
     * @throws ReplicationException.UnavailableException
     */
    long getPlacementPolicyCheckCTime() throws ReplicationException.UnavailableException;

    /**
     * Setter for the ReplicasCheck last executed ctime.
     *
     * @param replicasCheckCTime
     * @throws ReplicationException.UnavailableException
     */
    void setReplicasCheckCTime(long replicasCheckCTime) throws ReplicationException.UnavailableException;

    /**
     * Getter for the ReplicasCheck last executed ctime.
     *
     * @return the long value of replicasCheckCTime
     * @throws ReplicationException.UnavailableException
     */
    long getReplicasCheckCTime() throws ReplicationException.UnavailableException;

    /**
     * Receive notification asynchronously when the lostBookieRecoveryDelay value is Changed.
     *
     * @param cb
     * @throws ReplicationException.UnavailableException
     */
    void notifyLostBookieRecoveryDelayChanged(GenericCallback<Void> cb)
            throws ReplicationException.UnavailableException;

    /**
     * If a replicationworker has acquired lock on an underreplicated ledger,
     * then getReplicationWorkerIdRereplicatingLedger should return
     * ReplicationWorkerId (BookieId) of the ReplicationWorker that is holding
     * lock. If lock for the underreplicated ledger is not yet acquired or if it
     * is released then it is supposed to return null.
     *
     * @param ledgerId
     * @return
     * @throws ReplicationException.UnavailableException
     */
    String getReplicationWorkerIdRereplicatingLedger(long ledgerId) throws ReplicationException.UnavailableException;
}
