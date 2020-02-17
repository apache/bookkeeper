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
package org.apache.distributedlog.bk;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Transaction.OpListener;
import org.apache.distributedlog.util.Utils;
import org.apache.distributedlog.zk.ZKTransaction;
import org.apache.distributedlog.zk.ZKVersionedSetOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allocator to allocate ledgers.
 */
public class SimpleLedgerAllocator implements LedgerAllocator, FutureEventListener<LedgerHandle>, OpListener<Version> {

    static final Logger LOG = LoggerFactory.getLogger(SimpleLedgerAllocator.class);

    enum Phase {
        ALLOCATING, ALLOCATED, HANDING_OVER, HANDED_OVER, ERROR
    }

    static class AllocationException extends IOException {

        private static final long serialVersionUID = -1111397872059426882L;

        private final Phase phase;

        public AllocationException(Phase phase, String msg) {
            super(msg);
            this.phase = phase;
        }

        public Phase getPhase() {
            return this.phase;
        }

    }

    static class ConcurrentObtainException extends AllocationException {

        private static final long serialVersionUID = -8532471098537176913L;

        public ConcurrentObtainException(Phase phase, String msg) {
            super(phase, msg);
        }
    }

    // zookeeper client
    final ZooKeeperClient zkc;
    // bookkeeper client
    final BookKeeperClient bkc;
    // znode path
    final String allocatePath;
    // allocation phase
    Phase phase = Phase.HANDED_OVER;
    // version
    LongVersion version = new LongVersion(-1);
    // outstanding allocation
    CompletableFuture<LedgerHandle> allocatePromise;
    // outstanding tryObtain transaction
    Transaction<Object> tryObtainTxn = null;
    OpListener<LedgerHandle> tryObtainListener = null;
    // ledger id left from previous allocation
    Long ledgerIdLeftFromPrevAllocation = null;
    // Allocated Ledger
    LedgerHandle allocatedLh = null;

    LedgerMetadata ledgerMetadata;

    CompletableFuture<Void> closeFuture = null;
    final LinkedList<CompletableFuture<Void>> ledgerDeletions =
            new LinkedList<CompletableFuture<Void>>();

    // Ledger configuration
    private final QuorumConfigProvider quorumConfigProvider;

    static CompletableFuture<Versioned<byte[]>> getAndCreateAllocationData(final String allocatePath,
                                                                final ZooKeeperClient zkc) {
        return Utils.zkGetData(zkc, allocatePath, false)
                .thenCompose(new Function<Versioned<byte[]>, CompletionStage<Versioned<byte[]>>>() {
            @Override
            public CompletableFuture<Versioned<byte[]>> apply(Versioned<byte[]> result) {
                if (null != result && null != result.getVersion() && null != result.getValue()) {
                    return FutureUtils.value(result);
                }
                return createAllocationData(allocatePath, zkc);
            }
        });
    }

    private static CompletableFuture<Versioned<byte[]>> createAllocationData(final String allocatePath,
                                                                  final ZooKeeperClient zkc) {
        try {
            final CompletableFuture<Versioned<byte[]>> promise = new CompletableFuture<Versioned<byte[]>>();
            zkc.get().create(allocatePath, DistributedLogConstants.EMPTY_BYTES,
                    zkc.getDefaultACL(), CreateMode.PERSISTENT,
                    (rc, path, ctx, name) -> {
                        if (KeeperException.Code.OK.intValue() == rc) {
                            // Since the z-node was just created, we are sure at this point the version is 0
                            promise.complete(new Versioned<byte[]>(DistributedLogConstants.EMPTY_BYTES,
                                    new LongVersion(0)));
                        } else if (KeeperException.Code.NODEEXISTS.intValue() == rc) {
                            FutureUtils.proxyTo(
                              Utils.zkGetData(zkc, allocatePath, false),
                              promise
                            );
                        } else {
                            promise.completeExceptionally(Utils.zkException(
                                    KeeperException.create(KeeperException.Code.get(rc)), allocatePath));
                        }
                    }, null);
            return promise;
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            return FutureUtils.exception(Utils.zkException(e, allocatePath));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return FutureUtils.exception(Utils.zkException(e, allocatePath));
        }
    }

    public static CompletableFuture<SimpleLedgerAllocator> of(final String allocatePath,
                                                   final Versioned<byte[]> allocationData,
                                                   final QuorumConfigProvider quorumConfigProvider,
                                                   final ZooKeeperClient zkc,
                                                   final BookKeeperClient bkc) {
        return SimpleLedgerAllocator.of(allocatePath, allocationData, quorumConfigProvider, zkc, bkc, null);
    }

    public static CompletableFuture<SimpleLedgerAllocator> of(final String allocatePath,
                                                   final Versioned<byte[]> allocationData,
                                                   final QuorumConfigProvider quorumConfigProvider,
                                                   final ZooKeeperClient zkc,
                                                   final BookKeeperClient bkc,
                                                   final LedgerMetadata ledgerMetadata) {
        if (null != allocationData && null != allocationData.getValue()
                && null != allocationData.getVersion()) {
            return FutureUtils.value(new SimpleLedgerAllocator(allocatePath, allocationData,
                    quorumConfigProvider, zkc, bkc, ledgerMetadata));
        }
        return getAndCreateAllocationData(allocatePath, zkc)
            .thenApply(allocationData1 -> new SimpleLedgerAllocator(allocatePath, allocationData1,
                        quorumConfigProvider, zkc, bkc, ledgerMetadata));
    }

    /**
     * Construct a ledger allocator.
     *
     * @param allocatePath
     *          znode path to store the allocated ledger.
     * @param allocationData
     *          allocation data.
     * @param quorumConfigProvider
     *          Quorum configuration provider.
     * @param zkc
     *          zookeeper client.
     * @param bkc
     *          bookkeeper client.
     */
    public SimpleLedgerAllocator(String allocatePath,
                                 Versioned<byte[]> allocationData,
                                 QuorumConfigProvider quorumConfigProvider,
                                 ZooKeeperClient zkc,
                                 BookKeeperClient bkc) {
        this(allocatePath, allocationData, quorumConfigProvider, zkc, bkc, null);
    }

    /**
     * Construct a ledger allocator.
     *
     * @param allocatePath
     *          znode path to store the allocated ledger.
     * @param allocationData
     *          allocation data.
     * @param quorumConfigProvider
     *          Quorum configuration provider.
     * @param zkc
     *          zookeeper client.
     * @param bkc
     *          bookkeeper client.
     * @param ledgerMetadata
     *          metadata to attach to allocated ledgers
     */
    public SimpleLedgerAllocator(String allocatePath,
                                 Versioned<byte[]> allocationData,
                                 QuorumConfigProvider quorumConfigProvider,
                                 ZooKeeperClient zkc,
                                 BookKeeperClient bkc,
                                 LedgerMetadata ledgerMetadata) {
        this.zkc = zkc;
        this.bkc = bkc;
        this.allocatePath = allocatePath;
        this.quorumConfigProvider = quorumConfigProvider;
        this.ledgerMetadata = ledgerMetadata;
        initialize(allocationData);
    }

    /**
     * Initialize the allocator.
     *
     * @param allocationData
     *          Allocation Data.
     */
    private void initialize(Versioned<byte[]> allocationData) {
        setVersion((LongVersion) allocationData.getVersion());
        byte[] data = allocationData.getValue();
        if (null != data && data.length > 0) {
            // delete the allocated ledger since this is left by last allocation.
            try {
                ledgerIdLeftFromPrevAllocation = DLUtils.bytes2LogSegmentId(data);
            } catch (NumberFormatException nfe) {
                LOG.warn("Invalid data found in allocator path {} : ", allocatePath, nfe);
            }
        }

    }

    private synchronized void deleteLedgerLeftFromPreviousAllocationIfNecessary() {
        if (null != ledgerIdLeftFromPrevAllocation) {
            LOG.info("Deleting allocated-but-unused ledger left from previous allocation {}.",
                    ledgerIdLeftFromPrevAllocation);
            deleteLedger(ledgerIdLeftFromPrevAllocation);
            ledgerIdLeftFromPrevAllocation = null;
        }
    }

    @Override
    public synchronized void allocate() throws IOException {
        if (Phase.ERROR == phase) {
            throw new AllocationException(Phase.ERROR, "Error on ledger allocator for " + allocatePath);
        }
        if (Phase.HANDED_OVER == phase) {
            // issue an allocate request when ledger is already handed over.
            allocateLedger(ledgerMetadata);
        }
    }

    @Override
    public synchronized CompletableFuture<LedgerHandle> tryObtain(final Transaction<Object> txn,
                                                                  final OpListener<LedgerHandle> listener) {
        if (Phase.ERROR == phase) {
            return FutureUtils.exception(new AllocationException(Phase.ERROR,
                    "Error on allocating ledger under " + allocatePath));
        }
        if (Phase.HANDING_OVER == phase || Phase.HANDED_OVER == phase || null != tryObtainTxn) {
            return FutureUtils.exception(new ConcurrentObtainException(phase,
                    "Ledger handle is handling over to another thread : " + phase));
        }
        tryObtainTxn = txn;
        tryObtainListener = listener;
        if (null != allocatedLh) {
            completeAllocation(allocatedLh);
        }
        return allocatePromise;
    }

    @Override
    public void onCommit(Version r) {
        confirmObtain((LongVersion) r);
    }

    private void confirmObtain(LongVersion zkVersion) {
        boolean shouldAllocate = false;
        OpListener<LedgerHandle> listenerToNotify = null;
        LedgerHandle lhToNotify = null;
        synchronized (this) {
            if (Phase.HANDING_OVER == phase) {
                setPhase(Phase.HANDED_OVER);
                setVersion(zkVersion);
                listenerToNotify = tryObtainListener;
                lhToNotify = allocatedLh;
                // reset the state
                allocatedLh = null;
                allocatePromise = null;
                tryObtainTxn = null;
                tryObtainListener = null;
                // mark flag to issue an allocation request
                shouldAllocate = true;
            }
        }
        if (null != listenerToNotify && null != lhToNotify) {
            // notify the listener
            listenerToNotify.onCommit(lhToNotify);
        }
        if (shouldAllocate) {
            // issue an allocation request
            allocateLedger();
        }
    }

    @Override
    public void onAbort(Throwable t) {
        OpListener<LedgerHandle> listenerToNotify;
        synchronized (this) {
            listenerToNotify = tryObtainListener;
            if (t instanceof KeeperException
                    && ((KeeperException) t).code() == KeeperException.Code.BADVERSION) {
                LOG.info("Set ledger allocator {} to ERROR state after hit bad version : version = {}",
                        allocatePath, getVersion());
                setPhase(Phase.ERROR);
            } else {
                if (Phase.HANDING_OVER == phase) {
                    setPhase(Phase.ALLOCATED);
                    tryObtainTxn = null;
                    tryObtainListener = null;
                }
            }
        }
        if (null != listenerToNotify) {
            listenerToNotify.onAbort(t);
        }
    }

    private synchronized void setPhase(Phase phase) {
        this.phase = phase;
        LOG.info("Ledger allocator {} moved to phase {} : version = {}.",
            allocatePath, phase, version);
    }

    private synchronized void allocateLedger() {
        allocateLedger(null);
    }

    private synchronized void allocateLedger(LedgerMetadata ledgerMetadata) {
        // make sure previous allocation is already handed over.
        if (Phase.HANDED_OVER != phase) {
            LOG.error("Trying allocate ledger for {} in phase {}, giving up.", allocatePath, phase);
            return;
        }
        setPhase(Phase.ALLOCATING);
        allocatePromise = new CompletableFuture<LedgerHandle>();
        QuorumConfig quorumConfig = quorumConfigProvider.getQuorumConfig();
        bkc.createLedger(
                quorumConfig.getEnsembleSize(),
                quorumConfig.getWriteQuorumSize(),
                quorumConfig.getAckQuorumSize(),
                ledgerMetadata
        ).whenComplete(this);
    }

    private synchronized void completeAllocation(LedgerHandle lh) {
        allocatedLh = lh;
        if (null == tryObtainTxn) {
            return;
        }
        org.apache.zookeeper.Op zkSetDataOp = org.apache.zookeeper.Op.setData(
                allocatePath, DistributedLogConstants.EMPTY_BYTES, (int) version.getLongVersion());
        ZKVersionedSetOp commitOp = new ZKVersionedSetOp(zkSetDataOp, this);
        tryObtainTxn.addOp(commitOp);
        setPhase(Phase.HANDING_OVER);
        allocatePromise.complete(lh);
    }

    private synchronized void failAllocation(Throwable cause) {
        allocatePromise.completeExceptionally(cause);
    }

    @Override
    public void onSuccess(LedgerHandle lh) {
        // a ledger is created, update the ledger to allocation path before handling it over for usage.
        markAsAllocated(lh);
    }

    @Override
    public void onFailure(Throwable cause) {
        LOG.error("Error creating ledger for allocating {} : ", allocatePath, cause);
        setPhase(Phase.ERROR);
        failAllocation(cause);
    }

    private synchronized LongVersion getVersion() {
        return version;
    }

    private synchronized void setVersion(LongVersion newVersion) {
        Version.Occurred occurred = newVersion.compare(version);
        if (occurred == Version.Occurred.AFTER) {
            LOG.info("Ledger allocator for {} moved version from {} to {}.",
                allocatePath, version, newVersion);
            version = newVersion;
        } else {
            LOG.warn("Ledger allocator for {} received an old version {}, current version is {}.",
                allocatePath, newVersion, version);
        }
    }

    private void markAsAllocated(final LedgerHandle lh) {
        byte[] data = DLUtils.logSegmentId2Bytes(lh.getId());
        Utils.zkSetData(zkc, allocatePath, data, getVersion())
            .whenComplete(new FutureEventListener<LongVersion>() {
                @Override
                public void onSuccess(LongVersion version) {
                    // we only issue deleting ledger left from previous allocation when we could allocate first ledger
                    // as zookeeper version could prevent us doing stupid things.
                    deleteLedgerLeftFromPreviousAllocationIfNecessary();
                    setVersion(version);
                    setPhase(Phase.ALLOCATED);
                    // complete the allocation after it is marked as allocated
                    completeAllocation(lh);
                }

                @Override
                public void onFailure(Throwable cause) {
                    setPhase(Phase.ERROR);
                    deleteLedger(lh.getId());
                    LOG.error("Fail mark ledger {} as allocated under {} : ",
                        lh.getId(), allocatePath, cause);
                    // fail the allocation since failed to mark it as allocated
                    failAllocation(cause);
                }
            });
    }

    void deleteLedger(final long ledgerId) {
        final CompletableFuture<Void> deleteFuture = bkc.deleteLedger(ledgerId, true);
        synchronized (ledgerDeletions) {
            ledgerDeletions.add(deleteFuture);
        }
        deleteFuture.whenComplete((value, cause) -> {
            if (null != cause) {
                LOG.error("Error deleting ledger {} for ledger allocator {}, retrying : ",
                    ledgerId, allocatePath, cause);
                if (!isClosing()) {
                    deleteLedger(ledgerId);
                }
            }
            synchronized (ledgerDeletions) {
                ledgerDeletions.remove(deleteFuture);
            }
        });
    }

    private synchronized boolean isClosing() {
        return closeFuture != null;
    }

    private CompletableFuture<Void> closeInternal(boolean cleanup) {
        CompletableFuture<Void> closePromise;
        synchronized (this) {
            if (null != closeFuture) {
                return closeFuture;
            }
            closePromise = new CompletableFuture<Void>();
            closeFuture = closePromise;
        }
        if (!cleanup) {
            LOG.info("Abort ledger allocator without cleaning up on {}.", allocatePath);
            FutureUtils.complete(closePromise, null);
            return closePromise;
        }
        cleanupAndClose(closePromise);
        return closePromise;
    }

    private void cleanupAndClose(final CompletableFuture<Void> closePromise) {
        LOG.info("Closing ledger allocator on {}.", allocatePath);
        final ZKTransaction txn = new ZKTransaction(zkc);
        // try obtain ledger handle
        tryObtain(txn, new OpListener<LedgerHandle>() {
            @Override
            public void onCommit(LedgerHandle r) {
                // no-op
                complete();
            }

            @Override
            public void onAbort(Throwable t) {
                // no-op
                complete();
            }

            private void complete() {
                closePromise.complete(null);
                LOG.info("Closed ledger allocator on {}.", allocatePath);
            }
        }).whenComplete(new FutureEventListener<LedgerHandle>() {
            @Override
            public void onSuccess(LedgerHandle lh) {
                // try obtain succeed
                // if we could obtain the ledger handle, we have the responsibility to close it
                deleteLedger(lh.getId());
                // wait for deletion to be completed
                List<CompletableFuture<Void>> outstandingDeletions;
                synchronized (ledgerDeletions) {
                    outstandingDeletions = Lists.newArrayList(ledgerDeletions);
                }
                FutureUtils.collect(outstandingDeletions).whenComplete(new FutureEventListener<List<Void>>() {
                    @Override
                    public void onSuccess(List<Void> values) {
                        txn.execute();
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        LOG.debug("Fail to obtain the allocated ledger handle when closing the allocator : ", cause);
                        FutureUtils.complete(closePromise, null);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                LOG.debug("Fail to obtain the allocated ledger handle when closing the allocator : ", cause);
                FutureUtils.complete(closePromise, null);
            }
        });

    }

    @Override
    public void start() {
        // nop
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return closeInternal(false);
    }

    @Override
    public CompletableFuture<Void> delete() {
        return closeInternal(true).thenCompose(value -> Utils.zkDelete(zkc, allocatePath, getVersion()));
    }

}
