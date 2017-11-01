/**
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
package org.apache.bookkeeper.replication;

import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieThread;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.BKException.BKLedgerRecoveryException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BKException.BKReadException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.replication.ReplicationStats.BK_CLIENT_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATE_EXCEPTION;
import static org.apache.bookkeeper.replication.ReplicationStats.REREPLICATE_OP;

/**
 * ReplicationWorker will take the fragments one by one from
 * ZKLedgerUnderreplicationManager and replicates to it.
 */
public class ReplicationWorker implements Runnable {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReplicationWorker.class);
    private final LedgerUnderreplicationManager underreplicationManager;
    private final ServerConfiguration conf;
    private final ZooKeeper zkc;
    private volatile boolean workerRunning = false;
    private final BookKeeperAdmin admin;
    private final LedgerChecker ledgerChecker;
    private final BookKeeper bkc;
    private final Thread workerThread;
    private final long rwRereplicateBackoffMs;
    private final long openLedgerRereplicationGracePeriod;
    private final Timer pendingReplicationTimer;

    // Expose Stats
    private final StatsLogger statsLogger;
    private final OpStatsLogger rereplicateOpStats;
    private final Counter numLedgersReplicated;
    private final Map<String,Counter> exceptionCounters;

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     *
     * @param zkc
     *            - ZK instance
     * @param conf
     *            - configurations
     */
    public ReplicationWorker(final ZooKeeper zkc,
                             final ServerConfiguration conf)
            throws CompatibilityException, KeeperException,
            InterruptedException, IOException {
        this(zkc, conf, NullStatsLogger.INSTANCE);
    }

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     *
     * @param zkc
     *            - ZK instance
     * @param conf
     *            - configurations
     * @param statsLogger
     *            - stats logger
     */
    public ReplicationWorker(final ZooKeeper zkc,
                             final ServerConfiguration conf,
                             StatsLogger statsLogger)
            throws CompatibilityException, KeeperException,
            InterruptedException, IOException {
        this.zkc = zkc;
        this.conf = conf;
        LedgerManagerFactory mFactory = LedgerManagerFactory
                .newLedgerManagerFactory(this.conf, this.zkc);
        this.underreplicationManager = mFactory
                .newLedgerUnderreplicationManager();
        try {
            this.bkc = BookKeeper.forConfig(new ClientConfiguration(conf))
                    .zk(zkc)
                    .statsLogger(statsLogger.scope(BK_CLIENT_SCOPE))
                    .build();
        } catch (BKException e) {
            throw new IOException("Failed to instantiate replication worker", e);
        }
        this.admin = new BookKeeperAdmin(bkc, statsLogger);
        this.ledgerChecker = new LedgerChecker(bkc);
        this.workerThread = new BookieThread(this, "ReplicationWorker");
        this.openLedgerRereplicationGracePeriod = conf
                .getOpenLedgerRereplicationGracePeriod();
        this.rwRereplicateBackoffMs = conf.getRwRereplicateBackoffMs();
        this.pendingReplicationTimer = new Timer("PendingReplicationTimer");

        // Expose Stats
        this.statsLogger = statsLogger;
        this.rereplicateOpStats = this.statsLogger.getOpStatsLogger(REREPLICATE_OP);
        this.numLedgersReplicated = this.statsLogger.getCounter(NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED);
        this.exceptionCounters = new HashMap<String, Counter>();
    }

    /** Start the replication worker */
    public void start() {
        this.workerThread.start();
    }

    @Override
    public void run() {
        workerRunning = true;
        while (workerRunning) {
            try {
                rereplicate();
            } catch (InterruptedException e) {
                LOG.info("InterruptedException "
                        + "while replicating fragments", e);
                shutdown();
                Thread.currentThread().interrupt();
                return;
            } catch (BKException e) {
                LOG.error("BKException while replicating fragments", e);
                waitBackOffTime(rwRereplicateBackoffMs);
            } catch (UnavailableException e) {
                LOG.error("UnavailableException "
                        + "while replicating fragments", e);
                waitBackOffTime(rwRereplicateBackoffMs);
            }
        }
        LOG.info("ReplicationWorker exited loop!");
    }

    private static void waitBackOffTime(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Replicates the under replicated fragments from failed bookie ledger to
     * targetBookie
     */
    private void rereplicate() throws InterruptedException, BKException,
            UnavailableException {
        long ledgerIdToReplicate = underreplicationManager
                .getLedgerToRereplicate();

        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            success = rereplicate(ledgerIdToReplicate);
        } finally {
            long latencyMillis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            if (success) {
                rereplicateOpStats.registerSuccessfulEvent(latencyMillis, TimeUnit.MILLISECONDS);
            } else {
                rereplicateOpStats.registerFailedEvent(latencyMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    private boolean rereplicate(long ledgerIdToReplicate) throws InterruptedException, BKException,
            UnavailableException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Going to replicate the fragments of the ledger: {}", ledgerIdToReplicate);
        }
        try (LedgerHandle lh = admin.openLedgerNoRecovery(ledgerIdToReplicate)) {
            Set<LedgerFragment> fragmentsBeforeReplicate = getUnderreplicatedFragments(lh);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Founds fragments {} for replication from ledger: {}",
                    fragmentsBeforeReplicate, ledgerIdToReplicate);
            }

            boolean foundOpenFragments = false;
            long numFragsReplicated = 0;
            for (LedgerFragment ledgerFragment : fragmentsBeforeReplicate) {
                if (!ledgerFragment.isClosed()) {
                    foundOpenFragments = true;
                    continue;
                }
                LOG.info("Going to replicate the fragments of the ledger: {}", ledgerIdToReplicate);
                admin.replicateLedgerFragment(lh, ledgerFragment);
                numFragsReplicated++;
            }

            if (numFragsReplicated > 0) {
                numLedgersReplicated.inc();
            }

            if (foundOpenFragments || isLastSegmentOpenAndMissingBookies(lh)) {
                deferLedgerLockRelease(ledgerIdToReplicate);
                return false;
            }

            Set<LedgerFragment> fragmentsAfterReplicate = getUnderreplicatedFragments(lh);
            if (fragmentsAfterReplicate.size() == 0) {
                LOG.info("Ledger {} is replicated successfully.", ledgerIdToReplicate);
                underreplicationManager.markLedgerReplicated(ledgerIdToReplicate);
                return true;
            } else {
                LOG.info("Fail to replicate ledger {}.", ledgerIdToReplicate);
                // Releasing the underReplication ledger lock and compete
                // for the replication again for the pending fragments
                underreplicationManager
                        .releaseUnderreplicatedLedger(ledgerIdToReplicate);
                return false;
            }
        } catch (BKNoSuchLedgerExistsException e) {
            // Ledger might have been deleted by user
            LOG.info("BKNoSuchLedgerExistsException while opening "
                    + "ledger {} for replication. Other clients "
                    + "might have deleted the ledger. "
                    + "So, no harm to continue", ledgerIdToReplicate);
            underreplicationManager.markLedgerReplicated(ledgerIdToReplicate);
            getExceptionCounter("BKNoSuchLedgerExistsException").inc();
            return false;
        } catch (BKNotEnoughBookiesException e) {
            logBKExceptionAndReleaseLedger(e, ledgerIdToReplicate);
            throw e;
        } catch (BKException e) {
            logBKExceptionAndReleaseLedger(e, ledgerIdToReplicate);
            return false;
        }
    }

    private void logBKExceptionAndReleaseLedger(BKException e, long ledgerIdToReplicate)
            throws UnavailableException {
        LOG.info("{} while"
                + " rereplicating ledger {}."
                + " Enough Bookies might not have available"
                + " So, no harm to continue",
            e.getClass().getSimpleName(),
            ledgerIdToReplicate);
        underreplicationManager
                .releaseUnderreplicatedLedger(ledgerIdToReplicate);
        getExceptionCounter(e.getClass().getSimpleName()).inc();
    }

    /**
     * When checking the fragments of a ledger, there is a corner case
     * where if the last segment/ensemble is open, but nothing has been written to
     * some of the quorums in the ensemble, bookies can fail without any action being
     * taken. This is fine, until enough bookies fail to cause a quorum to become
     * unavailable, by which time the ledger is unrecoverable.
     *
     * For example, if in a E3Q2, only 1 entry is written and the last bookie
     * in the ensemble fails, nothing has been written to it, so nothing needs to be
     * recovered. But if the second to last bookie fails, we've now lost quorum for
     * the second entry, so it's impossible to see if the second has been written or
     * not.
     *
     * To avoid this situation, we need to check if bookies in the final open ensemble
     * are unavailable, and take action if so. The action to take is to close the ledger,
     * after a grace period as the writting client may replace the faulty bookie on its
     * own.
     *
     * Missing bookies in closed ledgers are fine, as we know the last confirmed add, so
     * we can tell which entries are supposed to exist and rereplicate them if necessary.
     */
    private boolean isLastSegmentOpenAndMissingBookies(LedgerHandle lh) throws BKException {
        LedgerMetadata md = admin.getLedgerMetadata(lh);
        if (md.isClosed()) {
            return false;
        }

        SortedMap<Long, ArrayList<BookieSocketAddress>> ensembles
            = admin.getLedgerMetadata(lh).getEnsembles();
        ArrayList<BookieSocketAddress> finalEnsemble = ensembles.get(ensembles.lastKey());
        Collection<BookieSocketAddress> available = admin.getAvailableBookies();
        for (BookieSocketAddress b : finalEnsemble) {
            if (!available.contains(b)) {
                return true;
            }
        }
        return false;
    }

    /** Gets the under replicated fragments */
    private Set<LedgerFragment> getUnderreplicatedFragments(LedgerHandle lh)
            throws InterruptedException {
        CheckerCallback checkerCb = new CheckerCallback();
        ledgerChecker.checkLedger(lh, checkerCb);
        Set<LedgerFragment> fragments = checkerCb.waitAndGetResult();
        return fragments;
    }

    /**
     * Schedules a timer task for releasing the lock which will be scheduled
     * after open ledger fragment replication time. Ledger will be fenced if it
     * is still in open state when timer task fired
     */
    private void deferLedgerLockRelease(final long ledgerId) {
        long gracePeriod = this.openLedgerRereplicationGracePeriod;
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                LedgerHandle lh = null;
                try {
                    lh = admin.openLedgerNoRecovery(ledgerId);
                    if (isLastSegmentOpenAndMissingBookies(lh)) {
                        lh = admin.openLedger(ledgerId);
                    }

                    Set<LedgerFragment> fragments = getUnderreplicatedFragments(lh);
                    for (LedgerFragment fragment : fragments) {
                        if (!fragment.isClosed()) {
                            lh = admin.openLedger(ledgerId);
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("InterruptedException "
                            + "while replicating fragments", e);
                } catch (BKNoSuchLedgerExistsException bknsle) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ledger was deleted, safe to continue", bknsle);
                    }
                } catch (BKException e) {
                    LOG.error("BKException while fencing the ledger"
                            + " for rereplication of postponed ledgers", e);
                } finally {
                    try {
                        if (lh != null) {
                            lh.close();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("InterruptedException while closing "
                                + "ledger", e);
                    } catch (BKException e) {
                        // Lets go ahead and release the lock. Catch actual
                        // exception in normal replication flow and take
                        // action.
                        LOG.warn("BKException while closing ledger ", e);
                    } finally {
                        try {
                            underreplicationManager
                                    .releaseUnderreplicatedLedger(ledgerId);
                        } catch (UnavailableException e) {
                            LOG.error("UnavailableException "
                                    + "while replicating fragments", e);
                            shutdown();
                        }
                    }
                }
            }
        };
        pendingReplicationTimer.schedule(timerTask, gracePeriod);
    }

    /**
     * Stop the replication worker service
     */
    public void shutdown() {
        LOG.info("Shutting down replication worker");

        synchronized (this) {
            if (!workerRunning) {
                return;
            }
            workerRunning = false;
        }
        LOG.info("Shutting down ReplicationWorker");
        this.pendingReplicationTimer.cancel();
        try {
            this.workerThread.interrupt();
            this.workerThread.join();
        } catch (InterruptedException e) {
            LOG.error("Interrupted during shutting down replication worker : ",
                    e);
            Thread.currentThread().interrupt();
        }
        try {
            bkc.close();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while closing the Bookie client", e);
            Thread.currentThread().interrupt();
        } catch (BKException e) {
            LOG.warn("Exception while closing the Bookie client", e);
        }
        try {
            underreplicationManager.close();
        } catch (UnavailableException e) {
            LOG.warn("Exception while closing the "
                    + "ZkLedgerUnderrepliationManager", e);
        }
    }

    /**
     * Gives the running status of ReplicationWorker
     */
    boolean isRunning() {
        return workerRunning && workerThread.isAlive();
    }

    /** Ledger checker call back */
    private static class CheckerCallback implements
            GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }

        /**
         * Wait until operation complete call back comes and return the ledger
         * fragments set
         */
        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }
    }

    private Counter getExceptionCounter(String name) {
        Counter counter = this.exceptionCounters.get(name);
        if (counter == null) {
            counter = this.statsLogger.scope(REPLICATE_EXCEPTION).getCounter(name);
            this.exceptionCounters.put(name, counter);
        }
        return counter;
    }

}
