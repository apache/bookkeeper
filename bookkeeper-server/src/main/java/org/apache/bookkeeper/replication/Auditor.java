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
package org.apache.bookkeeper.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auditor is a single entity in the entire Bookie cluster and will be watching
 * all the bookies under 'ledgerrootpath/available' zkpath. When any of the
 * bookie failed or disconnected from zk, he will start initiating the
 * re-replication activities by keeping all the corresponding ledgers of the
 * failed bookie as underreplicated znode in zk.
 *
 * <p>TODO: eliminate the direct usage of zookeeper here {@link https://github.com/apache/bookkeeper/issues/1332}
 */
public class Auditor implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Auditor.class);
    private final ServerConfiguration conf;
    private final BookKeeper bkc;
    private final boolean ownBkc;
    private final BookKeeperAdmin admin;
    private final boolean ownAdmin;
    private BookieLedgerIndexer bookieLedgerIndexer;
    private LedgerManager ledgerManager;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private final ScheduledExecutorService executor;
    private List<String> knownBookies = new ArrayList<String>();
    private final String bookieIdentifier;
    protected volatile Future<?> auditTask;
    private final Set<String> bookiesToBeAudited = Sets.newHashSet();
    private volatile int lostBookieRecoveryDelayBeforeChange;
    protected AuditorBookieCheckTask auditorBookieCheckTask;
    protected AuditorTask auditorCheckAllLedgersTask;
    protected AuditorTask auditorPlacementPolicyCheckTask;
    protected AuditorTask auditorReplicasCheckTask;
    private final List<AuditorTask> allAuditorTasks = Lists.newArrayList();

    private final AuditorStats auditorStats;

    static BookKeeper createBookKeeperClient(ServerConfiguration conf) throws InterruptedException, IOException {
        return createBookKeeperClient(conf, NullStatsLogger.INSTANCE);
    }

    static BookKeeper createBookKeeperClient(ServerConfiguration conf, StatsLogger statsLogger)
            throws InterruptedException, IOException {
        ClientConfiguration clientConfiguration = new ClientConfiguration(conf);
        clientConfiguration.setClientRole(ClientConfiguration.CLIENT_ROLE_SYSTEM);
        try {
            return BookKeeper.forConfig(clientConfiguration).statsLogger(statsLogger).build();
        } catch (BKException e) {
            throw new IOException("Failed to create bookkeeper client", e);
        }
    }

    static BookKeeper createBookKeeperClientThrowUnavailableException(ServerConfiguration conf)
            throws UnavailableException {
        try {
            return createBookKeeperClient(conf);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnavailableException("Failed to create bookkeeper client", e);
        } catch (IOException e) {
            throw new UnavailableException("Failed to create bookkeeper client", e);
        }
    }

    public Auditor(final String bookieIdentifier,
                   ServerConfiguration conf,
                   StatsLogger statsLogger)
            throws UnavailableException {
        this(
                bookieIdentifier,
                conf,
                createBookKeeperClientThrowUnavailableException(conf),
                true,
                statsLogger);
    }

    public Auditor(final String bookieIdentifier,
                   ServerConfiguration conf,
                   BookKeeper bkc,
                   boolean ownBkc,
                   StatsLogger statsLogger)
            throws UnavailableException {
        this(bookieIdentifier,
                conf,
                bkc,
                ownBkc,
                new BookKeeperAdmin(bkc, statsLogger, new ClientConfiguration(conf)),
                true,
                statsLogger);
    }

    public Auditor(final String bookieIdentifier,
                   ServerConfiguration conf,
                   BookKeeper bkc,
                   boolean ownBkc,
                   BookKeeperAdmin admin,
                   boolean ownAdmin,
                   StatsLogger statsLogger)
            throws UnavailableException {
        this.conf = conf;
        this.bookieIdentifier = bookieIdentifier;
        this.auditorStats = new AuditorStats(statsLogger);

        this.bkc = bkc;
        this.ownBkc = ownBkc;
        this.admin = admin;
        this.ownAdmin = ownAdmin;
        initialize(conf, bkc);

        AuditorTask.ShutdownTaskHandler shutdownTaskHandler = this::submitShutdownTask;
        BiConsumer<Void, Throwable> submitBookieCheckTask = (ignore, throwable) -> this.submitBookieCheckTask();
        BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask = (flag, throwable) -> flag.set(auditTask != null);
        this.auditorBookieCheckTask = new AuditorBookieCheckTask(
                conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler,
                bookieLedgerIndexer, hasAuditCheckTask, submitBookieCheckTask);
        allAuditorTasks.add(auditorBookieCheckTask);
        this.auditorCheckAllLedgersTask = new AuditorCheckAllLedgersTask(
                conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);
        allAuditorTasks.add(auditorCheckAllLedgersTask);
        this.auditorPlacementPolicyCheckTask = new AuditorPlacementPolicyCheckTask(
                conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);
        allAuditorTasks.add(auditorPlacementPolicyCheckTask);
        this.auditorReplicasCheckTask = new AuditorReplicasCheckTask(
                conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);
        allAuditorTasks.add(auditorReplicasCheckTask);
        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "AuditorBookie-" + bookieIdentifier);
                t.setDaemon(true);
                return t;
            }
        });
    }

    private void initialize(ServerConfiguration conf, BookKeeper bkc)
            throws UnavailableException {
        try {
            LedgerManagerFactory ledgerManagerFactory = bkc.getLedgerManagerFactory();
            ledgerManager = ledgerManagerFactory.newLedgerManager();
            this.bookieLedgerIndexer = new BookieLedgerIndexer(ledgerManager);

            this.ledgerUnderreplicationManager = ledgerManagerFactory
                    .newLedgerUnderreplicationManager();
            LOG.info("AuthProvider used by the Auditor is {}",
                    admin.getConf().getClientAuthProviderFactoryClass());
            if (this.ledgerUnderreplicationManager
                    .initializeLostBookieRecoveryDelay(conf.getLostBookieRecoveryDelay())) {
                LOG.info("Initializing lostBookieRecoveryDelay zNode to the conf value: {}",
                        conf.getLostBookieRecoveryDelay());
            } else {
                LOG.info("Valid lostBookieRecoveryDelay zNode is available, so not creating "
                        + "lostBookieRecoveryDelay zNode as part of Auditor initialization ");
            }
            lostBookieRecoveryDelayBeforeChange = this.ledgerUnderreplicationManager.getLostBookieRecoveryDelay();
        } catch (CompatibilityException ce) {
            throw new UnavailableException(
                    "CompatibilityException while initializing Auditor", ce);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new UnavailableException(
                    "Interrupted while initializing Auditor", ie);
        }
    }

    private void submitShutdownTask() {
        synchronized (this) {
            LOG.info("Executing submitShutdownTask");
            if (executor.isShutdown()) {
                LOG.info("executor is already shutdown");
                return;
            }
            executor.submit(() -> {
                synchronized (Auditor.this) {
                    LOG.info("Shutting down Auditor's Executor");
                    executor.shutdown();
                }
            });
        }
    }

    @VisibleForTesting
    synchronized Future<?> submitAuditTask() {
        if (executor.isShutdown()) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.setException(new BKAuditException("Auditor shutting down"));
            return f;
        }
        return executor.submit(() -> {
            try {
                waitIfLedgerReplicationDisabled();
                int lostBookieRecoveryDelay = Auditor.this.ledgerUnderreplicationManager
                        .getLostBookieRecoveryDelay();
                List<String> availableBookies = getAvailableBookies();

                // casting to String, as knownBookies and availableBookies
                // contains only String values
                // find new bookies(if any) and update the known bookie list
                Collection<String> newBookies = CollectionUtils.subtract(
                        availableBookies, knownBookies);
                knownBookies.addAll(newBookies);
                if (!bookiesToBeAudited.isEmpty() && knownBookies.containsAll(bookiesToBeAudited)) {
                    // the bookie, which went down earlier and had an audit scheduled for,
                    // has come up. So let us stop tracking it and cancel the audit. Since
                    // we allow delaying of audit when there is only one failed bookie,
                    // bookiesToBeAudited should just have 1 element and hence containsAll
                    // check should be ok
                    if (auditTask != null && auditTask.cancel(false)) {
                        auditTask = null;
                        auditorStats.getNumDelayedBookieAuditsCancelled().inc();
                    }
                    bookiesToBeAudited.clear();
                }

                // find lost bookies(if any)
                bookiesToBeAudited.addAll(CollectionUtils.subtract(knownBookies, availableBookies));
                if (bookiesToBeAudited.size() == 0) {
                    return;
                }

                knownBookies.removeAll(bookiesToBeAudited);
                if (lostBookieRecoveryDelay == 0) {
                    auditorBookieCheckTask.startAudit(false);
                    bookiesToBeAudited.clear();
                    return;
                }
                if (bookiesToBeAudited.size() > 1) {
                    // if more than one bookie is down, start the audit immediately;
                    LOG.info("Multiple bookie failure; not delaying bookie audit. "
                                    + "Bookies lost now: {}; All lost bookies: {}",
                            CollectionUtils.subtract(knownBookies, availableBookies),
                            bookiesToBeAudited);
                    if (auditTask != null && auditTask.cancel(false)) {
                        auditTask = null;
                        auditorStats.getNumDelayedBookieAuditsCancelled().inc();
                    }
                    auditorBookieCheckTask.startAudit(false);
                    bookiesToBeAudited.clear();
                    return;
                }
                if (auditTask == null) {
                    // if there is no scheduled audit, schedule one
                    auditTask = executor.schedule(() -> {
                        auditorBookieCheckTask.startAudit(false);
                        auditTask = null;
                        bookiesToBeAudited.clear();
                    }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                    auditorStats.getNumBookieAuditsDelayed().inc();
                    LOG.info("Delaying bookie audit by {} secs for {}", lostBookieRecoveryDelay,
                            bookiesToBeAudited);
                }
            } catch (BKException bke) {
                LOG.error("Exception getting bookie list", bke);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while watching available bookies ", ie);
            } catch (UnavailableException ue) {
                LOG.error("Exception while watching available bookies", ue);
            }
        });
    }

    synchronized Future<?> submitLostBookieRecoveryDelayChangedEvent() {
        if (executor.isShutdown()) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.setException(new BKAuditException("Auditor shutting down"));
            return f;
        }
        return executor.submit(() -> {
            int lostBookieRecoveryDelay = -1;
            try {
                waitIfLedgerReplicationDisabled();
                lostBookieRecoveryDelay = Auditor.this.ledgerUnderreplicationManager
                        .getLostBookieRecoveryDelay();
                // if there is pending auditTask, cancel the task. So that it can be rescheduled
                // after new lostBookieRecoveryDelay period
                if (auditTask != null) {
                    LOG.info("lostBookieRecoveryDelay period has been changed so canceling the pending AuditTask");
                    auditTask.cancel(false);
                    auditorStats.getNumDelayedBookieAuditsCancelled().inc();
                }

                // if lostBookieRecoveryDelay is set to its previous value then consider it as
                // signal to trigger the Audit immediately.
                if ((lostBookieRecoveryDelay == 0)
                        || (lostBookieRecoveryDelay == lostBookieRecoveryDelayBeforeChange)) {
                    LOG.info(
                            "lostBookieRecoveryDelay has been set to 0 or reset to its previous value, "
                                    + "so starting AuditTask. Current lostBookieRecoveryDelay: {}, "
                                    + "previous lostBookieRecoveryDelay: {}",
                            lostBookieRecoveryDelay, lostBookieRecoveryDelayBeforeChange);
                    auditorBookieCheckTask.startAudit(false);
                    auditTask = null;
                    bookiesToBeAudited.clear();
                } else if (auditTask != null) {
                    LOG.info("lostBookieRecoveryDelay has been set to {}, so rescheduling AuditTask accordingly",
                            lostBookieRecoveryDelay);
                    auditTask = executor.schedule(() -> {
                        auditorBookieCheckTask.startAudit(false);
                        auditTask = null;
                        bookiesToBeAudited.clear();
                    }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                    auditorStats.getNumBookieAuditsDelayed().inc();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while for LedgersReplication to be enabled ", ie);
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                LOG.error("Non Recoverable Exception while reading from ZK", nre);
                submitShutdownTask();
            } catch (UnavailableException ue) {
                LOG.error("Exception while reading from ZK", ue);
            } finally {
                if (lostBookieRecoveryDelay != -1) {
                    lostBookieRecoveryDelayBeforeChange = lostBookieRecoveryDelay;
                }
            }
        });
    }

    public void start() {
        LOG.info("I'm starting as Auditor Bookie. ID: {}", bookieIdentifier);
        // on startup watching available bookie and based on the
        // available bookies determining the bookie failures.
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }

            try {
                watchBookieChanges();
                knownBookies = getAvailableBookies();
                this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(new LostBookieRecoveryDelayChangedCb());
            } catch (BKException bke) {
                LOG.error("Couldn't get bookie list, so exiting", bke);
                submitShutdownTask();
                return;
            } catch (UnavailableException ue) {
                LOG.error("Exception while registering for change notification, so exiting", ue);
                submitShutdownTask();
                return;
            }
            scheduleBookieCheckTask();
            scheduleCheckAllLedgersTask();
            schedulePlacementPolicyCheckTask();
            scheduleReplicasCheckTask();
        }
    }

    protected void submitBookieCheckTask() {
        executor.submit(auditorBookieCheckTask);
    }

    private void scheduleBookieCheckTask() {
        long bookieCheckInterval = conf.getAuditorPeriodicBookieCheckInterval();
        if (bookieCheckInterval == 0) {
            LOG.info("Auditor periodic bookie checking disabled, running once check now anyhow");
            submitBookieCheckTask();
        } else {
            LOG.info("Auditor periodic bookie checking enabled" + " 'auditorPeriodicBookieCheckInterval' {} seconds",
                    bookieCheckInterval);
            executor.scheduleAtFixedRate(auditorBookieCheckTask, 0, bookieCheckInterval, TimeUnit.SECONDS);
        }
    }

    private void scheduleCheckAllLedgersTask() {
        long interval = conf.getAuditorPeriodicCheckInterval();

        if (interval > 0) {
            LOG.info("Auditor periodic ledger checking enabled" + " 'auditorPeriodicCheckInterval' {} seconds",
                    interval);

            long checkAllLedgersLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                checkAllLedgersLastExecutedCTime = ledgerUnderreplicationManager.getCheckAllLedgersCTime();
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                LOG.error("Non Recoverable Exception while reading from ZK", nre);
                submitShutdownTask();
                return;
            } catch (UnavailableException ue) {
                LOG.error("Got UnavailableException while trying to get checkAllLedgersCTime", ue);
                checkAllLedgersLastExecutedCTime = -1;
            }
            if (checkAllLedgersLastExecutedCTime == -1) {
                durationSinceLastExecutionInSecs = -1;
                initialDelay = 0;
            } else {
                durationSinceLastExecutionInSecs = (System.currentTimeMillis() - checkAllLedgersLastExecutedCTime)
                        / 1000;
                if (durationSinceLastExecutionInSecs < 0) {
                    // this can happen if there is no strict time ordering
                    durationSinceLastExecutionInSecs = 0;
                }
                initialDelay = durationSinceLastExecutionInSecs > interval ? 0
                        : (interval - durationSinceLastExecutionInSecs);
            }
            LOG.info(
                    "checkAllLedgers scheduling info.  checkAllLedgersLastExecutedCTime: {} "
                            + "durationSinceLastExecutionInSecs: {} initialDelay: {} interval: {}",
                    checkAllLedgersLastExecutedCTime, durationSinceLastExecutionInSecs, initialDelay, interval);

            executor.scheduleAtFixedRate(auditorCheckAllLedgersTask, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            LOG.info("Periodic checking disabled");
        }
    }

    private void schedulePlacementPolicyCheckTask() {
        long interval = conf.getAuditorPeriodicPlacementPolicyCheckInterval();

        if (interval > 0) {
            LOG.info("Auditor periodic placement policy check enabled"
                    + " 'auditorPeriodicPlacementPolicyCheckInterval' {} seconds", interval);

            long placementPolicyCheckLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                placementPolicyCheckLastExecutedCTime = ledgerUnderreplicationManager.getPlacementPolicyCheckCTime();
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                LOG.error("Non Recoverable Exception while reading from ZK", nre);
                submitShutdownTask();
                return;
            } catch (UnavailableException ue) {
                LOG.error("Got UnavailableException while trying to get placementPolicyCheckCTime", ue);
                placementPolicyCheckLastExecutedCTime = -1;
            }
            if (placementPolicyCheckLastExecutedCTime == -1) {
                durationSinceLastExecutionInSecs = -1;
                initialDelay = 0;
            } else {
                durationSinceLastExecutionInSecs = (System.currentTimeMillis() - placementPolicyCheckLastExecutedCTime)
                        / 1000;
                if (durationSinceLastExecutionInSecs < 0) {
                    // this can happen if there is no strict time ordering
                    durationSinceLastExecutionInSecs = 0;
                }
                initialDelay = durationSinceLastExecutionInSecs > interval ? 0
                        : (interval - durationSinceLastExecutionInSecs);
            }
            LOG.info(
                    "placementPolicyCheck scheduling info.  placementPolicyCheckLastExecutedCTime: {} "
                            + "durationSinceLastExecutionInSecs: {} initialDelay: {} interval: {}",
                    placementPolicyCheckLastExecutedCTime, durationSinceLastExecutionInSecs, initialDelay, interval);

            executor.scheduleAtFixedRate(auditorPlacementPolicyCheckTask, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            LOG.info("Periodic placementPolicy check disabled");
        }
    }

    private void scheduleReplicasCheckTask() {
        long interval = conf.getAuditorPeriodicReplicasCheckInterval();

        if (interval <= 0) {
            LOG.info("Periodic replicas check disabled");
            return;
        }

        LOG.info("Auditor periodic replicas check enabled" + " 'auditorReplicasCheckInterval' {} seconds", interval);
        long replicasCheckLastExecutedCTime;
        long durationSinceLastExecutionInSecs;
        long initialDelay;
        try {
            replicasCheckLastExecutedCTime = ledgerUnderreplicationManager.getReplicasCheckCTime();
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            LOG.error("Non Recoverable Exception while reading from ZK", nre);
            submitShutdownTask();
            return;
        } catch (UnavailableException ue) {
            LOG.error("Got UnavailableException while trying to get replicasCheckCTime", ue);
            replicasCheckLastExecutedCTime = -1;
        }
        if (replicasCheckLastExecutedCTime == -1) {
            durationSinceLastExecutionInSecs = -1;
            initialDelay = 0;
        } else {
            durationSinceLastExecutionInSecs = (System.currentTimeMillis() - replicasCheckLastExecutedCTime) / 1000;
            if (durationSinceLastExecutionInSecs < 0) {
                // this can happen if there is no strict time ordering
                durationSinceLastExecutionInSecs = 0;
            }
            initialDelay = durationSinceLastExecutionInSecs > interval ? 0
                    : (interval - durationSinceLastExecutionInSecs);
        }
        LOG.info(
                "replicasCheck scheduling info. replicasCheckLastExecutedCTime: {} "
                        + "durationSinceLastExecutionInSecs: {} initialDelay: {} interval: {}",
                replicasCheckLastExecutedCTime, durationSinceLastExecutionInSecs, initialDelay, interval);

        executor.scheduleAtFixedRate(auditorReplicasCheckTask, initialDelay, interval, TimeUnit.SECONDS);
    }

    private class LostBookieRecoveryDelayChangedCb implements GenericCallback<Void> {
        @Override
        public void operationComplete(int rc, Void result) {
            try {
                Auditor.this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(LostBookieRecoveryDelayChangedCb.this);
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                LOG.error("Non Recoverable Exception while reading from ZK", nre);
                submitShutdownTask();
            } catch (UnavailableException ae) {
                LOG.error("Exception while registering for a LostBookieRecoveryDelay notification", ae);
            }
            Auditor.this.submitLostBookieRecoveryDelayChangedEvent();
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
            ReplicationEnableCb cb = new ReplicationEnableCb();
            LOG.info("LedgerReplication is disabled externally through Zookeeper, "
                    + "since DISABLE_NODE ZNode is created, so waiting untill it is enabled");
            ledgerUnderreplicationManager.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

    protected List<String> getAvailableBookies() throws BKException {
        // Get the available bookies
        Collection<BookieId> availableBkAddresses = admin.getAvailableBookies();
        Collection<BookieId> readOnlyBkAddresses = admin.getReadOnlyBookies();
        availableBkAddresses.addAll(readOnlyBkAddresses);

        List<String> availableBookies = new ArrayList<String>();
        for (BookieId addr : availableBkAddresses) {
            availableBookies.add(addr.toString());
        }
        return availableBookies;
    }

    private void watchBookieChanges() throws BKException {
        admin.watchWritableBookiesChanged(bookies -> submitAuditTask());
        admin.watchReadOnlyBookiesChanged(bookies -> submitAuditTask());
    }

    /**
     * Shutdown the auditor.
     */
    public void shutdown() {
        LOG.info("Shutting down auditor");
        executor.shutdown();
        try {
            while (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Executor not shutting down, interrupting");
                executor.shutdownNow();
            }

            // shutdown all auditorTasks to clean some resource
            allAuditorTasks.forEach(AuditorTask::shutdown);
            allAuditorTasks.clear();

            if (ownAdmin) {
                admin.close();
            }
            if (ownBkc) {
                bkc.close();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down auditor bookie", ie);
        } catch (BKException bke) {
            LOG.warn("Exception while shutting down auditor bookie", bke);
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Return true if auditor is running otherwise return false.
     *
     * @return auditor status
     */
    public boolean isRunning() {
        return !executor.isShutdown();
    }

    int getLostBookieRecoveryDelayBeforeChange() {
        return lostBookieRecoveryDelayBeforeChange;
    }

    Future<?> getAuditTask() {
        return auditTask;
    }
}
