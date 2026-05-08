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
import java.util.stream.Collectors;
import lombok.CustomLog;
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

/**
 * Auditor is a single entity in the entire Bookie cluster and will be watching
 * all the bookies under 'ledgerrootpath/available' zkpath. When any of the
 * bookie failed or disconnected from zk, he will start initiating the
 * re-replication activities by keeping all the corresponding ledgers of the
 * failed bookie as underreplicated znode in zk.
 *
 * <p>TODO: eliminate the direct usage of zookeeper here {@link https://github.com/apache/bookkeeper/issues/1332}
 */
@CustomLog
public class Auditor implements AutoCloseable {
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
            log.info()
                    .attr("authProvider", admin.getConf().getClientAuthProviderFactoryClass())
                    .log("AuthProvider used by the Auditor");
            if (this.ledgerUnderreplicationManager
                    .initializeLostBookieRecoveryDelay(conf.getLostBookieRecoveryDelay())) {
                log.info()
                        .attr("lostBookieRecoveryDelay", conf.getLostBookieRecoveryDelay())
                        .log("Initializing lostBookieRecoveryDelay zNode to the conf value");
            } else {
                log.info("Valid lostBookieRecoveryDelay zNode is available, so not creating "
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
            log.info("Executing submitShutdownTask");
            if (executor.isShutdown()) {
                log.info("executor is already shutdown");
                return;
            }
            executor.submit(() -> {
                synchronized (Auditor.this) {
                    log.info("Shutting down Auditor's Executor");
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
                    log.info()
                            .attr("bookiesLostNow", CollectionUtils.subtract(knownBookies, availableBookies))
                            .attr("allLostBookies", bookiesToBeAudited)
                            .log("Multiple bookie failure; not delaying bookie audit");
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
                    log.info()
                            .attr("delaySecs", lostBookieRecoveryDelay)
                            .attr("bookies", bookiesToBeAudited)
                            .log("Delaying bookie audit");
                }
            } catch (BKException bke) {
                log.error().exception(bke).log("Exception getting bookie list");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error().exception(ie).log("Interrupted while watching available bookies");
            } catch (UnavailableException ue) {
                log.error().exception(ue).log("Exception while watching available bookies");
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
                    log.info("lostBookieRecoveryDelay period has been changed so canceling the pending AuditTask");
                    auditTask.cancel(false);
                    auditorStats.getNumDelayedBookieAuditsCancelled().inc();
                }

                // if lostBookieRecoveryDelay is set to its previous value then consider it as
                // signal to trigger the Audit immediately.
                if ((lostBookieRecoveryDelay == 0)
                        || (lostBookieRecoveryDelay == lostBookieRecoveryDelayBeforeChange)) {
                    log.info()
                            .attr("currentDelay", lostBookieRecoveryDelay)
                            .attr("previousDelay", lostBookieRecoveryDelayBeforeChange)
                            .log("lostBookieRecoveryDelay has been set to 0"
                                    + " or reset to its previous value,"
                                    + " so starting AuditTask");
                    auditorBookieCheckTask.startAudit(false);
                    auditTask = null;
                    bookiesToBeAudited.clear();
                } else if (auditTask != null) {
                    log.info()
                            .attr("lostBookieRecoveryDelay", lostBookieRecoveryDelay)
                            .log("lostBookieRecoveryDelay changed, rescheduling AuditTask accordingly");
                    auditTask = executor.schedule(() -> {
                        auditorBookieCheckTask.startAudit(false);
                        auditTask = null;
                        bookiesToBeAudited.clear();
                    }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                    auditorStats.getNumBookieAuditsDelayed().inc();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.error().exception(ie).log("Interrupted while waiting for LedgersReplication to be enabled");
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                log.error().exception(nre).log("Non Recoverable Exception while reading from ZK");
                submitShutdownTask();
            } catch (UnavailableException ue) {
                log.error().exception(ue).log("Exception while reading from ZK");
            } finally {
                if (lostBookieRecoveryDelay != -1) {
                    lostBookieRecoveryDelayBeforeChange = lostBookieRecoveryDelay;
                }
            }
        });
    }

    public void start() {
        log.info().attr("bookieId", bookieIdentifier).log("I'm starting as Auditor Bookie");
        // on startup watching available bookie and based on the
        // available bookies determining the bookie failures.
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }

            try {
                watchBookieChanges();
                // Start with all available bookies
                // to handle situations where the auditor
                // is started after some bookies have already failed
                knownBookies = admin.getAllBookies().stream()
                        .map(BookieId::toString)
                        .collect(Collectors.toList());
                this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(new LostBookieRecoveryDelayChangedCb());
            } catch (BKException bke) {
                log.error().exception(bke).log("Couldn't get bookie list, so exiting");
                submitShutdownTask();
                return;
            } catch (UnavailableException ue) {
                log.error().exception(ue).log("Exception while registering for change notification, so exiting");
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
            log.info("Auditor periodic bookie checking disabled, running once check now anyhow");
            submitBookieCheckTask();
        } else {
            log.info()
                    .attr("auditorPeriodicBookieCheckInterval", bookieCheckInterval)
                    .log("Auditor periodic bookie checking enabled");
            executor.scheduleAtFixedRate(auditorBookieCheckTask, 0, bookieCheckInterval, TimeUnit.SECONDS);
        }
    }

    private void scheduleCheckAllLedgersTask() {
        long interval = conf.getAuditorPeriodicCheckInterval();

        if (interval > 0) {
            log.info().attr("auditorPeriodicCheckInterval", interval).log("Auditor periodic ledger checking enabled");

            long checkAllLedgersLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                checkAllLedgersLastExecutedCTime = ledgerUnderreplicationManager.getCheckAllLedgersCTime();
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                log.error().exception(nre).log("Non Recoverable Exception while reading from ZK");
                submitShutdownTask();
                return;
            } catch (UnavailableException ue) {
                log.error().exception(ue).log("Got UnavailableException while trying to get checkAllLedgersCTime");
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
            log.info()
                    .attr("checkAllLedgersLastExecutedCTime", checkAllLedgersLastExecutedCTime)
                    .attr("durationSinceLastExecutionInSecs", durationSinceLastExecutionInSecs)
                    .attr("initialDelay", initialDelay)
                    .attr("interval", interval)
                    .log("checkAllLedgers scheduling info");

            executor.scheduleAtFixedRate(auditorCheckAllLedgersTask, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            log.info("Periodic checking disabled");
        }
    }

    private void schedulePlacementPolicyCheckTask() {
        long interval = conf.getAuditorPeriodicPlacementPolicyCheckInterval();

        if (interval > 0) {
            log.info()
                    .attr("auditorPeriodicPlacementPolicyCheckInterval", interval)
                    .log("Auditor periodic placement policy check enabled");

            long placementPolicyCheckLastExecutedCTime;
            long durationSinceLastExecutionInSecs;
            long initialDelay;
            try {
                placementPolicyCheckLastExecutedCTime = ledgerUnderreplicationManager.getPlacementPolicyCheckCTime();
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                log.error().exception(nre).log("Non Recoverable Exception while reading from ZK");
                submitShutdownTask();
                return;
            } catch (UnavailableException ue) {
                log.error().exception(ue).log("Got UnavailableException while trying to get placementPolicyCheckCTime");
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
            log.info()
                    .attr("placementPolicyCheckLastExecutedCTime", placementPolicyCheckLastExecutedCTime)
                    .attr("durationSinceLastExecutionInSecs", durationSinceLastExecutionInSecs)
                    .attr("initialDelay", initialDelay)
                    .attr("interval", interval)
                    .log("placementPolicyCheck scheduling info");

            executor.scheduleAtFixedRate(auditorPlacementPolicyCheckTask, initialDelay, interval, TimeUnit.SECONDS);
        } else {
            log.info("Periodic placementPolicy check disabled");
        }
    }

    private void scheduleReplicasCheckTask() {
        long interval = conf.getAuditorPeriodicReplicasCheckInterval();

        if (interval <= 0) {
            log.info("Periodic replicas check disabled");
            return;
        }

        log.info().attr("auditorReplicasCheckInterval", interval).log("Auditor periodic replicas check enabled");
        long replicasCheckLastExecutedCTime;
        long durationSinceLastExecutionInSecs;
        long initialDelay;
        try {
            replicasCheckLastExecutedCTime = ledgerUnderreplicationManager.getReplicasCheckCTime();
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            log.error().exception(nre).log("Non Recoverable Exception while reading from ZK");
            submitShutdownTask();
            return;
        } catch (UnavailableException ue) {
            log.error().exception(ue).log("Got UnavailableException while trying to get replicasCheckCTime");
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
        log.info()
                .attr("replicasCheckLastExecutedCTime", replicasCheckLastExecutedCTime)
                .attr("durationSinceLastExecutionInSecs", durationSinceLastExecutionInSecs)
                .attr("initialDelay", initialDelay)
                .attr("interval", interval)
                .log("replicasCheck scheduling info");

        executor.scheduleAtFixedRate(auditorReplicasCheckTask, initialDelay, interval, TimeUnit.SECONDS);
    }

    private class LostBookieRecoveryDelayChangedCb implements GenericCallback<Void> {
        @Override
        public void operationComplete(int rc, Void result) {
            try {
                Auditor.this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(LostBookieRecoveryDelayChangedCb.this);
            } catch (ReplicationException.NonRecoverableReplicationException nre) {
                log.error().exception(nre).log("Non Recoverable Exception while reading from ZK");
                submitShutdownTask();
            } catch (UnavailableException ae) {
                log.error().exception(ae).log("Exception while registering for a LostBookieRecoveryDelay notification");
            }
            Auditor.this.submitLostBookieRecoveryDelayChangedEvent();
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
            ReplicationEnableCb cb = new ReplicationEnableCb();
            log.info("LedgerReplication is disabled externally through Zookeeper, "
                    + "since DISABLE_NODE ZNode is created, so waiting until it is enabled");
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
        log.info("Shutting down auditor");
        executor.shutdown();
        try {
            while (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor not shutting down, interrupting");
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
            if (ledgerManager != null) {
                ledgerManager.close();
            }
            if (ledgerUnderreplicationManager != null) {
                ledgerUnderreplicationManager.close();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn().exception(ie).log("Interrupted while shutting down auditor bookie");
        } catch (UnavailableException | IOException | BKException bke) {
            log.warn().exception(bke).log("Exception while shutting down auditor bookie");
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
