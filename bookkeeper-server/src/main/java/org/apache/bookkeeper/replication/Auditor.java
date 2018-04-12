/**
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
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
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
public class Auditor {
    private static final Logger LOG = LoggerFactory.getLogger(Auditor.class);
    private final ServerConfiguration conf;
    private BookKeeper bkc;
    private BookKeeperAdmin admin;
    private BookieLedgerIndexer bookieLedgerIndexer;
    private LedgerManager ledgerManager;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private final ScheduledExecutorService executor;
    private List<String> knownBookies = new ArrayList<String>();
    private final String bookieIdentifier;
    private final StatsLogger statsLogger;
    private final OpStatsLogger numUnderReplicatedLedger;
    private final OpStatsLogger uRLPublishTimeForLostBookies;
    private final OpStatsLogger bookieToLedgersMapCreationTime;
    private final OpStatsLogger checkAllLedgersTime;
    private final Counter numLedgersChecked;
    private final OpStatsLogger numFragmentsPerLedger;
    private final OpStatsLogger numBookiesPerLedger;
    private final Counter numBookieAuditsDelayed;
    private final Counter numDelayedBookieAuditsCancelled;
    private volatile Future<?> auditTask;
    private Set<String> bookiesToBeAudited = Sets.newHashSet();
    private volatile int lostBookieRecoveryDelayBeforeChange;

    public Auditor(final String bookieIdentifier, ServerConfiguration conf,
                   ZooKeeper zkc, StatsLogger statsLogger) throws UnavailableException {
        this.conf = conf;
        this.bookieIdentifier = bookieIdentifier;
        this.statsLogger = statsLogger;

        numUnderReplicatedLedger = this.statsLogger.getOpStatsLogger(ReplicationStats.NUM_UNDER_REPLICATED_LEDGERS);
        uRLPublishTimeForLostBookies = this.statsLogger
                .getOpStatsLogger(ReplicationStats.URL_PUBLISH_TIME_FOR_LOST_BOOKIE);
        bookieToLedgersMapCreationTime = this.statsLogger
                .getOpStatsLogger(ReplicationStats.BOOKIE_TO_LEDGERS_MAP_CREATION_TIME);
        checkAllLedgersTime = this.statsLogger.getOpStatsLogger(ReplicationStats.CHECK_ALL_LEDGERS_TIME);
        numLedgersChecked = this.statsLogger.getCounter(ReplicationStats.NUM_LEDGERS_CHECKED);
        numFragmentsPerLedger = statsLogger.getOpStatsLogger(ReplicationStats.NUM_FRAGMENTS_PER_LEDGER);
        numBookiesPerLedger = statsLogger.getOpStatsLogger(ReplicationStats.NUM_BOOKIES_PER_LEDGER);
        numBookieAuditsDelayed = this.statsLogger.getCounter(ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED);
        numDelayedBookieAuditsCancelled = this.statsLogger
                .getCounter(ReplicationStats.NUM_DELAYED_BOOKIE_AUDITS_DELAYES_CANCELLED);

        initialize(conf, zkc);

        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "AuditorBookie-" + bookieIdentifier);
                    t.setDaemon(true);
                    return t;
                }
            });
    }

    private void initialize(ServerConfiguration conf, ZooKeeper zkc)
            throws UnavailableException {
        try {
            ClientConfiguration clientConfiguration = new ClientConfiguration(conf);
            clientConfiguration.setClientRole(ClientConfiguration.CLIENT_ROLE_SYSTEM);
            LOG.info("AuthProvider used by the Auditor is {}",
                clientConfiguration.getClientAuthProviderFactoryClass());
            this.bkc = new BookKeeper(clientConfiguration, zkc);

            LedgerManagerFactory ledgerManagerFactory = AbstractZkLedgerManagerFactory
                    .newLedgerManagerFactory(
                        conf,
                        bkc.getMetadataClientDriver().getLayoutManager());
            ledgerManager = ledgerManagerFactory.newLedgerManager();
            this.bookieLedgerIndexer = new BookieLedgerIndexer(ledgerManager);

            this.ledgerUnderreplicationManager = ledgerManagerFactory
                    .newLedgerUnderreplicationManager();
            this.admin = new BookKeeperAdmin(bkc, statsLogger);
            if (this.ledgerUnderreplicationManager
                    .initializeLostBookieRecoveryDelay(conf.getLostBookieRecoveryDelay())) {
                LOG.info("Initializing lostBookieRecoveryDelay zNode to the conif value: {}",
                        conf.getLostBookieRecoveryDelay());
            } else {
                LOG.info("Valid lostBookieRecoveryDelay zNode is available, so not creating "
                        + "lostBookieRecoveryDelay zNode as part of Auditor initialization ");
            }
            lostBookieRecoveryDelayBeforeChange = this.ledgerUnderreplicationManager.getLostBookieRecoveryDelay();
        } catch (CompatibilityException ce) {
            throw new UnavailableException(
                    "CompatibilityException while initializing Auditor", ce);
        } catch (IOException | BKException | KeeperException ioe) {
            throw new UnavailableException(
                    "Exception while initializing Auditor", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new UnavailableException(
                    "Interrupted while initializing Auditor", ie);
        }
    }

    private void submitShutdownTask() {
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }
            executor.submit(new Runnable() {
                    public void run() {
                        synchronized (Auditor.this) {
                            executor.shutdown();
                        }
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
        return executor.submit(new Runnable() {
                @SuppressWarnings("unchecked")
                public void run() {
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
                                numDelayedBookieAuditsCancelled.inc();
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
                            startAudit(false);
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
                                numDelayedBookieAuditsCancelled.inc();
                            }
                            startAudit(false);
                            bookiesToBeAudited.clear();
                            return;
                        }
                        if (auditTask == null) {
                            // if there is no scheduled audit, schedule one
                            auditTask = executor.schedule(new Runnable() {
                                public void run() {
                                    startAudit(false);
                                    auditTask = null;
                                    bookiesToBeAudited.clear();
                                }
                            }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                            numBookieAuditsDelayed.inc();
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
                }
            });
    }

    synchronized Future<?> submitLostBookieRecoveryDelayChangedEvent() {
        if (executor.isShutdown()) {
            SettableFuture<Void> f = SettableFuture.<Void> create();
            f.setException(new BKAuditException("Auditor shutting down"));
            return f;
        }
        return executor.submit(new Runnable() {
            int lostBookieRecoveryDelay = -1;
            public void run() {
                try {
                    waitIfLedgerReplicationDisabled();
                    lostBookieRecoveryDelay = Auditor.this.ledgerUnderreplicationManager
                            .getLostBookieRecoveryDelay();
                    // if there is pending auditTask, cancel the task. So that it can be rescheduled
                    // after new lostBookieRecoveryDelay period
                    if (auditTask != null) {
                        LOG.info("lostBookieRecoveryDelay period has been changed so canceling the pending AuditTask");
                        auditTask.cancel(false);
                        numDelayedBookieAuditsCancelled.inc();
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
                        startAudit(false);
                        auditTask = null;
                        bookiesToBeAudited.clear();
                    } else if (auditTask != null) {
                        LOG.info("lostBookieRecoveryDelay has been set to {}, so rescheduling AuditTask accordingly",
                                lostBookieRecoveryDelay);
                        auditTask = executor.schedule(new Runnable() {
                            public void run() {
                                startAudit(false);
                                auditTask = null;
                                bookiesToBeAudited.clear();
                            }
                        }, lostBookieRecoveryDelay, TimeUnit.SECONDS);
                        numBookieAuditsDelayed.inc();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted while for LedgersReplication to be enabled ", ie);
                } catch (UnavailableException ue) {
                    LOG.error("Exception while reading from ZK", ue);
                } finally {
                    if (lostBookieRecoveryDelay != -1) {
                        lostBookieRecoveryDelayBeforeChange = lostBookieRecoveryDelay;
                    }
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

            long interval = conf.getAuditorPeriodicCheckInterval();

            if (interval > 0) {
                LOG.info("Auditor periodic ledger checking enabled"
                         + " 'auditorPeriodicCheckInterval' {} seconds", interval);
                executor.scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            try {
                                if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                                    LOG.info("Ledger replication disabled, skipping");
                                    return;
                                }

                                Stopwatch stopwatch = Stopwatch.createStarted();
                                checkAllLedgers();
                                checkAllLedgersTime.registerSuccessfulEvent(stopwatch.stop()
                                                .elapsed(TimeUnit.MILLISECONDS),
                                        TimeUnit.MILLISECONDS);
                            } catch (KeeperException ke) {
                                LOG.error("Exception while running periodic check", ke);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                LOG.error("Interrupted while running periodic check", ie);
                            } catch (BKAuditException bkae) {
                                LOG.error("Exception while running periodic check", bkae);
                            } catch (BKException bke) {
                                LOG.error("Exception running periodic check", bke);
                            } catch (IOException ioe) {
                                LOG.error("I/O exception running periodic check", ioe);
                            } catch (ReplicationException.UnavailableException ue) {
                                LOG.error("Underreplication manager unavailable running periodic check", ue);
                            }
                        }
                    }, interval, interval, TimeUnit.SECONDS);
            } else {
                LOG.info("Periodic checking disabled");
            }
            try {
                watchBookieChanges();
                knownBookies = getAvailableBookies();
            } catch (BKException bke) {
                LOG.error("Couldn't get bookie list, exiting", bke);
                submitShutdownTask();
            }

            try {
                this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(new LostBookieRecoveryDelayChangedCb());
            } catch (UnavailableException ue) {
                LOG.error("Exception while registering for LostBookieRecoveryDelay change notification", ue);
                submitShutdownTask();
            }

            long bookieCheckInterval = conf.getAuditorPeriodicBookieCheckInterval();
            if (bookieCheckInterval == 0) {
                LOG.info("Auditor periodic bookie checking disabled, running once check now anyhow");
                executor.submit(bookieCheck);
            } else {
                LOG.info("Auditor periodic bookie checking enabled"
                         + " 'auditorPeriodicBookieCheckInterval' {} seconds", bookieCheckInterval);
                executor.scheduleAtFixedRate(bookieCheck, 0, bookieCheckInterval, TimeUnit.SECONDS);
            }
        }
    }

    private class LostBookieRecoveryDelayChangedCb implements GenericCallback<Void> {
        @Override
        public void operationComplete(int rc, Void result) {
            try {
                Auditor.this.ledgerUnderreplicationManager
                        .notifyLostBookieRecoveryDelayChanged(LostBookieRecoveryDelayChangedCb.this);
            } catch (UnavailableException ae) {
                LOG.error("Exception while registering for a LostBookieRecoveryDelay notification", ae);
            }
            Auditor.this.submitLostBookieRecoveryDelayChangedEvent();
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        ReplicationEnableCb cb = new ReplicationEnableCb();
        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
            LOG.info("LedgerReplication is disabled externally through Zookeeper, "
                    + "since DISABLE_NODE ZNode is created, so waiting untill it is enabled");
            ledgerUnderreplicationManager.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

    private List<String> getAvailableBookies() throws BKException {
        // Get the available bookies
        Collection<BookieSocketAddress> availableBkAddresses = admin.getAvailableBookies();
        Collection<BookieSocketAddress> readOnlyBkAddresses = admin.getReadOnlyBookies();
        availableBkAddresses.addAll(readOnlyBkAddresses);

        List<String> availableBookies = new ArrayList<String>();
        for (BookieSocketAddress addr : availableBkAddresses) {
            availableBookies.add(addr.toString());
        }
        return availableBookies;
    }

    private void watchBookieChanges() throws BKException {
        admin.watchWritableBookiesChanged(bookies -> submitAuditTask());
        admin.watchReadOnlyBookiesChanged(bookies -> submitAuditTask());
    }

    /**
     * Start running the actual audit task.
     *
     * @param shutDownTask
     *      A boolean that indicates whether or not to schedule shutdown task on any failure
     */
    private void startAudit(boolean shutDownTask) {
        try {
            auditBookies();
            shutDownTask = false;
        } catch (BKException bke) {
            LOG.error("Exception getting bookie list", bke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while watching available bookies ", ie);
        } catch (BKAuditException bke) {
            LOG.error("Exception while watching available bookies", bke);
        } catch (KeeperException ke) {
            LOG.error("Exception reading bookie list", ke);
        }
        if (shutDownTask) {
            submitShutdownTask();
        }
    }

    @SuppressWarnings("unchecked")
    private void auditBookies()
            throws BKAuditException, KeeperException,
            InterruptedException, BKException {
        try {
            waitIfLedgerReplicationDisabled();
        } catch (UnavailableException ue) {
            LOG.error("Underreplication unavailable, skipping audit."
                      + "Will retry after a period");
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        // put exit cases here
        Map<String, Set<Long>> ledgerDetails = generateBookie2LedgersIndex();
        try {
            if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                // has been disabled while we were generating the index
                // discard this run, and schedule a new one
                executor.submit(bookieCheck);
                return;
            }
        } catch (UnavailableException ue) {
            LOG.error("Underreplication unavailable, skipping audit."
                      + "Will retry after a period");
            return;
        }

        List<String> availableBookies = getAvailableBookies();
        // find lost bookies
        Set<String> knownBookies = ledgerDetails.keySet();
        Collection<String> lostBookies = CollectionUtils.subtract(knownBookies,
                availableBookies);

        bookieToLedgersMapCreationTime.registerSuccessfulEvent(stopwatch.elapsed(TimeUnit.MILLISECONDS),
                TimeUnit.MILLISECONDS);
        if (lostBookies.size() > 0) {
            handleLostBookies(lostBookies, ledgerDetails);
            uRLPublishTimeForLostBookies.registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MILLISECONDS),
                    TimeUnit.MILLISECONDS);
        }

    }

    private Map<String, Set<Long>> generateBookie2LedgersIndex()
            throws BKAuditException {
        return bookieLedgerIndexer.getBookieToLedgerIndex();
    }

    private void handleLostBookies(Collection<String> lostBookies,
            Map<String, Set<Long>> ledgerDetails) throws BKAuditException {
        LOG.info("Following are the failed bookies: " + lostBookies
                + " and searching its ledgers for re-replication");

        for (String bookieIP : lostBookies) {
            // identify all the ledgers in bookieIP and publishing these ledgers
            // as under-replicated.
            publishSuspectedLedgers(bookieIP, ledgerDetails.get(bookieIP));
        }
    }

    private void publishSuspectedLedgers(String bookieIP, Set<Long> ledgers)
            throws BKAuditException {
        if (null == ledgers || ledgers.size() == 0) {
            // there is no ledgers available for this bookie and just
            // ignoring the bookie failures
            LOG.info("There is no ledgers for the failed bookie: {}", bookieIP);
            return;
        }
        LOG.info("Following ledgers: {} of bookie: {} are identified as underreplicated", ledgers, bookieIP);
        numUnderReplicatedLedger.registerSuccessfulValue(ledgers.size());
        for (Long ledgerId : ledgers) {
            try {
                ledgerUnderreplicationManager.markLedgerUnderreplicated(
                        ledgerId, bookieIP);
            } catch (UnavailableException ue) {
                throw new BKAuditException(
                        "Failed to publish underreplicated ledger: " + ledgerId
                                + " of bookie: " + bookieIP, ue);
            }
        }
    }

    /**
     * Process the result returned from checking a ledger.
     */
    private class ProcessLostFragmentsCb implements GenericCallback<Set<LedgerFragment>> {
        final LedgerHandle lh;
        final AsyncCallback.VoidCallback callback;

        ProcessLostFragmentsCb(LedgerHandle lh, AsyncCallback.VoidCallback callback) {
            this.lh = lh;
            this.callback = callback;
        }

        public void operationComplete(int rc, Set<LedgerFragment> fragments) {
            try {
                if (rc == BKException.Code.OK) {
                    Set<BookieSocketAddress> bookies = Sets.newHashSet();
                    for (LedgerFragment f : fragments) {
                        bookies.addAll(f.getAddresses());
                    }
                    for (BookieSocketAddress bookie : bookies) {
                        publishSuspectedLedgers(bookie.toString(), Sets.newHashSet(lh.getId()));
                    }
                }
                lh.close();
            } catch (BKException bke) {
                LOG.error("Error closing lh", bke);
                if (rc == BKException.Code.OK) {
                    rc = BKException.Code.ReplicationException;
                }
            } catch (InterruptedException ie) {
                LOG.error("Interrupted publishing suspected ledger", ie);
                Thread.currentThread().interrupt();
                if (rc == BKException.Code.OK) {
                    rc = BKException.Code.InterruptedException;
                }
            } catch (BKAuditException bkae) {
                LOG.error("Auditor exception publishing suspected ledger", bkae);
                if (rc == BKException.Code.OK) {
                    rc = BKException.Code.ReplicationException;
                }
            }

            callback.processResult(rc, null, null);
        }
    }

    /**
     * List all the ledgers and check them individually. This should not
     * be run very often.
     */
    void checkAllLedgers() throws BKAuditException, BKException,
            IOException, InterruptedException, KeeperException {
        ZooKeeper newzk = ZooKeeperClient.newBuilder()
                .connectString(ZKMetadataDriverBase.resolveZkServers(conf))
                .sessionTimeoutMs(conf.getZkTimeout())
                .build();

        final BookKeeper client = new BookKeeper(new ClientConfiguration(conf),
                                                 newzk);
        final BookKeeperAdmin admin = new BookKeeperAdmin(client, statsLogger);

        try {
            final LedgerChecker checker = new LedgerChecker(client);

            final AtomicInteger returnCode = new AtomicInteger(BKException.Code.OK);
            final CountDownLatch processDone = new CountDownLatch(1);

            Processor<Long> checkLedgersProcessor = new Processor<Long>() {
                @Override
                public void process(final Long ledgerId,
                                    final AsyncCallback.VoidCallback callback) {
                    try {
                        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                            LOG.info("Ledger rereplication has been disabled, aborting periodic check");
                            processDone.countDown();
                            return;
                        }
                    } catch (ReplicationException.UnavailableException ue) {
                        LOG.error("Underreplication manager unavailable running periodic check", ue);
                        processDone.countDown();
                        return;
                    }

                    LedgerHandle lh = null;
                    try {
                        lh = admin.openLedgerNoRecovery(ledgerId);
                        checker.checkLedger(lh,
                            new ProcessLostFragmentsCb(lh, callback),
                            conf.getAuditorLedgerVerificationPercentage());
                        // we collect the following stats to get a measure of the
                        // distribution of a single ledger within the bk cluster
                        // the higher the number of fragments/bookies, the more distributed it is
                        numFragmentsPerLedger.registerSuccessfulValue(lh.getNumFragments());
                        numBookiesPerLedger.registerSuccessfulValue(lh.getNumBookies());
                        numLedgersChecked.inc();
                    } catch (BKException.BKNoSuchLedgerExistsException bknsle) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ledger was deleted before we could check it", bknsle);
                        }
                        callback.processResult(BKException.Code.OK,
                                               null, null);
                        return;
                    } catch (BKException bke) {
                        LOG.error("Couldn't open ledger " + ledgerId, bke);
                        callback.processResult(BKException.Code.BookieHandleNotAvailableException,
                                         null, null);
                        return;
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted opening ledger", ie);
                        Thread.currentThread().interrupt();
                        callback.processResult(BKException.Code.InterruptedException, null, null);
                        return;
                    } finally {
                        if (lh != null) {
                            try {
                                lh.close();
                            } catch (BKException bke) {
                                LOG.warn("Couldn't close ledger " + ledgerId, bke);
                            } catch (InterruptedException ie) {
                                LOG.warn("Interrupted closing ledger " + ledgerId, ie);
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
            };

            ledgerManager.asyncProcessLedgers(checkLedgersProcessor,
                    new AsyncCallback.VoidCallback() {
                        @Override
                        public void processResult(int rc, String s, Object obj) {
                            returnCode.set(rc);
                            processDone.countDown();
                        }
                    }, null, BKException.Code.OK, BKException.Code.ReadException);
            try {
                processDone.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BKAuditException(
                        "Exception while checking ledgers", e);
            }
            if (returnCode.get() != BKException.Code.OK) {
                throw BKException.create(returnCode.get());
            }
        } finally {
            admin.close();
            client.close();
            newzk.close();
        }
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
            admin.close();
            bkc.close();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down auditor bookie", ie);
        } catch (BKException bke) {
            LOG.warn("Exception while shutting down auditor bookie", bke);
        }
    }

    /**
     * Return true if auditor is running otherwise return false.
     *
     * @return auditor status
     */
    public boolean isRunning() {
        return !executor.isShutdown();
    }

    private final Runnable bookieCheck = new Runnable() {
            public void run() {
                if (auditTask == null) {
                    startAudit(true);
                } else {
                    // if due to a lost bookie an audit task was scheduled,
                    // let us not run this periodic bookie check now, if we
                    // went ahead, we'll report under replication and the user
                    // wanted to avoid that(with lostBookieRecoveryDelay option)
                    LOG.info("Audit already scheduled; skipping periodic bookie check");
                }
            }
        };

    int getLostBookieRecoveryDelayBeforeChange() {
        return lostBookieRecoveryDelayBeforeChange;
    }

    Future<?> getAuditTask() {
        return auditTask;
    }
}
