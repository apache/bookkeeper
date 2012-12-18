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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auditor is a single entity in the entire Bookie cluster and will be watching
 * all the bookies under 'ledgerrootpath/available' zkpath. When any of the
 * bookie failed or disconnected from zk, he will start initiating the
 * re-replication activities by keeping all the corresponding ledgers of the
 * failed bookie as underreplicated znode in zk.
 */
public class Auditor implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Auditor.class);

    private final AbstractConfiguration conf;
    private final ZooKeeper zkc;
    private BookieLedgerIndexer bookieLedgerIndexer;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private final ExecutorService executor;
    private List<String> knownBookies = new ArrayList<String>();

    public Auditor(final String bookieIdentifier, AbstractConfiguration conf,
            ZooKeeper zkc) throws UnavailableException {
        this.conf = conf;
        this.zkc = zkc;
        initialize(conf, zkc);

        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "AuditorBookie-" + bookieIdentifier);
                    t.setDaemon(true);
                    return t;
                }
            });
    }

    private void initialize(AbstractConfiguration conf, ZooKeeper zkc)
            throws UnavailableException {
        try {
            LedgerManagerFactory ledgerManagerFactory = LedgerManagerFactory
                    .newLedgerManagerFactory(conf, zkc);

            this.bookieLedgerIndexer = new BookieLedgerIndexer(
                    ledgerManagerFactory.newLedgerManager());

            this.ledgerUnderreplicationManager = ledgerManagerFactory
                    .newLedgerUnderreplicationManager();

        } catch (CompatibilityException ce) {
            throw new UnavailableException(
                    "CompatibilityException while initializing Auditor", ce);
        } catch (IOException ioe) {
            throw new UnavailableException(
                    "IOException while initializing Auditor", ioe);
        } catch (KeeperException ke) {
            throw new UnavailableException(
                    "KeeperException while initializing Auditor", ke);
        } catch (InterruptedException ie) {
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

    private synchronized void submitAuditTask() {
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }
            executor.submit(new Runnable() {
                    public void run() {
                        try {
                            waitIfLedgerReplicationDisabled();

                            List<String> availableBookies = getAvailableBookies();

                            // casting to String, as knownBookies and availableBookies
                            // contains only String values
                            // find new bookies(if any) and update the known bookie list
                            Collection<String> newBookies = CollectionUtils.subtract(
                                    availableBookies, knownBookies);
                            knownBookies.addAll(newBookies);

                            // find lost bookies(if any)
                            Collection<String> lostBookies = CollectionUtils.subtract(
                                    knownBookies, availableBookies);

                            if (lostBookies.size() > 0) {
                                knownBookies.removeAll(lostBookies);
                                Map<String, Set<Long>> ledgerDetails = generateBookie2LedgersIndex();
                                handleLostBookies(lostBookies, ledgerDetails);
                            }
                        } catch (KeeperException ke) {
                            LOG.error("Exception while watching available bookies", ke);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            LOG.error("Interrupted while watching available bookies ", ie);
                        } catch (BKAuditException bke) {
                            LOG.error("Exception while watching available bookies", bke);
                        } catch (UnavailableException ue) {
                            LOG.error("Exception while watching available bookies", ue);
                        }
                    }
                });
        }
    }

    public void start() {
        LOG.info("I'm starting as Auditor Bookie");
        // on startup watching available bookie and based on the
        // available bookies determining the bookie failures.
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }
            executor.submit(new Runnable() {
                    public void run() {
                        try {
                            knownBookies = getAvailableBookies();
                            auditingBookies(knownBookies);
                        } catch (KeeperException ke) {
                            LOG.error("Exception while watching available bookies", ke);
                            submitShutdownTask();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            LOG.error("Interrupted while watching available bookies ", ie);
                            submitShutdownTask();
                        } catch (BKAuditException bke) {
                            LOG.error("Exception while watching available bookies", bke);
                            submitShutdownTask();
                        }
                    }
                });
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        ReplicationEnableCb cb = new ReplicationEnableCb();
        if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
            ledgerUnderreplicationManager.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }
    
    private List<String> getAvailableBookies() throws KeeperException,
            InterruptedException {
        return zkc.getChildren(conf.getZkAvailableBookiesPath(), this);
    }

    private void auditingBookies(List<String> availableBookies)
            throws BKAuditException, KeeperException, InterruptedException {

        Map<String, Set<Long>> ledgerDetails = generateBookie2LedgersIndex();

        // find lost bookies
        Set<String> knownBookies = ledgerDetails.keySet();
        Collection<String> lostBookies = CollectionUtils.subtract(knownBookies,
                availableBookies);

        if (lostBookies.size() > 0)
            handleLostBookies(lostBookies, ledgerDetails);
    }

    private Map<String, Set<Long>> generateBookie2LedgersIndex()
            throws BKAuditException {
        return bookieLedgerIndexer.getBookieToLedgerIndex();
    }

    private void handleLostBookies(Collection<String> lostBookies,
            Map<String, Set<Long>> ledgerDetails) throws BKAuditException,
            KeeperException, InterruptedException {
        LOG.info("Following are the failed bookies: " + lostBookies
                + " and searching its ledgers for re-replication");

        for (String bookieIP : lostBookies) {
            // identify all the ledgers in bookieIP and publishing these ledgers
            // as under-replicated.
            publishSuspectedLedgers(bookieIP, ledgerDetails.get(bookieIP));
        }
    }

    private void publishSuspectedLedgers(String bookieIP, Set<Long> ledgers)
            throws KeeperException, InterruptedException, BKAuditException {
        if (null == ledgers || ledgers.size() == 0) {
            // there is no ledgers available for this bookie and just
            // ignoring the bookie failures
            LOG.info("There is no ledgers for the failed bookie: " + bookieIP);
            return;
        }
        LOG.info("Following ledgers: " + ledgers + " of bookie: " + bookieIP
                + " are identified as underreplicated");
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

    @Override
    public void process(WatchedEvent event) {
        // listen children changed event from ZooKeeper
        if (event.getState() == KeeperState.Disconnected
                || event.getState() == KeeperState.Expired) {
            submitShutdownTask();
        } else if (event.getType() == EventType.NodeChildrenChanged) {
            submitAuditTask();
        }
    }

    /**
     * Shutdown the auditor
     */
    public void shutdown() {
        LOG.info("Shutting down auditor");
        submitShutdownTask();

        try {
            while (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Executor not shutting down, interrupting");
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while shutting down auditor bookie", ie);
        }
    }

    /**
     * Return true if auditor is running otherwise return false
     * 
     * @return auditor status
     */
    public boolean isRunning() {
        return !executor.isShutdown();
    }
}
