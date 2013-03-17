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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import java.net.InetSocketAddress;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.util.StringUtils;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;

import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.commons.collections.CollectionUtils;
import com.google.common.collect.Sets;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback;
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
    private final ServerConfiguration conf;
    private final ZooKeeper zkc;
    private BookieLedgerIndexer bookieLedgerIndexer;
    private LedgerManager ledgerManager;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;
    private final ScheduledExecutorService executor;
    private List<String> knownBookies = new ArrayList<String>();

    public Auditor(final String bookieIdentifier, ServerConfiguration conf,
                   ZooKeeper zkc) throws UnavailableException {
        this.conf = conf;
        this.zkc = zkc;
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
            LedgerManagerFactory ledgerManagerFactory = LedgerManagerFactory
                    .newLedgerManagerFactory(conf, zkc);
            ledgerManager = ledgerManagerFactory.newLedgerManager();
            this.bookieLedgerIndexer = new BookieLedgerIndexer(ledgerManager);

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

            long interval = conf.getAuditorPeriodicCheckInterval();
            if (interval > 0) {
                LOG.info("Periodic checking enabled");
                executor.scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            LOG.info("Running periodic check");

                            try {
                                if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                                    LOG.info("Ledger replication disabled, skipping");
                                    return;
                                }

                                checkAllLedgers();
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
                                LOG.error("Underreplication manager unavailable "
                                          +"running periodic check", ue);
                            }
                        }
                    }, interval, interval, TimeUnit.MILLISECONDS);
            } else {
                LOG.info("Periodic checking disabled");
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

    /**
     * Process the result returned from checking a ledger
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
                    Set<InetSocketAddress> bookies = Sets.newHashSet();
                    for (LedgerFragment f : fragments) {
                        bookies.add(f.getAddress());
                    }
                    for (InetSocketAddress bookie : bookies) {
                        publishSuspectedLedgers(StringUtils.addrToString(bookie),
                                                Sets.newHashSet(lh.getId()));
                    }
                }
                lh.close();
            } catch (BKException bke) {
                LOG.error("Error closing lh", bke);
                if (rc == BKException.Code.OK) {
                    rc = BKException.Code.ZKException;
                }
            } catch (KeeperException ke) {
                LOG.error("Couldn't publish suspected ledger", ke);
                if (rc == BKException.Code.OK) {
                    rc = BKException.Code.ZKException;
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
                    rc = BKException.Code.ZKException;
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
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(conf.getZkTimeout());
        ZooKeeper newzk = ZkUtils.createConnectedZookeeperClient(conf.getZkServers(), w);

        final BookKeeper client = new BookKeeper(new ClientConfiguration(conf),
                                                 newzk);
        final BookKeeperAdmin admin = new BookKeeperAdmin(client);

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
                        LOG.error("Underreplication manager unavailable "
                                  +"running periodic check", ue);
                        processDone.countDown();
                        return;
                    }

                    LedgerHandle lh = null;
                    try {
                        lh = admin.openLedgerNoRecovery(ledgerId);
                        checker.checkLedger(lh, new ProcessLostFragmentsCb(lh, callback));
                    } catch (BKException.BKNoSuchLedgerExistsException bknsle) {
                        LOG.debug("Ledger was deleted before we could check it", bknsle);
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
