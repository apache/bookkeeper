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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException.BKBookieHandleNotAvailableException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BKException.BKReadException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplicationWorker will take the fragments one by one from
 * ZKLedgerUnderreplicationManager and replicates to it.
 */
public class ReplicationWorker implements Runnable {
    private static Logger LOG = LoggerFactory
            .getLogger(ReplicationWorker.class);
    private LedgerUnderreplicationManager underreplicationManager;
    private AbstractConfiguration conf;
    private ZooKeeper zkc;
    private volatile boolean workerRunning = false;
    private BookKeeperAdmin admin;
    private LedgerChecker ledgerChecker;
    private InetSocketAddress targetBookie;
    private BookKeeper bkc;
    private Thread workerThread;

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     * 
     * @param zkc
     *            - ZK instance
     * @param conf
     *            - configurations
     * @param targetBKAddr
     *            - to where replication should happen. Ideally this will be
     *            local Bookie address.
     */
    public ReplicationWorker(final ZooKeeper zkc,
            final AbstractConfiguration conf, InetSocketAddress targetBKAddr)
            throws CompatibilityException, KeeperException,
            InterruptedException, IOException {
        this.zkc = zkc;
        this.conf = conf;
        this.targetBookie = targetBKAddr;
        LedgerManagerFactory mFactory = LedgerManagerFactory
                .newLedgerManagerFactory(this.conf, this.zkc);
        this.underreplicationManager = mFactory
                .newLedgerUnderreplicationManager();
        this.bkc = new BookKeeper(new ClientConfiguration(conf), zkc);
        this.admin = new BookKeeperAdmin(bkc);
        this.ledgerChecker = new LedgerChecker(bkc);
        this.workerThread = new Thread(this);
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
                long ledgerIdToReplicate = underreplicationManager
                        .getLedgerToRereplicate();
                LOG.info("Going to replicate the fragments of the ledger: "
                        + ledgerIdToReplicate);
                LedgerHandle lh;
                try {
                    lh = admin.openLedgerNoRecovery(ledgerIdToReplicate);
                } catch (BKNoSuchLedgerExistsException e) {
                    // Ledger might have been deleted by user
                    LOG.info("BKNoSuchLedgerExistsException while opening "
                            + "ledger for replication. Other clients "
                            + "might have deleted the ledger. "
                            + "So, no harm to continue");
                    underreplicationManager
                            .markLedgerReplicated(ledgerIdToReplicate);
                    continue;
                } catch (BKReadException e) {
                    LOG.info("BKReadException while"
                            + " opening ledger for replication."
                            + " Enough Bookies might not have available"
                            + "So, no harm to continue");
                    underreplicationManager
                            .releaseUnderreplicatedLedger(ledgerIdToReplicate);
                    continue;
                } catch (BKBookieHandleNotAvailableException e) {
                    LOG.info("BKBookieHandleNotAvailableException while"
                            + " opening ledger for replication."
                            + " Enough Bookies might not have available"
                            + "So, no harm to continue");
                    underreplicationManager
                            .releaseUnderreplicatedLedger(ledgerIdToReplicate);
                    continue;
                }

                boolean isAllFragmentsReplicated = doReplicateFragments(
                        ledgerIdToReplicate, lh);
                if (!isAllFragmentsReplicated) {
                    // Releasing the underReplication ledger lock and compete
                    // for the replication again for the pending fragments
                    underreplicationManager
                            .releaseUnderreplicatedLedger(ledgerIdToReplicate);
                    continue;
                }

                LOG.info("Ledger replicated successfully. ledger id is: "
                        + ledgerIdToReplicate);
                underreplicationManager
                        .markLedgerReplicated(ledgerIdToReplicate);
            } catch (InterruptedException e) {
                shutdown();
                Thread.currentThread().interrupt();
                LOG.info("InterruptedException "
                        + "while replicating fragments", e);
                return;
            } catch (BKException e) {
                shutdown();
                LOG.error("BKException while replicating fragments", e);
                return;
            } catch (UnavailableException e) {
                shutdown();
                LOG.error("UnavailableException "
                        + "while replicating fragments", e);
                return;
            }
        }
    }

    /**
     * Replicates the under replicated fragments from failed bookie ledger to
     * targetBookie
     * 
     * @return - false if the re-replication fails for any fragment. Also
     *         returns false if fragment ensemble contains the target bookie
     *         (since target bookie already present in the current fragment
     *         ensemble, it will skip replication for that particular
     *         fragments). Returns true if all fragments replicated
     *         successfully.
     * @throws BKException 
     */
    private boolean doReplicateFragments(long ledgerIdToReplicate,
            LedgerHandle lh) throws InterruptedException, BKException {
        CheckerCallback checkerCb = new CheckerCallback();
        ledgerChecker.checkLedger(lh, checkerCb);
        Set<LedgerFragment> fragments = checkerCb.waitAndGetResult();
        LOG.info("Founds fragments " + fragments
                + " for replication from ledger: " + ledgerIdToReplicate);
        boolean isTargetBookieExistsInFragmentEnsemble = false;
        boolean isAllFragmentsReplicated = true;
        for (LedgerFragment ledgerFragment : fragments) {
            if (isTargetBookieExistsInFragmentEnsemble(lh, ledgerFragment)) {
                LOG.info("Target Bookie[" + targetBookie
                        + "] found in the fragment ensemble:"
                        + ledgerFragment.getEnsemble());
                isTargetBookieExistsInFragmentEnsemble = true;
                continue;
            }
            try {
                admin.replicateLedgerFragment(lh, ledgerFragment, targetBookie);
            } catch (BKException.BKBookieHandleNotAvailableException e) {
                LOG.warn("BKBookieHandleNotAvailableException "
                        + "while replicating the fragment", e);
                isAllFragmentsReplicated = false;
            } catch (BKException.BKLedgerRecoveryException e) {
                LOG.warn("BKLedgerRecoveryException "
                        + "while replicating the fragment", e);
                isAllFragmentsReplicated = false;
            }

        }
        if (isTargetBookieExistsInFragmentEnsemble) {
            LOG.info("Releasing the lock, as target Bookie found"
                    + " in the fragments ensemble.");
            return false;
        }

        // There might be some connectivity issues while replicating,
        // so, still I am eligible to replicate it, lets retry.
        if (!isAllFragmentsReplicated) {
            LOG.info("Could not replicate all fragments."
                    + "So, Releasing the lock. Let's compete "
                    + "for the replication again");
            return false;
        }

        // Re-replication success
        return true;

    }

    /**
     * Stop the replication worker service
     */
    public void shutdown() {
        workerRunning = false;
        try {
            underreplicationManager.close();
        } catch (UnavailableException e) {
            LOG.warn("Exception while closing the "
                    + "ZkLedgerUnderrepliationManager", e);
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
            this.workerThread.interrupt();
            this.workerThread.join();
        } catch (InterruptedException e) {
            LOG.error("Interrupted during shutting down replication worker : ",
                    e);
            Thread.currentThread().interrupt();
        }
    }

    private boolean isTargetBookieExistsInFragmentEnsemble(LedgerHandle lh,
            LedgerFragment ledgerFragment) {
        List<InetSocketAddress> ensemble = ledgerFragment.getEnsemble();
        for (InetSocketAddress bkAddr : ensemble) {
            if (targetBookie.equals(bkAddr)) {
                return true;
            }
        }
        return false;
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

}
