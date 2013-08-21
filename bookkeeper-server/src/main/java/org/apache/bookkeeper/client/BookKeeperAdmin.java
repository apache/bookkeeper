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
package org.apache.bookkeeper.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.SyncOpenCallback;
import org.apache.bookkeeper.client.LedgerFragmentReplicator.SingleFragmentCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin client for BookKeeper clusters
 */
public class BookKeeperAdmin {
    private static Logger LOG = LoggerFactory.getLogger(BookKeeperAdmin.class);
    // ZK client instance
    private ZooKeeper zk;
    // ZK ledgers related String constants
    private final String bookiesPath;

    // BookKeeper client instance
    private BookKeeper bkc;
    
    // LedgerFragmentReplicator instance
    private LedgerFragmentReplicator lfr;

    /*
     * Random number generator used to choose an available bookie server to
     * replicate data from a dead bookie.
     */
    private Random rand = new Random();

    /**
     * Constructor that takes in a ZooKeeper servers connect string so we know
     * how to connect to ZooKeeper to retrieve information about the BookKeeper
     * cluster. We need this before we can do any type of admin operations on
     * the BookKeeper cluster.
     *
     * @param zkServers
     *            Comma separated list of hostname:port pairs for the ZooKeeper
     *            servers cluster.
     * @throws IOException
     *             throws this exception if there is an error instantiating the
     *             ZooKeeper client.
     * @throws InterruptedException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     * @throws KeeperException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(String zkServers) throws IOException, InterruptedException, KeeperException {
        this(new ClientConfiguration().setZkServers(zkServers));
    }

    /**
     * Constructor that takes in a configuration object so we know
     * how to connect to ZooKeeper to retrieve information about the BookKeeper
     * cluster. We need this before we can do any type of admin operations on
     * the BookKeeper cluster.
     *
     * @param conf
     *           Client Configuration Object
     * @throws IOException
     *             throws this exception if there is an error instantiating the
     *             ZooKeeper client.
     * @throws InterruptedException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     * @throws KeeperException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(ClientConfiguration conf) throws IOException, InterruptedException, KeeperException {
        // Create the ZooKeeper client instance
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(conf.getZkTimeout());
        zk = ZkUtils.createConnectedZookeeperClient(conf.getZkServers(), w);
        // Create the bookie path
        bookiesPath = conf.getZkAvailableBookiesPath();
        // Create the BookKeeper client instance
        bkc = new BookKeeper(conf, zk);
        this.lfr = new LedgerFragmentReplicator(bkc);
    }

    /**
     * Constructor that takes in a BookKeeper instance . This will be useful,
     * when users already has bk instance ready.
     * 
     * @param bkc
     *            - bookkeeper instance
     */
    public BookKeeperAdmin(final BookKeeper bkc) {
        this.bkc = bkc;
        this.zk = bkc.zk;
        this.bookiesPath = bkc.getConf().getZkAvailableBookiesPath();
        this.lfr = new LedgerFragmentReplicator(bkc);
    }

    /**
     * Gracefully release resources that this client uses.
     *
     * @throws InterruptedException
     *             if there is an error shutting down the clients that this
     *             class uses.
     */
    public void close() throws InterruptedException, BKException {
        bkc.close();
        zk.close();
    }

    /**
     * Get a list of the available bookies.
     *
     * @return a collection of bookie addresses
     */
    public Collection<InetSocketAddress> getAvailableBookies()
            throws BKException {
        return bkc.bookieWatcher.getBookies();
    }

    /**
     * Get a list of readonly bookies
     *
     * @return a collection of bookie addresses
     */
    public Collection<InetSocketAddress> getReadOnlyBookies() {
        return bkc.bookieWatcher.getReadOnlyBookies();
    }

    /**
     * Notify when the available list of bookies changes.
     * This is a one-shot notification. To receive subsequent notifications
     * the listener must be registered again.
     *
     * @param listener the listener to notify
     */
    public void notifyBookiesChanged(final BookiesListener listener)
            throws BKException {
        bkc.bookieWatcher.notifyBookiesChanged(listener);
    }

    /**
     * Open a ledger as an administrator. This means that no digest password
     * checks are done. Otherwise, the call is identical to BookKeeper#asyncOpenLedger
     *
     * @param lId
     *          ledger identifier
     * @param cb
     *          Callback which will receive a LedgerHandle object
     * @param ctx
     *          optional context object, to be passwd to the callback (can be null)
     *
     * @see BookKeeper#asyncOpenLedger
     */
    public void asyncOpenLedger(final long lId, final OpenCallback cb, final Object ctx) {
        new LedgerOpenOp(bkc, lId, cb, ctx).initiate();
    }
    
    /**
     * Open a ledger as an administrator. This means that no digest password
     * checks are done. Otherwise, the call is identical to
     * BookKeeper#openLedger
     * 
     * @param lId
     *            - ledger identifier
     * @see BookKeeper#openLedger
     */
    public LedgerHandle openLedger(final long lId) throws InterruptedException,
            BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();
        new LedgerOpenOp(bkc, lId, new SyncOpenCallback(), counter).initiate();
        /*
         * Wait
         */
        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getLh();
    }

    /**
     * Open a ledger as an administrator without recovering the ledger. This means
     * that no digest password  checks are done. Otherwise, the call is identical
     * to BookKeeper#asyncOpenLedgerNoRecovery
     *
     * @param lId
     *          ledger identifier
     * @param cb
     *          Callback which will receive a LedgerHandle object
     * @param ctx
     *          optional context object, to be passwd to the callback (can be null)
     *
     * @see BookKeeper#asyncOpenLedgerNoRecovery
     */
    public void asyncOpenLedgerNoRecovery(final long lId, final OpenCallback cb, final Object ctx) {
        new LedgerOpenOp(bkc, lId, cb, ctx).initiateWithoutRecovery();
    }
    
    /**
     * Open a ledger as an administrator without recovering the ledger. This
     * means that no digest password checks are done. Otherwise, the call is
     * identical to BookKeeper#openLedgerNoRecovery
     * 
     * @param lId
     *            ledger identifier
     * @see BookKeeper#openLedgerNoRecovery
     */
    public LedgerHandle openLedgerNoRecovery(final long lId)
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();
        new LedgerOpenOp(bkc, lId, new SyncOpenCallback(), counter)
                .initiateWithoutRecovery();
        /*
         * Wait
         */
        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getLh();
    }

    // Object used for calling async methods and waiting for them to complete.
    static class SyncObject {
        boolean value;
        int rc;

        public SyncObject() {
            value = false;
            rc = BKException.Code.OK;
        }
    }

    /**
     * Synchronous method to rebuild and recover the ledger fragments data that
     * was stored on the source bookie. That bookie could have failed completely
     * and now the ledger data that was stored on it is under replicated. An
     * optional destination bookie server could be given if we want to copy all
     * of the ledger fragments data on the failed source bookie to it.
     * Otherwise, we will just randomly distribute the ledger fragments to the
     * active set of bookies, perhaps based on load. All ZooKeeper ledger
     * metadata will be updated to point to the new bookie(s) that contain the
     * replicated ledger fragments.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     */
    public void recoverBookieData(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest)
            throws InterruptedException, BKException {
        SyncObject sync = new SyncObject();
        // Call the async method to recover bookie data.
        asyncRecoverBookieData(bookieSrc, bookieDest, new RecoverCallback() {
            @Override
            public void recoverComplete(int rc, Object ctx) {
                LOG.info("Recover bookie operation completed with rc: " + rc);
                SyncObject syncObj = (SyncObject) ctx;
                synchronized (syncObj) {
                    syncObj.rc = rc;
                    syncObj.value = true;
                    syncObj.notify();
                }
            }
        }, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
    }

    /**
     * Async method to rebuild and recover the ledger fragments data that was
     * stored on the source bookie. That bookie could have failed completely and
     * now the ledger data that was stored on it is under replicated. An
     * optional destination bookie server could be given if we want to copy all
     * of the ledger fragments data on the failed source bookie to it.
     * Otherwise, we will just randomly distribute the ledger fragments to the
     * active set of bookies, perhaps based on load. All ZooKeeper ledger
     * metadata will be updated to point to the new bookie(s) that contain the
     * replicated ledger fragments.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    public void asyncRecoverBookieData(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                       final RecoverCallback cb, final Object context) {
        // Sync ZK to make sure we're reading the latest bookie data.
        zk.sync(bookiesPath, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error syncing: ", KeeperException.create(KeeperException.Code.get(rc), path));
                    cb.recoverComplete(BKException.Code.ZKException, context);
                    return;
                }
                getAvailableBookies(bookieSrc, bookieDest, cb, context);
            };
        }, null);
    }

    /**
     * This method asynchronously gets the set of available Bookies that the
     * dead input bookie's data will be copied over into. If the user passed in
     * a specific destination bookie, then just use that one. Otherwise, we'll
     * randomly pick one of the other available bookies to use for each ledger
     * fragment we are replicating.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    private void getAvailableBookies(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                     final RecoverCallback cb, final Object context) {
        final List<InetSocketAddress> availableBookies = new LinkedList<InetSocketAddress>();
        if (bookieDest != null) {
            availableBookies.add(bookieDest);
            // Now poll ZK to get the active ledgers
            getActiveLedgers(bookieSrc, bookieDest, cb, context, availableBookies);
        } else {
            zk.getChildren(bookiesPath, null, new AsyncCallback.ChildrenCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    if (rc != Code.OK.intValue()) {
                        LOG.error("ZK error getting bookie nodes: ", KeeperException.create(KeeperException.Code
                                  .get(rc), path));
                        cb.recoverComplete(BKException.Code.ZKException, context);
                        return;
                    }
                    for (String bookieNode : children) {
                        if (BookKeeperConstants.READONLY
                                        .equals(bookieNode)) {
                            // exclude the readonly node from available bookies.
                            continue;
                        }
                        String parts[] = bookieNode.split(BookKeeperConstants.COLON);
                        if (parts.length < 2) {
                            LOG.error("Bookie Node retrieved from ZK has invalid name format: " + bookieNode);
                            cb.recoverComplete(BKException.Code.ZKException, context);
                            return;
                        }
                        availableBookies.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                    }
                    // Now poll ZK to get the active ledgers
                    getActiveLedgers(bookieSrc, null, cb, context, availableBookies);
                }
            }, null);
        }
    }

    /**
     * This method asynchronously polls ZK to get the current set of active
     * ledgers. From this, we can open each ledger and look at the metadata to
     * determine if any of the ledger fragments for it were stored at the dead
     * input bookie.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     * @param availableBookies
     *            List of Bookie Servers that are available to use for
     *            replicating data on the failed bookie. This could contain a
     *            single bookie server if the user explicitly chose a bookie
     *            server to replicate data to.
     */
    private void getActiveLedgers(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                  final RecoverCallback cb, final Object context, final List<InetSocketAddress> availableBookies) {
        // Wrapper class around the RecoverCallback so it can be used
        // as the final VoidCallback to process ledgers
        class RecoverCallbackWrapper implements AsyncCallback.VoidCallback {
            final RecoverCallback cb;

            RecoverCallbackWrapper(RecoverCallback cb) {
                this.cb = cb;
            }

            @Override
            public void processResult(int rc, String path, Object ctx) {
                cb.recoverComplete(rc, ctx);
            }
        }

        Processor<Long> ledgerProcessor = new Processor<Long>() {
            @Override
            public void process(Long ledgerId, AsyncCallback.VoidCallback iterCallback) {
                recoverLedger(bookieSrc, ledgerId, iterCallback, availableBookies);
            }
        };
        bkc.getLedgerManager().asyncProcessLedgers(
            ledgerProcessor, new RecoverCallbackWrapper(cb),
            context, BKException.Code.OK, BKException.Code.LedgerRecoveryException);
    }

    /**
     * Get a new random bookie, but ensure that it isn't one that is already
     * in the ensemble for the ledger.
     */
    private InetSocketAddress getNewBookie(final List<InetSocketAddress> bookiesAlreadyInEnsemble, 
                                           final List<InetSocketAddress> availableBookies) 
            throws BKException.BKNotEnoughBookiesException {
        ArrayList<InetSocketAddress> candidates = new ArrayList<InetSocketAddress>();
        candidates.addAll(availableBookies);
        candidates.removeAll(bookiesAlreadyInEnsemble);
        if (candidates.size() == 0) {
            throw new BKException.BKNotEnoughBookiesException();
        }
        return candidates.get(rand.nextInt(candidates.size()));
    }

    /**
     * This method asynchronously recovers a given ledger if any of the ledger
     * entries were stored on the failed bookie.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param lId
     *            Ledger id we want to recover.
     * @param ledgerIterCb
     *            IterationCallback to invoke once we've recovered the current
     *            ledger.
     * @param availableBookies
     *            List of Bookie Servers that are available to use for
     *            replicating data on the failed bookie. This could contain a
     *            single bookie server if the user explicitly chose a bookie
     *            server to replicate data to.
     */
    private void recoverLedger(final InetSocketAddress bookieSrc, final long lId,
                               final AsyncCallback.VoidCallback ledgerIterCb, final List<InetSocketAddress> availableBookies) {
        LOG.debug("Recovering ledger : {}", lId);

        asyncOpenLedgerNoRecovery(lId, new OpenCallback() {
            @Override
            public void openComplete(int rc, final LedgerHandle lh, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("BK error opening ledger: " + lId, BKException.create(rc));
                    ledgerIterCb.processResult(rc, null, null);
                    return;
                }

                LedgerMetadata lm = lh.getLedgerMetadata();
                if (!lm.isClosed() &&
                    lm.getEnsembles().size() > 0) {
                    Long lastKey = lm.getEnsembles().lastKey();
                    ArrayList<InetSocketAddress> lastEnsemble = lm.getEnsembles().get(lastKey);
                    // the original write has not removed faulty bookie from
                    // current ledger ensemble. to avoid data loss issue in
                    // the case of concurrent updates to the ensemble composition,
                    // the recovery tool should first close the ledger
                    if (lastEnsemble.contains(bookieSrc)) {
                        // close opened non recovery ledger handle
                        try {
                            lh.close();
                        } catch (Exception ie) {
                            LOG.warn("Error closing non recovery ledger handle for ledger " + lId, ie);
                        }
                        asyncOpenLedger(lId, new OpenCallback() {
                            @Override
                            public void openComplete(int newrc, final LedgerHandle newlh, Object newctx) {
                                if (newrc != Code.OK.intValue()) {
                                    LOG.error("BK error close ledger: " + lId, BKException.create(newrc));
                                    ledgerIterCb.processResult(newrc, null, null);
                                    return;
                                }
                                // do recovery
                                recoverLedger(bookieSrc, lId, ledgerIterCb, availableBookies);
                            }
                        }, null);
                        return;
                    }
                }

                /*
                 * This List stores the ledger fragments to recover indexed by
                 * the start entry ID for the range. The ensembles TreeMap is
                 * keyed off this.
                 */
                final List<Long> ledgerFragmentsToRecover = new LinkedList<Long>();
                /*
                 * This Map will store the start and end entry ID values for
                 * each of the ledger fragment ranges. The only exception is the
                 * current active fragment since it has no end yet. In the event
                 * of a bookie failure, a new ensemble is created so the current
                 * ensemble should not contain the dead bookie we are trying to
                 * recover.
                 */
                Map<Long, Long> ledgerFragmentsRange = new HashMap<Long, Long>();
                Long curEntryId = null;
                for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : lh.getLedgerMetadata().getEnsembles()
                         .entrySet()) {
                    if (curEntryId != null)
                        ledgerFragmentsRange.put(curEntryId, entry.getKey() - 1);
                    curEntryId = entry.getKey();
                    if (entry.getValue().contains(bookieSrc)) {
                        /*
                         * Current ledger fragment has entries stored on the
                         * dead bookie so we'll need to recover them.
                         */
                        ledgerFragmentsToRecover.add(entry.getKey());
                    }
                }
                // add last ensemble otherwise if the failed bookie existed in
                // the last ensemble of a closed ledger. the entries belonged to
                // last ensemble would not be replicated.
                if (curEntryId != null) {
                    ledgerFragmentsRange.put(curEntryId, lh.getLastAddConfirmed());
                }
                /*
                 * See if this current ledger contains any ledger fragment that
                 * needs to be re-replicated. If not, then just invoke the
                 * multiCallback and return.
                 */
                if (ledgerFragmentsToRecover.size() == 0) {
                    ledgerIterCb.processResult(BKException.Code.OK, null, null);
                    return;
                }

                /*
                 * Multicallback for ledger. Once all fragments for the ledger have been recovered
                 * trigger the ledgerIterCb
                 */
                MultiCallback ledgerFragmentsMcb
                    = new MultiCallback(ledgerFragmentsToRecover.size(), ledgerIterCb, null,
                                        BKException.Code.OK, BKException.Code.LedgerRecoveryException);
                /*
                 * Now recover all of the necessary ledger fragments
                 * asynchronously using a MultiCallback for every fragment.
                 */
                for (final Long startEntryId : ledgerFragmentsToRecover) {
                    Long endEntryId = ledgerFragmentsRange.get(startEntryId);
                    InetSocketAddress newBookie = null;
                    try {
                        newBookie = getNewBookie(lh.getLedgerMetadata().getEnsembles().get(startEntryId),
                                                 availableBookies);
                    } catch (BKException.BKNotEnoughBookiesException bke) {
                        ledgerFragmentsMcb.processResult(BKException.Code.NotEnoughBookiesException, 
                                                         null, null);
                        continue;
                    }
                    
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Replicating fragment from [" + startEntryId 
                                  + "," + endEntryId + "] of ledger " + lh.getId()
                                  + " to " + newBookie);
                    }
                    try {
                        LedgerFragmentReplicator.SingleFragmentCallback cb = new LedgerFragmentReplicator.SingleFragmentCallback(
                                                                               ledgerFragmentsMcb, lh, startEntryId, bookieSrc, newBookie);
                        ArrayList<InetSocketAddress> currentEnsemble =  lh.getLedgerMetadata().getEnsemble(startEntryId);
                        int bookieIndex = -1;
                        if (null != currentEnsemble) {
                            for (int i = 0; i < currentEnsemble.size(); i++) {
                                if (currentEnsemble.get(i).equals(bookieSrc)) {
                                    bookieIndex = i;
                                    break;
                                }
                            }
                        }
                        LedgerFragment ledgerFragment = new LedgerFragment(lh,
                                startEntryId, endEntryId, bookieIndex);
                        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, newBookie);
                    } catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            }, null);
    }

    /**
     * This method asynchronously recovers a ledger fragment which is a
     * contiguous portion of a ledger that was stored in an ensemble that
     * included the failed bookie.
     * 
     * @param lh
     *            - LedgerHandle for the ledger
     * @param lf
     *            - LedgerFragment to replicate
     * @param ledgerFragmentMcb
     *            - MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param newBookie
     *            - New bookie we want to use to recover and replicate the
     *            ledger entries that were stored on the failed bookie.
     */
    private void asyncRecoverLedgerFragment(final LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final InetSocketAddress newBookie) throws InterruptedException {
        lfr.replicate(lh, ledgerFragment, ledgerFragmentMcb, newBookie);
    }

    /**
     * Replicate the Ledger fragment to target Bookie passed.
     * 
     * @param lh
     *            - ledgerHandle
     * @param ledgerFragment
     *            - LedgerFragment to replicate
     * @param targetBookieAddress
     *            - target Bookie, to where entries should be replicated.
     */
    public void replicateLedgerFragment(LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final InetSocketAddress targetBookieAddress)
            throws InterruptedException, BKException {
        SyncCounter syncCounter = new SyncCounter();
        ResultCallBack resultCallBack = new ResultCallBack(syncCounter);
        SingleFragmentCallback cb = new SingleFragmentCallback(resultCallBack,
                lh, ledgerFragment.getFirstEntryId(), ledgerFragment
                        .getAddress(), targetBookieAddress);
        syncCounter.inc();
        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, targetBookieAddress);
        syncCounter.block(0);
        if (syncCounter.getrc() != BKException.Code.OK) {
            throw BKException.create(syncCounter.getrc());
        }
    }

    /** This is the class for getting the replication result */
    static class ResultCallBack implements AsyncCallback.VoidCallback {
        private SyncCounter sync;

        public ResultCallBack(SyncCounter sync) {
            this.sync = sync;
        }

        @Override
        public void processResult(int rc, String s, Object obj) {
            sync.setrc(rc);
            sync.dec();
        }
    }

    /**
     * Format the BookKeeper metadata in zookeeper
     * 
     * @param isInteractive
     *            Whether format should ask prompt for confirmation if old data
     *            exists or not.
     * @param force
     *            If non interactive and force is true, then old data will be
     *            removed without prompt.
     * @return Returns true if format succeeds else false.
     */
    public static boolean format(ClientConfiguration conf,
            boolean isInteractive, boolean force) throws Exception {
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(conf.getZkTimeout());
        ZooKeeper zkc = ZkUtils.createConnectedZookeeperClient(
                conf.getZkServers(), w);
        BookKeeper bkc = null;
        try {
            boolean ledgerRootExists = null != zkc.exists(
                    conf.getZkLedgersRootPath(), false);
            boolean availableNodeExists = null != zkc.exists(
                    conf.getZkAvailableBookiesPath(), false);

            // Create ledgers root node if not exists
            if (!ledgerRootExists) {
                zkc.create(conf.getZkLedgersRootPath(), "".getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // create available bookies node if not exists
            if (!availableNodeExists) {
                zkc.create(conf.getZkAvailableBookiesPath(), "".getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            // If old data was there then confirm with admin.
            if (ledgerRootExists) {
                boolean confirm = false;
                if (!isInteractive) {
                    // If non interactive and force is set, then delete old
                    // data.
                    if (force) {
                        confirm = true;
                    } else {
                        confirm = false;
                    }
                } else {
                    // Confirm with the admin.
                    confirm = IOUtils
                            .confirmPrompt("Ledger root already exists. "
                                    +"Are you sure to format bookkeeper metadata? "
                                    +"This may cause data loss.");
                }
                if (!confirm) {
                    LOG.error("BookKeeper metadata Format aborted!!");
                    return false;
                }
            }
            bkc = new BookKeeper(conf, zkc);
            // Format all ledger metadata layout
            bkc.ledgerManagerFactory.format(conf, zkc);

            // Clear the cookies
            try {
                ZKUtil.deleteRecursive(zkc, conf.getZkLedgersRootPath()
                        + "/cookies");
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("cookies node not exists in zookeeper to delete");
            }

            // Clear the INSTANCEID
            try {
                zkc.delete(conf.getZkLedgersRootPath() + "/"
                        + BookKeeperConstants.INSTANCEID, -1);
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("INSTANCEID not exists in zookeeper to delete");
            }

            // create INSTANCEID
            String instanceId = UUID.randomUUID().toString();
            zkc.create(conf.getZkLedgersRootPath() + "/"
                    + BookKeeperConstants.INSTANCEID, instanceId.getBytes(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            LOG.info("Successfully formatted BookKeeper metadata");
        } finally {
            if (null != bkc) {
                bkc.close();
            }
            if (null != zkc) {
                zkc.close();
            }
        }
        return true;
    }

    /**
     * This method returns an iterable object for the list of ledger identifiers of
     * the ledgers currently available.
     *
     * @return an iterable object for the list of ledger identifiers
     * @throws IOException  if the list of ledger identifiers cannot be read from the
     *  metadata store
     */
    public Iterable<Long> listLedgers()
    throws IOException {
        final LedgerRangeIterator iterator = bkc.getLedgerManager().getLedgerRanges();
        return new Iterable<Long>() {
            public Iterator<Long> iterator() {
                return new Iterator<Long>() {
                    Iterator<Long> currentRange = null;

                    @Override
                    public boolean hasNext() {
                        try {
                            if (iterator.hasNext()) {
                                LOG.info("I'm in this part of");
                                return true;
                            } else if (currentRange != null) {
                                if (currentRange.hasNext()) {
                                    return true;
                                }
                            }
                        } catch (IOException e) {
                            LOG.error("Error while checking if there is a next element", e);
                        }

                        return false;
                    }

                    @Override
                    public Long next()
                    throws NoSuchElementException {
                        try{
                            if (currentRange == null) {
                                currentRange = iterator.next().getLedgers().iterator();
                            }
                        } catch (IOException e) {
                            LOG.error("Error while reading the next element", e);
                            throw new NoSuchElementException(e.getMessage());
                        }

                        return currentRange.next();
                    }

                    @Override
                    public void remove()
                    throws UnsupportedOperationException {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
