package org.apache.bookkeeper.client;

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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Admin client for BookKeeper clusters
 */
public class BookKeeperAdmin {
    private static Logger LOG = LoggerFactory.getLogger(BookKeeperAdmin.class);

    static final String COLON = ":";

    // ZK client instance
    private ZooKeeper zk;
    // ZK ledgers related String constants
    private final String bookiesPath;

    // BookKeeper client instance
    private BookKeeper bkc;

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
        final CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper(conf.getZkServers(), conf.getZkTimeout(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                latch.countDown();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Process: " + event.getType() + " " + event.getPath());
                }
            }
        });
        if (!latch.await(conf.getZkTimeout(), TimeUnit.MILLISECONDS)
            || !zk.getState().isConnected()) {
            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
        }
        // Create the bookie path
        bookiesPath = conf.getZkAvailableBookiesPath();
        // Create the BookKeeper client instance
        bkc = new BookKeeper(conf, zk);
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
                        String parts[] = bookieNode.split(COLON);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering ledger : " + lId);
        }

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
                        SingleFragmentCallback cb = new SingleFragmentCallback(
                                                                               ledgerFragmentsMcb, lh, startEntryId, bookieSrc, newBookie);
                        recoverLedgerFragment(bookieSrc, lh, startEntryId, endEntryId, cb, newBookie);
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
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param lh
     *            LedgerHandle for the ledger
     * @param startEntryId
     *            Start entry Id for the ledger fragment
     * @param endEntryId
     *            End entry Id for the ledger fragment
     * @param ledgerFragmentMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param newBookie
     *            New bookie we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    private void recoverLedgerFragment(final InetSocketAddress bookieSrc, final LedgerHandle lh,
                                       final Long startEntryId, final Long endEntryId, final SingleFragmentCallback cb,
                                       final InetSocketAddress newBookie) throws InterruptedException {
        if (endEntryId == null) {
            /*
             * Ideally this should never happen if bookie failure is taken care
             * of properly. Nothing we can do though in this case.
             */
            LOG.warn("Dead bookie (" + bookieSrc + ") is still part of the current active ensemble for ledgerId: "
                     + lh.getId());
            cb.processResult(BKException.Code.OK, null, null);
            return;
        }
        if (startEntryId > endEntryId) {
            // for open ledger which there is no entry, the start entry id is 0, the end entry id is -1.
            // we can return immediately to trigger forward read
            cb.processResult(BKException.Code.OK, null, null);
            return;
        }

        ArrayList<InetSocketAddress> curEnsemble = lh.getLedgerMetadata().getEnsembles().get(startEntryId);
        int bookieIndex = 0;
        for (int i = 0; i < curEnsemble.size(); i++) {
            if (curEnsemble.get(i).equals(bookieSrc)) {
                bookieIndex = i;
                break;
            }
        }
        /*
         * Loop through all entries in the current ledger fragment range and
         * find the ones that were stored on the dead bookie.
         */
        List<Long> entriesToReplicate = new LinkedList<Long>();
        for (long i = startEntryId; i <= endEntryId; i++) {
            if (lh.getDistributionSchedule().getReplicaIndex(i, bookieIndex) >= 0) {
                /*
                 * Current entry is stored on the dead bookie so we'll need to
                 * read it and replicate it to a new bookie.
                 */
                entriesToReplicate.add(i);
            }
        }
        /*
         * Now asynchronously replicate all of the entries for the ledger
         * fragment that were on the dead bookie.
         */
        MultiCallback ledgerFragmentEntryMcb =
            new MultiCallback(entriesToReplicate.size(), cb, null,
                              BKException.Code.OK, BKException.Code.LedgerRecoveryException);
        for (final Long entryId : entriesToReplicate) {
            recoverLedgerFragmentEntry(entryId, lh, ledgerFragmentEntryMcb, newBookie);
        }
    }

    /**
     * This method asynchronously recovers a specific ledger entry by reading
     * the values via the BookKeeper Client (which would read it from the other
     * replicas) and then writing it to the chosen new bookie.
     *
     * @param entryId
     *            Ledger Entry ID to recover.
     * @param lh
     *            LedgerHandle for the ledger
     * @param ledgerFragmentEntryMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger entry.
     * @param newBookie
     *            New bookie we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    private void recoverLedgerFragmentEntry(final Long entryId, final LedgerHandle lh,
                                            final AsyncCallback.VoidCallback ledgerFragmentEntryMcb,
                                            final InetSocketAddress newBookie) throws InterruptedException {
        /*
         * Read the ledger entry using the LedgerHandle. This will allow us to
         * read the entry from one of the other replicated bookies other than
         * the dead one.
         */
        lh.asyncReadEntries(entryId, entryId, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("BK error reading ledger entry: " + entryId, BKException.create(rc));
                    ledgerFragmentEntryMcb.processResult(rc, null, null);
                    return;
                }
                /*
                 * Now that we've read the ledger entry, write it to the new
                 * bookie we've selected.
                 */
                LedgerEntry entry = seq.nextElement();
                byte[] data = entry.getEntry();
                ChannelBuffer toSend = lh.getDigestManager().computeDigestAndPackageForSending(entryId,
                                       lh.getLastAddConfirmed(), entry.getLength(), data, 0, data.length);
                bkc.getBookieClient().addEntry(newBookie, lh.getId(), lh.getLedgerKey(), entryId, toSend,
                new WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr,
                    Object ctx) {
                        if (rc != Code.OK.intValue()) {
                            LOG.error("BK error writing entry for ledgerId: " + ledgerId + ", entryId: "
                                      + entryId + ", bookie: " + addr, BKException.create(rc));
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Success writing ledger id " +ledgerId + ", entry id "
                                          + entryId + " to a new bookie " + addr + "!");
                            }
                        }
                        /*
                         * Pass the return code result up the chain with
                         * the parent callback.
                         */
                        ledgerFragmentEntryMcb.processResult(rc, null, null);
                    }
                }, null, BookieProtocol.FLAG_RECOVERY_ADD);
            }
        }, null);
    }

    /*
     * Callback for recovery of a single ledger fragment.
     * Once the fragment has had all entries replicated, update the ensemble 
     * in zookeeper.
     * Once finished propogate callback up to ledgerFragmentsMcb which should
     * be a multicallback responsible for all fragments in a single ledger
     */
    static class SingleFragmentCallback implements AsyncCallback.VoidCallback {
        final AsyncCallback.VoidCallback ledgerFragmentsMcb;
        final LedgerHandle lh;
        final long fragmentStartId;
        final InetSocketAddress oldBookie;
        final InetSocketAddress newBookie;

        SingleFragmentCallback(AsyncCallback.VoidCallback ledgerFragmentsMcb,
                               LedgerHandle lh,
                               long fragmentStartId,
                               InetSocketAddress oldBookie,
                               InetSocketAddress newBookie) {
            this.ledgerFragmentsMcb = ledgerFragmentsMcb;
            this.lh = lh;
            this.fragmentStartId = fragmentStartId;
            this.newBookie = newBookie;
            this.oldBookie = oldBookie;
        }
        
        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != Code.OK.intValue()) {
                LOG.error("BK error replicating ledger fragments for ledger: " + lh.getId(), 
                          BKException.create(rc));
                ledgerFragmentsMcb.processResult(rc, null, null);
                return;
            }
            writeLedgerConfig();
        }

        protected void writeLedgerConfig() {
            /*
             * Update the ledger metadata's ensemble info to point
             * to the new bookie.
             */
            ArrayList<InetSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().get(
                    fragmentStartId);
            int deadBookieIndex = ensemble.indexOf(oldBookie);
            ensemble.remove(deadBookieIndex);
            ensemble.add(deadBookieIndex, newBookie);
            lh.writeLedgerConfig(new WriteCb());
        }
        
        private class WriteCb implements GenericCallback<Void> {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc == BKException.Code.MetadataVersionException) {
                    LOG.warn("Two fragments attempted update at once; ledger id: " + lh.getId() 
                             + " startid: " + fragmentStartId);
                    // try again, the previous success (with which this has conflicted)
                    // will have updated the stat
                    // other operations such as (addEnsemble) would update it too.
                    lh.rereadMetadata(new GenericCallback<LedgerMetadata>() {
                        @Override
                        public void operationComplete(int rc, LedgerMetadata newMeta) {
                            if (rc != BKException.Code.OK) {
                                LOG.error("Error reading updated ledger metadata for ledger " + lh.getId());
                                ledgerFragmentsMcb.processResult(rc, null, null);
                            } else {
                                lh.metadata = newMeta;
                                writeLedgerConfig();
                            }
                        }
                    });
                    return;
                } else if (rc != BKException.Code.OK) {
                    LOG.error("Error updating ledger config metadata for ledgerId " + lh.getId() + " : "
                            + BKException.getMessage(rc));
                } else {
                    LOG.info("Updated ZK for ledgerId: (" + lh.getId() + " : " + fragmentStartId 
                             + ") to point ledger fragments from old dead bookie: (" + oldBookie
                             + ") to new bookie: (" + newBookie + ")");
                }
                /*
                 * Pass the return code result up the chain with
                 * the parent callback.
                 */
                ledgerFragmentsMcb.processResult(rc, null, null);
            }
        };
    }

}
