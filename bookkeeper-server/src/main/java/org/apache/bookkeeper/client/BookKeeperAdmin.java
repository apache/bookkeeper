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
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithMetadataBookieDriver;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.LedgerFragmentReplicator.SingleFragmentCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncOpenCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncReadCallback;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerAuditorManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.BookieLedgerIndexer;
import org.apache.bookkeeper.replication.ReplicationException.BKAuditException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin client for BookKeeper clusters.
 */
public class BookKeeperAdmin implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdmin.class);
    private static final Logger VERBOSE = LoggerFactory.getLogger("verbose");
    private static final BiConsumer<Long, Long> NOOP_BICONSUMER = (l, e) -> { };

    // BookKeeper client instance
    private BookKeeper bkc;
    private final boolean ownsBK;

    // LedgerFragmentReplicator instance
    private LedgerFragmentReplicator lfr;

    private LedgerManagerFactory mFactory;

    /*
     * underreplicationManager is not initialized as part of constructor use its
     * getter (getUnderreplicationManager) so that it can be lazy-initialized
     */
    private LedgerUnderreplicationManager underreplicationManager;

    private LedgerAuditorManager ledgerAuditorManager;

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
     * @throws BKException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(String zkServers) throws IOException, InterruptedException, BKException {
        this(new ClientConfiguration().setMetadataServiceUri("zk+null://" + zkServers + "/ledgers"));
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
     * @throws BKException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(ClientConfiguration conf) throws IOException, InterruptedException, BKException {
        // Create the BookKeeper client instance
        bkc = new BookKeeper(conf);
        ownsBK = true;
        this.lfr = new LedgerFragmentReplicator(bkc, NullStatsLogger.INSTANCE, conf);
        this.mFactory = bkc.ledgerManagerFactory;
    }

    /**
     * Constructor that takes in a BookKeeper instance . This will be useful,
     * when user already has bk instance ready.
     *
     * @param bkc
     *            - bookkeeper instance
     * @param statsLogger
     *            - stats logger
     */
    public BookKeeperAdmin(final BookKeeper bkc, StatsLogger statsLogger, ClientConfiguration conf) {
        Objects.requireNonNull(conf, "Client configuration cannot be null");
        this.bkc = bkc;
        ownsBK = false;
        this.lfr = new LedgerFragmentReplicator(bkc, statsLogger, conf);
        this.mFactory = bkc.ledgerManagerFactory;
    }

    public BookKeeperAdmin(final BookKeeper bkc, ClientConfiguration conf) {
        this(bkc, NullStatsLogger.INSTANCE, conf);
    }

    public BookKeeperAdmin(final BookKeeper bkc) {
        this.bkc = bkc;
        ownsBK = false;
        this.mFactory = bkc.ledgerManagerFactory;
    }

    public ClientConfiguration getConf() {
        return bkc.getConf();
    }

    /**
     * Gracefully release resources that this client uses.
     *
     * @throws InterruptedException
     *             if there is an error shutting down the clients that this
     *             class uses.
     */
    @Override
    public void close() throws InterruptedException, BKException {
        if (ownsBK) {
            bkc.close();
        }

        if (ledgerAuditorManager != null) {
            try {
                ledgerAuditorManager.close();
            } catch (Exception e) {
                throw new BKException.MetaStoreException(e);
            }
        }
    }

    /**
     * Get a list of the available bookies.
     *
     * @return a collection of bookie addresses
     */
    public Collection<BookieId> getAvailableBookies()
            throws BKException {
        return bkc.bookieWatcher.getBookies();
    }

    /**
     * Get a list of all bookies including the not available ones.
     *
     * @return a collection of bookie addresses
     */
    public Collection<BookieId> getAllBookies()
            throws BKException {
        return bkc.bookieWatcher.getAllBookies();
    }

    public BookieAddressResolver getBookieAddressResolver() {
        return bkc.bookieWatcher.getBookieAddressResolver();
    }

    @SneakyThrows
    public BookieServiceInfo getBookieServiceInfo(BookieId bookiedId)
            throws BKException {
        return FutureUtils.result(bkc.getMetadataClientDriver()
                .getRegistrationClient().getBookieServiceInfo(bookiedId)).getValue();
    }

    /**
     * Get a list of readonly bookies synchronously.
     *
     * @return a collection of bookie addresses
     * @throws BKException if there are issues trying to read the list.
     */
    public Collection<BookieId> getReadOnlyBookies() throws BKException {
        return bkc.bookieWatcher.getReadOnlyBookies();
    }

    /**
     * Notify when the available list of bookies changes.
     * This is a one-shot notification. To receive subsequent notifications
     * the listener must be registered again.
     *
     * @param listener the listener to notify
     */
    public void watchWritableBookiesChanged(final RegistrationListener listener)
            throws BKException {
        bkc
            .getMetadataClientDriver()
            .getRegistrationClient()
            .watchWritableBookies(listener);
    }

    /**
     * Notify when the available list of read only bookies changes.
     * This is a one-shot notification. To receive subsequent notifications
     * the listener must be registered again.
     *
     * @param listener the listener to notify
     */
    public void watchReadOnlyBookiesChanged(final RegistrationListener listener)
            throws BKException {
        bkc
            .getMetadataClientDriver()
            .getRegistrationClient()
            .watchReadOnlyBookies(listener);
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
        new LedgerOpenOp(bkc, bkc.getClientCtx().getClientStats(), lId, cb, ctx).initiate();
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
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        SyncOpenCallback result = new SyncOpenCallback(future);

        new LedgerOpenOp(bkc, bkc.getClientCtx().getClientStats(), lId, result, null).initiate();

        return SyncCallbackUtils.waitForResult(future);
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
        new LedgerOpenOp(bkc, bkc.getClientCtx().getClientStats(), lId, cb, ctx).initiateWithoutRecovery();
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
    @SuppressWarnings("unchecked")
    public LedgerHandle openLedgerNoRecovery(final long lId)
            throws InterruptedException, BKException {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        SyncOpenCallback result = new SyncOpenCallback(future);

        new LedgerOpenOp(bkc, bkc.getClientCtx().getClientStats(), lId, result, null)
                .initiateWithoutRecovery();

        return SyncCallbackUtils.waitForResult(future);
    }

    /**
     * Read entries from a ledger synchronously. If the lastEntry is -1, it will read all the entries in the ledger from
     * the firstEntry.
     *
     * @param ledgerId
     * @param firstEntry
     * @param lastEntry
     * @return
     * @throws InterruptedException
     * @throws BKException
     */
    public Iterable<LedgerEntry> readEntries(long ledgerId, long firstEntry, long lastEntry)
            throws InterruptedException, BKException {
        checkArgument(ledgerId >= 0 && firstEntry >= 0);
        return new LedgerEntriesIterable(ledgerId, firstEntry, lastEntry);
    }

    class LedgerEntriesIterable implements Iterable<LedgerEntry> {
        final long ledgerId;
        final long firstEntryId;
        final long lastEntryId;

        public LedgerEntriesIterable(long ledgerId, long firstEntry) {
            this(ledgerId, firstEntry, -1);
        }

        public LedgerEntriesIterable(long ledgerId, long firstEntry, long lastEntry) {
            this.ledgerId = ledgerId;
            this.firstEntryId = firstEntry;
            this.lastEntryId = lastEntry;
        }

        @Override
        public Iterator<LedgerEntry> iterator() {
            try {
                return new LedgerEntriesIterator(ledgerId, firstEntryId, lastEntryId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    class LedgerEntriesIterator implements Iterator<LedgerEntry> {
        final LedgerHandle handle;
        final long ledgerId;
        final long lastEntryId;

        long nextEntryId;
        LedgerEntry currentEntry;

        public LedgerEntriesIterator(long ledgerId, long firstEntry, long lastEntry)
                throws InterruptedException, BKException {
            this.handle = openLedgerNoRecovery(ledgerId);
            this.ledgerId = ledgerId;
            this.nextEntryId = firstEntry;
            this.lastEntryId = lastEntry;
            this.currentEntry = null;
        }

        @Override
        public boolean hasNext() {
            if (currentEntry != null) {
                return true;
            }
            if ((lastEntryId == -1 || nextEntryId <= lastEntryId) && nextEntryId <= handle.getLastAddConfirmed()) {
                try {
                    CompletableFuture<Enumeration<LedgerEntry>> result = new CompletableFuture<>();
                    handle.asyncReadEntriesInternal(nextEntryId, nextEntryId,
                        new SyncReadCallback(result), null, false);

                    currentEntry = SyncCallbackUtils.waitForResult(result).nextElement();

                    return true;
                } catch (Exception e) {
                    if (e instanceof BKException.BKNoSuchEntryException && lastEntryId == -1) {
                        // there are no more entries in the ledger, so we just return false and ignore this exception
                        // since the last entry id was undefined
                        close();
                        return false;
                    }
                    LOG.error("Error reading entry {} from ledger {}", nextEntryId, ledgerId, e);
                    close();
                    throw new RuntimeException(e);
                }
            }
            close();
            return false;
        }

        @Override
        public LedgerEntry next() {
            if (lastEntryId > -1 && nextEntryId > lastEntryId) {
                throw new NoSuchElementException();
            }
            ++nextEntryId;
            LedgerEntry entry = currentEntry;
            currentEntry = null;
            return entry;
        }

        @Override
        public void remove() {
            // noop
        }

        private void close() {
            if (handle != null) {
                try {
                    handle.close();
                } catch (Exception e) {
                    LOG.error("Error closing ledger handle {}", handle, e);
                }
            }
        }
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

    public SortedMap<Long, LedgerMetadata> getLedgersContainBookies(Set<BookieId> bookies)
            throws InterruptedException, BKException {
        final SyncObject sync = new SyncObject();
        final AtomicReference<SortedMap<Long, LedgerMetadata>> resultHolder =
                new AtomicReference<SortedMap<Long, LedgerMetadata>>(null);
        asyncGetLedgersContainBookies(bookies, new GenericCallback<SortedMap<Long, LedgerMetadata>>() {
            @Override
            public void operationComplete(int rc, SortedMap<Long, LedgerMetadata> result) {
                LOG.info("GetLedgersContainBookies completed with rc : {}", rc);
                synchronized (sync) {
                    sync.rc = rc;
                    sync.value = true;
                    resultHolder.set(result);
                    sync.notify();
                }
            }
        });
        synchronized (sync) {
            while (!sync.value) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
        return resultHolder.get();
    }

    public void asyncGetLedgersContainBookies(final Set<BookieId> bookies,
                                              final GenericCallback<SortedMap<Long, LedgerMetadata>> callback) {
        final SortedMap<Long, LedgerMetadata> ledgers = new ConcurrentSkipListMap<Long, LedgerMetadata>();
        bkc.getLedgerManager().asyncProcessLedgers(new Processor<Long>() {
            @Override
            public void process(final Long lid, final AsyncCallback.VoidCallback cb) {
                bkc.getLedgerManager().readLedgerMetadata(lid)
                    .whenComplete((metadata, exception) -> {
                            if (BKException.getExceptionCode(exception)
                                == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                                // the ledger was deleted during this iteration.
                                cb.processResult(BKException.Code.OK, null, null);
                                return;
                            } else if (exception != null) {
                                cb.processResult(BKException.getExceptionCode(exception), null, null);
                                return;
                            }
                            Set<BookieId> bookiesInLedger =
                                LedgerMetadataUtils.getBookiesInThisLedger(metadata.getValue());
                            Sets.SetView<BookieId> intersection =
                                Sets.intersection(bookiesInLedger, bookies);
                            if (!intersection.isEmpty()) {
                                ledgers.put(lid, metadata.getValue());
                            }
                            cb.processResult(BKException.Code.OK, null, null);
                        });
            }
        }, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                callback.operationComplete(rc, ledgers);
            }
        }, null, BKException.Code.OK, BKException.Code.MetaStoreException);
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
     */
    public void recoverBookieData(final BookieId bookieSrc)
            throws InterruptedException, BKException {
        Set<BookieId> bookiesSrc = Sets.newHashSet(bookieSrc);
        recoverBookieData(bookiesSrc);
    }

    public void recoverBookieData(final Set<BookieId> bookiesSrc)
            throws InterruptedException, BKException {
        recoverBookieData(bookiesSrc, false, false);
    }

    public void recoverBookieData(final Set<BookieId> bookiesSrc, boolean dryrun, boolean skipOpenLedgers)
        throws InterruptedException, BKException {
        recoverBookieData(bookiesSrc, dryrun, skipOpenLedgers, false);
    }

    public void recoverBookieData(final Set<BookieId> bookiesSrc, boolean dryrun, boolean skipOpenLedgers,
                                  boolean skipUnrecoverableLedgers) throws InterruptedException, BKException {
        SyncObject sync = new SyncObject();
        // Call the async method to recover bookie data.
        asyncRecoverBookieData(bookiesSrc, dryrun, skipOpenLedgers, skipUnrecoverableLedgers, new RecoverCallback() {
            @Override
            public void recoverComplete(int rc, Object ctx) {
                LOG.info("Recover bookie operation completed with rc: {}", BKException.codeLogger(rc));
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
            while (!sync.value) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
    }

    public void recoverBookieData(final long lid,
                                  final Set<BookieId> bookiesSrc,
                                  boolean dryrun,
                                  boolean skipOpenLedgers)
            throws InterruptedException, BKException {
        SyncObject sync = new SyncObject();
        // Call the async method to recover bookie data.
        asyncRecoverBookieData(lid, bookiesSrc, dryrun, skipOpenLedgers, (rc, ctx) -> {
            LOG.info("Recover bookie for {} completed with rc : {}", lid, BKException.codeLogger(rc));
            SyncObject syncObject = (SyncObject) ctx;
            synchronized (syncObject) {
                syncObject.rc = rc;
                syncObject.value = true;
                syncObject.notify();
            }
        }, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (!sync.value) {
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
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    public void asyncRecoverBookieData(final BookieId bookieSrc,
                                       final RecoverCallback cb, final Object context) {
        Set<BookieId> bookiesSrc = Sets.newHashSet(bookieSrc);
        asyncRecoverBookieData(bookiesSrc, cb, context);
    }

    public void asyncRecoverBookieData(final Set<BookieId> bookieSrc,
                                       final RecoverCallback cb, final Object context) {
        asyncRecoverBookieData(bookieSrc, false, false, false, cb, context);
    }

    public void asyncRecoverBookieData(final Set<BookieId> bookieSrc, boolean dryrun,
                                       final boolean skipOpenLedgers, final boolean skipUnrecoverableLedgers,
                                       final RecoverCallback cb, final Object context) {
        getActiveLedgers(bookieSrc, dryrun, skipOpenLedgers, skipUnrecoverableLedgers, cb, context);
    }

    /**
     * Recover a specific ledger.
     *
     * @param lid
     *          ledger to recover
     * @param bookieSrc
     *          Source bookies that had a failure. We want to replicate the ledger fragments that were stored there.
     * @param dryrun
     *          dryrun the recover procedure.
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param callback
     *          RecoverCallback to invoke once all of the data on the dead
     *          bookie has been recovered and replicated.
     * @param context
     *          Context for the RecoverCallback to call.
     */
    public void asyncRecoverBookieData(long lid, final Set<BookieId> bookieSrc, boolean dryrun,
                                       boolean skipOpenLedgers, final RecoverCallback callback, final Object context) {
        AsyncCallback.VoidCallback callbackWrapper = (rc, path, ctx)
            -> callback.recoverComplete(bkc.getReturnRc(rc), context);
        recoverLedger(bookieSrc, lid, dryrun, skipOpenLedgers, callbackWrapper);
    }

    /**
     * This method asynchronously polls ZK to get the current set of active
     * ledgers. From this, we can open each ledger and look at the metadata to
     * determine if any of the ledger fragments for it were stored at the dead
     * input bookie.
     *
     * @param bookiesSrc
     *            Source bookies that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param dryrun
     *            dryrun the recover procedure.
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    private void getActiveLedgers(final Set<BookieId> bookiesSrc, final boolean dryrun,
                                  final boolean skipOpenLedgers, final boolean skipUnrecoverableLedgers,
                                  final RecoverCallback cb, final Object context) {
        // Wrapper class around the RecoverCallback so it can be used
        // as the final VoidCallback to process ledgers
        class RecoverCallbackWrapper implements AsyncCallback.VoidCallback {
            final RecoverCallback cb;

            RecoverCallbackWrapper(RecoverCallback cb) {
                this.cb = cb;
            }

            @Override
            public void processResult(int rc, String path, Object ctx) {
                cb.recoverComplete(bkc.getReturnRc(rc), ctx);
            }
        }

        Processor<Long> ledgerProcessor = new Processor<Long>() {
            @Override
            public void process(Long ledgerId, AsyncCallback.VoidCallback iterCallback) {
                recoverLedger(bookiesSrc, ledgerId, dryrun, skipOpenLedgers, skipUnrecoverableLedgers, iterCallback);
            }
        };
        bkc.getLedgerManager().asyncProcessLedgers(
                ledgerProcessor, new RecoverCallbackWrapper(cb),
                context, BKException.Code.OK, BKException.Code.LedgerRecoveryException);
    }

    /**
     * This method asynchronously recovers a given ledger if any of the ledger
     * entries were stored on the failed bookie.
     *
     * @param bookiesSrc
     *            Source bookies that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param lId
     *            Ledger id we want to recover.
     * @param dryrun
     *            printing the recovery plan without actually recovering bookies
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param finalLedgerIterCb
     *            IterationCallback to invoke once we've recovered the current
     *            ledger.
     */
    private void recoverLedger(final Set<BookieId> bookiesSrc, final long lId, final boolean dryrun,
                               final boolean skipOpenLedgers, final AsyncCallback.VoidCallback finalLedgerIterCb) {
        recoverLedger(bookiesSrc, lId, dryrun, skipOpenLedgers, false, finalLedgerIterCb);
    }

    /**
     * This method asynchronously recovers a given ledger if any of the ledger
     * entries were stored on the failed bookie.
     *
     * @param bookiesSrc
     *            Source bookies that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param lId
     *            Ledger id we want to recover.
     * @param dryrun
     *            printing the recovery plan without actually recovering bookies
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param skipUnrecoverableLedgers
     *            Skip unrecoverable ledgers.
     * @param finalLedgerIterCb
     *            IterationCallback to invoke once we've recovered the current
     *            ledger.
     */
    private void recoverLedger(final Set<BookieId> bookiesSrc, final long lId, final boolean dryrun,
                               final boolean skipOpenLedgers, final boolean skipUnrecoverableLedgers,
                               final AsyncCallback.VoidCallback finalLedgerIterCb) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering ledger : {}", lId);
        }

        asyncOpenLedgerNoRecovery(lId, new OpenCallback() {
            @Override
            public void openComplete(int rc, final LedgerHandle lh, Object ctx) {
                if (rc != BKException.Code.OK) {
                    if (skipUnrecoverableLedgers) {
                        LOG.warn("BK error opening ledger: {}, skip recover it.", lId, BKException.create(rc));
                        finalLedgerIterCb.processResult(BKException.Code.OK, null, null);
                    } else {
                        LOG.error("BK error opening ledger: {}", lId, BKException.create(rc));
                        finalLedgerIterCb.processResult(rc, null, null);
                    }
                    return;
                }

                LedgerMetadata lm = lh.getLedgerMetadata();
                if (skipOpenLedgers && lm.getState() == LedgerMetadata.State.OPEN) {
                    LOG.info("Skip recovering open ledger {}.", lId);
                    try {
                        lh.close();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (BKException bke) {
                        LOG.warn("Error on closing ledger handle for {}.", lId);
                    }
                    finalLedgerIterCb.processResult(BKException.Code.OK, null, null);
                    return;
                }

                final boolean fenceRequired = !lm.isClosed() && containBookiesInLastEnsemble(lm, bookiesSrc);
                // the original write has not removed faulty bookie from
                // current ledger ensemble. to avoid data loss issue in
                // the case of concurrent updates to the ensemble composition,
                // the recovery tool should first close the ledger
                if (!dryrun && fenceRequired) {
                    // close opened non recovery ledger handle
                    try {
                        lh.close();
                    } catch (Exception ie) {
                        LOG.warn("Error closing non recovery ledger handle for ledger " + lId, ie);
                    }
                    asyncOpenLedger(lId, new OpenCallback() {
                        @Override
                        public void openComplete(int newrc, final LedgerHandle newlh, Object newctx) {
                            if (newrc != BKException.Code.OK) {
                                if (skipUnrecoverableLedgers) {
                                    LOG.warn("BK error opening ledger: {}, skip recover it.",
                                        lId, BKException.create(newrc));
                                    finalLedgerIterCb.processResult(BKException.Code.OK, null, null);
                                } else {
                                    LOG.error("BK error close ledger: {}", lId, BKException.create(newrc));
                                    finalLedgerIterCb.processResult(newrc, null, null);
                                }
                                return;
                            }
                            bkc.mainWorkerPool.submit(() -> {
                                // do recovery
                                recoverLedger(bookiesSrc, lId, dryrun, skipOpenLedgers,
                                    skipUnrecoverableLedgers, finalLedgerIterCb);
                            });
                        }
                    }, null);
                    return;
                }

                final AsyncCallback.VoidCallback ledgerIterCb = new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        if (BKException.Code.OK != rc) {
                            if (skipUnrecoverableLedgers) {
                                LOG.warn("Failed to recover ledger: {} : {}, skip recover it.", lId,
                                    BKException.codeLogger(rc));
                                rc = BKException.Code.OK;
                            } else {
                                LOG.error("Failed to recover ledger {} : {}", lId, BKException.codeLogger(rc));
                            }
                        } else {
                            LOG.info("Recovered ledger {}.", lId);
                        }
                        try {
                            lh.close();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } catch (BKException bke) {
                            LOG.warn("Error on closing ledger handle for {}.", lId);
                        }
                        finalLedgerIterCb.processResult(rc, path, ctx);
                    }
                };

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
                for (Map.Entry<Long, ? extends List<BookieId>> entry :
                         lh.getLedgerMetadata().getAllEnsembles().entrySet()) {
                    if (curEntryId != null) {
                        ledgerFragmentsRange.put(curEntryId, entry.getKey() - 1);
                    }
                    curEntryId = entry.getKey();
                    if (containBookies(entry.getValue(), bookiesSrc)) {
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

                if (dryrun) {
                    VERBOSE.info("Recovered ledger {} : {}", lId, (fenceRequired ? "[fence required]" : ""));
                }

                /*
                 * Multicallback for ledger. Once all fragments for the ledger have been recovered
                 * trigger the ledgerIterCb
                 */
                MultiCallback ledgerFragmentsMcb = new MultiCallback(ledgerFragmentsToRecover.size(), ledgerIterCb,
                        null, BKException.Code.OK, BKException.Code.LedgerRecoveryException);
                /*
                 * Now recover all of the necessary ledger fragments
                 * asynchronously using a MultiCallback for every fragment.
                 */
                for (final Long startEntryId : ledgerFragmentsToRecover) {
                    Long endEntryId = ledgerFragmentsRange.get(startEntryId);
                    List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().get(startEntryId);
                    // Get bookies to replace
                    Map<Integer, BookieId> targetBookieAddresses;
                    try {
                        targetBookieAddresses = getReplacementBookies(lh, ensemble, bookiesSrc);
                    } catch (BKException.BKNotEnoughBookiesException e) {
                        if (!dryrun) {
                            ledgerFragmentsMcb.processResult(BKException.Code.NotEnoughBookiesException, null, null);
                        } else {
                            VERBOSE.info("  Fragment [{} - {}] : {}", startEntryId, endEntryId,
                                BKException.getMessage(BKException.Code.NotEnoughBookiesException));
                        }
                        continue;
                    }

                    if (dryrun) {
                        ArrayList<BookieId> newEnsemble =
                                replaceBookiesInEnsemble(ensemble, targetBookieAddresses);
                        VERBOSE.info("  Fragment [{} - {}] : ", startEntryId, endEntryId);
                        VERBOSE.info("    old ensemble : {}", formatEnsemble(ensemble, bookiesSrc, '*'));
                        VERBOSE.info("    new ensemble : {}", formatEnsemble(newEnsemble, bookiesSrc, '*'));
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Replicating fragment from [{}, {}] of ledger {} to {}",
                                startEntryId, endEntryId, lh.getId(), targetBookieAddresses);
                        }
                        try {
                            LedgerFragmentReplicator.SingleFragmentCallback cb =
                                new LedgerFragmentReplicator.SingleFragmentCallback(ledgerFragmentsMcb, lh,
                                                                                    bkc.getLedgerManager(),
                                        startEntryId, getReplacementBookiesMap(ensemble, targetBookieAddresses));
                            LedgerFragment ledgerFragment = new LedgerFragment(lh,
                                startEntryId, endEntryId, targetBookieAddresses.keySet());
                            asyncRecoverLedgerFragment(lh, ledgerFragment, cb,
                                Sets.newHashSet(targetBookieAddresses.values()), NOOP_BICONSUMER);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
                if (dryrun) {
                    ledgerIterCb.processResult(BKException.Code.OK, null, null);
                }
            }
            }, null);
    }

    static String formatEnsemble(List<BookieId> ensemble, Set<BookieId> bookiesSrc,
            char marker) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < ensemble.size(); i++) {
            sb.append(ensemble.get(i));
            if (bookiesSrc.contains(ensemble.get(i))) {
                sb.append(marker);
            } else {
                sb.append(' ');
            }
            if (i != ensemble.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * This method asynchronously recovers a ledger fragment which is a
     * contiguous portion of a ledger that was stored in an ensemble that
     * included the failed bookie.
     *
     * @param lh
     *            - LedgerHandle for the ledger
     * @param ledgerFragment
     *            - LedgerFragment to replicate
     * @param ledgerFragmentMcb
     *            - MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param newBookies
     *            - New bookies we want to use to recover and replicate the
     *            ledger entries that were stored on the failed bookie.
     */
    private void asyncRecoverLedgerFragment(final LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final Set<BookieId> newBookies,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) throws InterruptedException {
        lfr.replicate(lh, ledgerFragment, ledgerFragmentMcb, newBookies, onReadEntryFailureCallback);
    }

    private Map<Integer, BookieId> getReplacementBookies(
                LedgerHandle lh,
                List<BookieId> ensemble,
                Set<BookieId> bookiesToRereplicate)
            throws BKException.BKNotEnoughBookiesException {
        Set<Integer> bookieIndexesToRereplicate = Sets.newHashSet();
        for (int bookieIndex = 0; bookieIndex < ensemble.size(); bookieIndex++) {
            BookieId bookieInEnsemble = ensemble.get(bookieIndex);
            if (bookiesToRereplicate.contains(bookieInEnsemble)) {
                bookieIndexesToRereplicate.add(bookieIndex);
            }
        }
        return getReplacementBookiesByIndexes(
                lh, ensemble, bookieIndexesToRereplicate, Optional.of(bookiesToRereplicate));
    }

    private Map<Integer, BookieId> getReplacementBookiesByIndexes(
                LedgerHandle lh,
                List<BookieId> ensemble,
                Set<Integer> bookieIndexesToRereplicate,
                Optional<Set<BookieId>> excludedBookies)
            throws BKException.BKNotEnoughBookiesException {
        // target bookies to replicate
        Map<Integer, BookieId> targetBookieAddresses =
                Maps.newHashMapWithExpectedSize(bookieIndexesToRereplicate.size());
        // bookies to exclude for ensemble allocation
        Set<BookieId> bookiesToExclude = Sets.newHashSet();
        if (excludedBookies.isPresent()) {
            bookiesToExclude.addAll(excludedBookies.get());
        }

        // excluding bookies that need to be replicated
        for (Integer bookieIndex : bookieIndexesToRereplicate) {
            BookieId bookie = ensemble.get(bookieIndex);
            bookiesToExclude.add(bookie);
        }

        // allocate bookies
        for (Integer bookieIndex : bookieIndexesToRereplicate) {
            BookieId oldBookie = ensemble.get(bookieIndex);
            EnsemblePlacementPolicy.PlacementResult<BookieId> replaceBookieResponse =
                    bkc.getPlacementPolicy().replaceBookie(
                            lh.getLedgerMetadata().getEnsembleSize(),
                            lh.getLedgerMetadata().getWriteQuorumSize(),
                            lh.getLedgerMetadata().getAckQuorumSize(),
                            lh.getLedgerMetadata().getCustomMetadata(),
                            ensemble,
                            oldBookie,
                            bookiesToExclude);
            BookieId newBookie = replaceBookieResponse.getResult();
            PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy = replaceBookieResponse.getAdheringToPolicy();
            if (isEnsembleAdheringToPlacementPolicy == PlacementPolicyAdherence.FAIL && LOG.isDebugEnabled()) {
                LOG.debug(
                        "replaceBookie for bookie: {} in ensemble: {} "
                                + "is not adhering to placement policy and chose {}",
                        oldBookie, ensemble, newBookie);
            }
            targetBookieAddresses.put(bookieIndex, newBookie);
            bookiesToExclude.add(newBookie);
        }

        return targetBookieAddresses;
    }

    private ArrayList<BookieId> replaceBookiesInEnsemble(
            List<BookieId> ensemble,
            Map<Integer, BookieId> replacedBookies) {
        ArrayList<BookieId> newEnsemble = Lists.newArrayList(ensemble);
        for (Map.Entry<Integer, BookieId> entry : replacedBookies.entrySet()) {
            newEnsemble.set(entry.getKey(), entry.getValue());
        }
        return newEnsemble;
    }

    /**
     * Replicate the Ledger fragment to target Bookie passed.
     *
     * @param lh
     *            - ledgerHandle
     * @param ledgerFragment
     *            - LedgerFragment to replicate
     */
    public void replicateLedgerFragment(LedgerHandle lh, final LedgerFragment ledgerFragment,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) throws InterruptedException, BKException {
        Map<Integer, BookieId> targetBookieAddresses = null;
        if (LedgerFragment.ReplicateType.DATA_LOSS == ledgerFragment.getReplicateType()) {
            Optional<Set<BookieId>> excludedBookies = Optional.empty();
            targetBookieAddresses = getReplacementBookiesByIndexes(lh, ledgerFragment.getEnsemble(),
                    ledgerFragment.getBookiesIndexes(), excludedBookies);
        } else if (LedgerFragment.ReplicateType.DATA_NOT_ADHERING_PLACEMENT == ledgerFragment.getReplicateType()) {
            targetBookieAddresses = replaceNotAdheringPlacementPolicyBookie(ledgerFragment.getEnsemble(),
                    lh.getLedgerMetadata().getWriteQuorumSize(), lh.getLedgerMetadata().getAckQuorumSize());
            ledgerFragment.getBookiesIndexes().addAll(targetBookieAddresses.keySet());
        }
        if (MapUtils.isEmpty(targetBookieAddresses)) {
            LOG.warn("Could not replicate for {} ledger: {}, not find target bookie.",
                    ledgerFragment.getReplicateType(), ledgerFragment.getLedgerId());
            throw new BKException.BKLedgerRecoveryException();
        }
        replicateLedgerFragment(lh, ledgerFragment, targetBookieAddresses, onReadEntryFailureCallback);
    }

    private void replicateLedgerFragment(LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final Map<Integer, BookieId> targetBookieAddresses,
            final BiConsumer<Long, Long> onReadEntryFailureCallback)
            throws InterruptedException, BKException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        ResultCallBack resultCallBack = new ResultCallBack(result);
        SingleFragmentCallback cb = new SingleFragmentCallback(
            resultCallBack,
            lh,
            bkc.getLedgerManager(),
            ledgerFragment.getFirstEntryId(),
            getReplacementBookiesMap(ledgerFragment, targetBookieAddresses));

        Set<BookieId> targetBookieSet = Sets.newHashSet();
        targetBookieSet.addAll(targetBookieAddresses.values());
        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, targetBookieSet, onReadEntryFailureCallback);

        try {
            SyncCallbackUtils.waitForResult(result);
        } catch (BKException err) {
            throw BKException.create(bkc.getReturnRc(err.getCode()));
        }
    }

    private static Map<BookieId, BookieId> getReplacementBookiesMap(
            List<BookieId> ensemble,
            Map<Integer, BookieId> targetBookieAddresses) {
        Map<BookieId, BookieId> bookiesMap =
                new HashMap<BookieId, BookieId>();
        for (Map.Entry<Integer, BookieId> entry : targetBookieAddresses.entrySet()) {
            BookieId oldBookie = ensemble.get(entry.getKey());
            BookieId newBookie = entry.getValue();
            bookiesMap.put(oldBookie, newBookie);
        }
        return bookiesMap;
    }

    private static Map<BookieId, BookieId> getReplacementBookiesMap(
            LedgerFragment ledgerFragment,
            Map<Integer, BookieId> targetBookieAddresses) {
        Map<BookieId, BookieId> bookiesMap =
                new HashMap<BookieId, BookieId>();
        for (Integer bookieIndex : ledgerFragment.getBookiesIndexes()) {
            BookieId oldBookie = ledgerFragment.getAddress(bookieIndex);
            BookieId newBookie = targetBookieAddresses.get(bookieIndex);
            bookiesMap.put(oldBookie, newBookie);
        }
        return bookiesMap;
    }

    private static boolean containBookiesInLastEnsemble(LedgerMetadata lm,
                                                        Set<BookieId> bookies) {
        if (lm.getAllEnsembles().size() <= 0) {
            return false;
        }
        Long lastKey = lm.getAllEnsembles().lastKey();
        List<BookieId> lastEnsemble = lm.getAllEnsembles().get(lastKey);
        return containBookies(lastEnsemble, bookies);
    }

    private static boolean containBookies(List<BookieId> ensemble,
                                          Set<BookieId> bookies) {
        for (BookieId bookie : ensemble) {
            if (bookies.contains(bookie)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This is the class for getting the replication result.
     */
    public static class ResultCallBack implements AsyncCallback.VoidCallback {
        private final CompletableFuture<Void> sync;

        public ResultCallBack(CompletableFuture<Void> sync) {
            this.sync = sync;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void processResult(int rc, String s, Object ctx) {
            SyncCallbackUtils.finish(rc, null, sync);
        }
    }

    /**
     * Format the BookKeeper metadata in zookeeper.
     *
     * @param isInteractive
     *            Whether format should ask prompt for confirmation if old data
     *            exists or not.
     * @param force
     *            If non interactive and force is true, then old data will be
     *            removed without prompt.
     * @return Returns true if format succeeds else false.
     */
    public static boolean format(ServerConfiguration conf,
            boolean isInteractive, boolean force) throws Exception {
        return runFunctionWithMetadataBookieDriver(conf, new Function<MetadataBookieDriver, Boolean>() {
            @Override
            @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
            public Boolean apply(MetadataBookieDriver driver) {
                try {
                    try (RegistrationManager regManager = driver.createRegistrationManager()) {
                        boolean ledgerRootExists = regManager.prepareFormat();

                        // If old data was there then confirm with admin.
                        boolean doFormat = true;
                        if (ledgerRootExists) {
                            if (!isInteractive) {
                                // If non interactive and force is set, then delete old data.
                                doFormat = force;
                            } else {
                                // Confirm with the admin.
                                doFormat = IOUtils
                                        .confirmPrompt("Ledger root already exists. "
                                                + "Are you sure to format bookkeeper metadata? "
                                                + "This may cause data loss.");
                            }
                        }

                        if (!doFormat) {
                            return false;
                        }

                        driver.getLedgerManagerFactory().format(
                                conf,
                                driver.getLayoutManager());

                        return regManager.format();
                    }
                } catch (Exception e) {
                    throw new UncheckedExecutionException(e.getMessage(), e);
                }
            }
        });
    }

    /**
     * Intializes new cluster by creating required znodes for the cluster. If
     * ledgersrootpath is already existing then it will error out.
     *
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean initNewCluster(ServerConfiguration conf) throws Exception {
        return runFunctionWithRegistrationManager(conf, rm -> {
            try {
                return rm.initNewCluster();
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
        });
    }

    /**
     * Nukes existing cluster metadata. But it does only if the provided
     * ledgersRootPath matches with configuration's zkLedgersRootPath and
     * provided instanceid matches with the cluster metadata. If force is
     * mentioned then instanceid will not be validated.
     *
     * @param conf
     * @param ledgersRootPath
     * @param instanceId
     * @param force
     * @return
     * @throws Exception
     */
    public static boolean nukeExistingCluster(ServerConfiguration conf, String ledgersRootPath, String instanceId,
            boolean force) throws Exception {
        String confLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        if (!confLedgersRootPath.equals(ledgersRootPath)) {
            LOG.error("Provided ledgerRootPath : {} is not matching with config's ledgerRootPath: {}, "
                    + "so exiting nuke operation", ledgersRootPath, confLedgersRootPath);
            return false;
        }

        return runFunctionWithRegistrationManager(conf, rm -> {
            try {
                if (!force) {
                    String readInstanceId = rm.getClusterInstanceId();
                    if ((instanceId == null) || !instanceId.equals(readInstanceId)) {
                        LOG.error("Provided InstanceId : {} is not matching with cluster InstanceId in ZK: {}",
                            instanceId, readInstanceId);
                        return false;
                    }
                }
                return rm.nukeExistingCluster();
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
        });
    }

    /**
     * Initializes bookie, by making sure that the journalDir, ledgerDirs and
     * indexDirs are empty and there is no registered Bookie with this BookieId.
     *
     * @param conf
     * @return
     * @throws Exception
     */
    public static boolean initBookie(ServerConfiguration conf) throws Exception {
        /*
         * make sure that journalDirs, ledgerDirs and indexDirs are empty
         */
        File[] journalDirs = conf.getJournalDirs();
        if (!validateDirectoriesAreEmpty(journalDirs, "JournalDir")) {
            return false;
        }

        File[] ledgerDirs = conf.getLedgerDirs();
        if (!validateDirectoriesAreEmpty(ledgerDirs, "LedgerDir")) {
            return false;
        }

        File[] indexDirs = conf.getIndexDirs();
        if (indexDirs != null) {
            if (!validateDirectoriesAreEmpty(indexDirs, "IndexDir")) {
                return false;
            }
        }

        return runFunctionWithRegistrationManager(conf, rm -> {
            try {
                /*
                 * make sure that there is no bookie registered with the same
                 * bookieid and the cookie for the same bookieid is not existing.
                 */
                BookieId bookieId = BookieImpl.getBookieId(conf);
                if (rm.isBookieRegistered(bookieId)) {
                    LOG.error("Bookie with bookieId: {} is still registered, "
                        + "If this node is running bookie process, try stopping it first.", bookieId);
                    return false;
                }

                try {
                    rm.readCookie(bookieId);
                    LOG.error("Cookie still exists in the ZK for this bookie: {}, try formatting the bookie", bookieId);
                    return false;
                } catch (BookieException.CookieNotFoundException nfe) {
                    // it is expected for readCookie to fail with
                    // BookieException.CookieNotFoundException
                }
                return true;
            } catch (Exception e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }
        });
    }

    private static boolean validateDirectoriesAreEmpty(File[] dirs, String typeOfDir) {
        for (File dir : dirs) {
            File[] dirFiles = dir.listFiles();
            if ((dirFiles != null) && dirFiles.length != 0) {
                LOG.error("{}: {} is existing and its not empty, try formatting the bookie", typeOfDir, dir);
                return false;
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
        final LedgerRangeIterator iterator = bkc.getLedgerManager().getLedgerRanges(0);
        return new Iterable<Long>() {
            @Override
            public Iterator<Long> iterator() {
                return new Iterator<Long>() {
                    Iterator<Long> currentRange = null;

                    @Override
                    public boolean hasNext() {
                        try {
                            if (iterator.hasNext()) {
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
                    public Long next() throws NoSuchElementException {
                        try {
                            if ((currentRange == null) || (!currentRange.hasNext())) {
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

    /**
     * @return the metadata for the passed ledger handle
     */
    public LedgerMetadata getLedgerMetadata(LedgerHandle lh) {
        return lh.getLedgerMetadata();
    }

    private LedgerUnderreplicationManager getUnderreplicationManager()
            throws CompatibilityException, UnavailableException, InterruptedException {
        if (underreplicationManager == null) {
            underreplicationManager = mFactory.newLedgerUnderreplicationManager();
        }
        return underreplicationManager;
    }

    private LedgerAuditorManager getLedgerAuditorManager()
            throws IOException, InterruptedException {
        if (ledgerAuditorManager == null) {
            ledgerAuditorManager = mFactory.newLedgerAuditorManager();
        }
        return ledgerAuditorManager;
    }

    /**
     * Setter for LostBookieRecoveryDelay value (in seconds) in Zookeeper.
     *
     * @param lostBookieRecoveryDelay
     *                              lostBookieRecoveryDelay value (in seconds) to set
     * @throws CompatibilityException
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnavailableException
     */
    public void setLostBookieRecoveryDelay(int lostBookieRecoveryDelay)
            throws CompatibilityException, KeeperException, InterruptedException, UnavailableException {
        LedgerUnderreplicationManager urlManager = getUnderreplicationManager();
        urlManager.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);
    }

    /**
     * Returns the current LostBookieRecoveryDelay value (in seconds) in Zookeeper.
     *
     * @return
     *          current lostBookieRecoveryDelay value (in seconds)
     * @throws CompatibilityException
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnavailableException
     */
    public int getLostBookieRecoveryDelay()
            throws CompatibilityException, KeeperException, InterruptedException, UnavailableException {
        LedgerUnderreplicationManager urlManager = getUnderreplicationManager();
        return urlManager.getLostBookieRecoveryDelay();
    }

    /**
     * Trigger AuditTask by resetting lostBookieRecoveryDelay to its current
     * value. If Autorecovery is not enabled or if there is no Auditor then this
     * method will throw UnavailableException.
     *
     * @throws CompatibilityException
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnavailableException
     * @throws IOException
     */
    public void triggerAudit()
            throws CompatibilityException, KeeperException, InterruptedException, UnavailableException, IOException {
        LedgerUnderreplicationManager urlManager = getUnderreplicationManager();
        if (!urlManager.isLedgerReplicationEnabled()) {
            LOG.error("Autorecovery is disabled. So giving up!");
            throw new UnavailableException("Autorecovery is disabled. So giving up!");
        }

        BookieId auditorId = getLedgerAuditorManager().getCurrentAuditor();
        if (auditorId == null) {
            LOG.error("No auditor elected, though Autorecovery is enabled. So giving up.");
            throw new UnavailableException("No auditor elected, though Autorecovery is enabled. So giving up.");
        }

        int previousLostBookieRecoveryDelayValue = urlManager.getLostBookieRecoveryDelay();
        LOG.info("Resetting LostBookieRecoveryDelay value: {}, to kickstart audit task",
                previousLostBookieRecoveryDelayValue);
        urlManager.setLostBookieRecoveryDelay(previousLostBookieRecoveryDelayValue);
    }

    /**
     * Triggers AuditTask by resetting lostBookieRecoveryDelay and then make
     * sure the ledgers stored in the given decommissioning bookie are properly
     * replicated and they are not underreplicated because of the given bookie.
     * This method waits untill there are no underreplicatedledgers because of this
     * bookie. If the given Bookie is not shutdown yet, then it will throw
     * BKIllegalOpException.
     *
     * @param bookieAddress
     *            address of the decommissioning bookie
     * @throws CompatibilityException
     * @throws UnavailableException
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     * @throws BKAuditException
     * @throws TimeoutException
     * @throws BKException
     */
    public void decommissionBookie(BookieId bookieAddress)
            throws CompatibilityException, UnavailableException, KeeperException, InterruptedException, IOException,
            BKAuditException, TimeoutException, BKException {
        if (getAvailableBookies().contains(bookieAddress) || getReadOnlyBookies().contains(bookieAddress)) {
            LOG.error("Bookie: {} is not shutdown yet", bookieAddress);
            throw BKException.create(BKException.Code.IllegalOpException);
        }

        triggerAudit();

        /*
         * Sleep for 30 secs, so that Auditor gets chance to trigger its
         * force audittask and let the underreplicationmanager process
         * to do its replication process
         */
        Thread.sleep(30 * 1000);

        /*
         * get the collection of the ledgers which are stored in this
         * bookie, by making a call to
         * bookieLedgerIndexer.getBookieToLedgerIndex.
         */

        BookieLedgerIndexer bookieLedgerIndexer = new BookieLedgerIndexer(bkc.ledgerManager);
        Map<String, Set<Long>> bookieToLedgersMap = bookieLedgerIndexer.getBookieToLedgerIndex();
        Set<Long> ledgersStoredInThisBookie = bookieToLedgersMap.get(bookieAddress.toString());
        if ((ledgersStoredInThisBookie != null) && (!ledgersStoredInThisBookie.isEmpty())) {
            /*
             * wait untill all the ledgers are replicated to other
             * bookies by making sure that these ledgers metadata don't
             * contain this bookie as part of their ensemble.
             */
            waitForLedgersToBeReplicated(ledgersStoredInThisBookie, bookieAddress, bkc.ledgerManager);
        }

        // for double-checking, check if any ledgers are listed as underreplicated because of this bookie
        Predicate<List<String>> predicate = replicasList -> replicasList.contains(bookieAddress.toString());
        Iterator<UnderreplicatedLedger> urLedgerIterator = underreplicationManager.listLedgersToRereplicate(predicate);
        if (urLedgerIterator.hasNext()) {
            //if there are any then wait and make sure those ledgers are replicated properly
            LOG.info("Still in some underreplicated ledgers metadata, this bookie is part of its ensemble. "
                    + "Have to make sure that those ledger fragments are rereplicated");
            List<Long> urLedgers = new ArrayList<>();
            urLedgerIterator.forEachRemaining((urLedger) -> {
                urLedgers.add(urLedger.getLedgerId());
            });
            waitForLedgersToBeReplicated(urLedgers, bookieAddress, bkc.ledgerManager);
        }
    }

    private void waitForLedgersToBeReplicated(Collection<Long> ledgers, BookieId thisBookieAddress,
            LedgerManager ledgerManager) throws InterruptedException, TimeoutException {
        int maxSleepTimeInBetweenChecks = 5 * 60 * 1000; // 5 minutes
        int sleepTimePerLedger = 3 * 1000; // 3 secs
        Predicate<Long> validateBookieIsNotPartOfEnsemble = ledgerId -> !areEntriesOfLedgerStoredInTheBookie(ledgerId,
                thisBookieAddress, ledgerManager);
        ledgers.removeIf(validateBookieIsNotPartOfEnsemble);

        while (!ledgers.isEmpty()) {
            int sleepTimeForThisCheck = (long) ledgers.size() * sleepTimePerLedger > maxSleepTimeInBetweenChecks
                    ? maxSleepTimeInBetweenChecks : ledgers.size() * sleepTimePerLedger;
            LOG.info("Count of Ledgers which need to be rereplicated: {}, waiting {} seconds for next check",
                ledgers.size(), sleepTimeForThisCheck / 1000);
            Thread.sleep(sleepTimeForThisCheck);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Making sure following ledgers replication to be completed: {}", ledgers);
            }
            ledgers.removeIf(validateBookieIsNotPartOfEnsemble);
        }
    }

    public static boolean areEntriesOfLedgerStoredInTheBookie(long ledgerId, BookieId bookieAddress,
            LedgerManager ledgerManager) {
        try {
            LedgerMetadata ledgerMetadata = ledgerManager.readLedgerMetadata(ledgerId).get().getValue();
            return areEntriesOfLedgerStoredInTheBookie(ledgerId, bookieAddress, ledgerMetadata);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        } catch (ExecutionException e) {
            if (e.getCause() != null
                    && e.getCause().getClass()
                    .equals(BKException.BKNoSuchLedgerExistsOnMetadataServerException.class)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ledger: {} has been deleted", ledgerId);
                }
                return false;
            } else {
                LOG.error("Got exception while trying to read LedgerMetadata of " + ledgerId, e);
                throw new RuntimeException(e);
            }
        }
    }

    public static boolean areEntriesOfLedgerStoredInTheBookie(long ledgerId, BookieId bookieAddress,
            LedgerMetadata ledgerMetadata) {
        Collection<? extends List<BookieId>> ensemblesOfSegments = ledgerMetadata.getAllEnsembles().values();
        Iterator<? extends List<BookieId>> ensemblesOfSegmentsIterator = ensemblesOfSegments.iterator();
        List<BookieId> ensemble;
        int segmentNo = 0;
        while (ensemblesOfSegmentsIterator.hasNext()) {
            ensemble = ensemblesOfSegmentsIterator.next();
            if (ensemble.contains(bookieAddress)) {
                if (areEntriesOfSegmentStoredInTheBookie(ledgerMetadata, bookieAddress, segmentNo)) {
                    return true;
                }
            }
            segmentNo++;
        }
        return false;
    }

    private static boolean areEntriesOfSegmentStoredInTheBookie(LedgerMetadata ledgerMetadata,
            BookieId bookieAddress, int segmentNo) {
        boolean isLedgerClosed = ledgerMetadata.isClosed();
        int ensembleSize = ledgerMetadata.getEnsembleSize();
        int writeQuorumSize = ledgerMetadata.getWriteQuorumSize();

        List<Entry<Long, ? extends List<BookieId>>> segments =
            new LinkedList<>(ledgerMetadata.getAllEnsembles().entrySet());
        List<BookieId> currentSegmentEnsemble = segments.get(segmentNo).getValue();
        boolean lastSegment = (segmentNo == (segments.size() - 1));

        /*
         * Checking the last segment of the ledger can be complicated in
         * some cases. In the case that the ledger is closed, we can just
         * check the fragments of the segment as normal, except in the case
         * that no entry was ever written, to the ledger, in which case we
         * check no fragments.
         *
         * Following the same approach as in LedgerChecker.checkLedger
         */
        if (lastSegment && isLedgerClosed && (ledgerMetadata.getLastEntryId() < segments.get(segmentNo).getKey())) {
            return false;
        }

        /*
         * If current segment ensemble doesn't contain this bookie then return
         * false.
         */
        if (!currentSegmentEnsemble.contains(bookieAddress)) {
            return false;
        }

        /*
         * if ensembleSize is equal to writeQuorumSize, then ofcourse all
         * the entries of this segment are supposed to be stored in this
         * bookie. If this is last segment of the ledger and if the ledger
         * is not closed (this is a corner case), then we have to return
         * true. For more info. Check BOOKKEEPER-237 and BOOKKEEPER-325.
         */
        if ((lastSegment && !isLedgerClosed) || (ensembleSize == writeQuorumSize)) {
            return true;
        }

        /*
         * the following check is required because ensembleSize can be
         * greater than writeQuorumSize and in this case if there are only
         * couple of entries then based on RoundRobinDistributionSchedule
         * there might not be any entry copy in this bookie though this
         * bookie is part of the ensemble of this segment. If no entry is
         * stored in this bookie then we should return false, because
         * ReplicationWorker wont take care of fixing the ledgerMetadata of
         * this segment in this case.
         *
         * if ensembleSize > writeQuorumSize, then in LedgerFragment.java
         * firstEntryID may not be equal to firstStoredEntryId lastEntryId
         * may not be equalto lastStoredEntryId. firstStoredEntryId and
         * lastStoredEntryId will be LedgerHandle.INVALID_ENTRY_ID, if no
         * entry of this segment stored in this bookie. In this case
         * LedgerChecker.verifyLedgerFragment will not consider it as
         * unavailable/bad fragment though this bookie is part of the
         * ensemble of the segment and it is down.
         */
        DistributionSchedule distributionSchedule = new RoundRobinDistributionSchedule(
                ledgerMetadata.getWriteQuorumSize(), ledgerMetadata.getAckQuorumSize(),
                ledgerMetadata.getEnsembleSize());

        int thisBookieIndexInCurrentEnsemble = currentSegmentEnsemble.indexOf(bookieAddress);
        long firstEntryId = segments.get(segmentNo).getKey();
        long lastEntryId = lastSegment ? ledgerMetadata.getLastEntryId() : segments.get(segmentNo + 1).getKey() - 1;
        long firstStoredEntryId = LedgerHandle.INVALID_ENTRY_ID;
        long firstEntryIter = firstEntryId;
        // following the same approach followed in LedgerFragment.getFirstStoredEntryId()
        for (int i = 0; i < ensembleSize && firstEntryIter <= lastEntryId; i++) {
            if (distributionSchedule.hasEntry(firstEntryIter, thisBookieIndexInCurrentEnsemble)) {
                firstStoredEntryId = firstEntryIter;
                break;
            } else {
                firstEntryIter++;
            }
        }
        return firstStoredEntryId != LedgerHandle.INVALID_ENTRY_ID;
    }

    /**
     * returns boolean value specifying if the ensemble of the segment is
     * adhering to the ensemble placement policy for the given writeQuorumSize
     * and ackQuorumSize.
     *
     * @param ensembleBookiesList
     *            ensemble of the segment
     * @param writeQuorumSize
     *            writeQuorumSize of the ledger
     * @param ackQuorumSize
     *            ackQuorumSize of the ledger
     * @return <tt>true</tt> if the ledger is adhering to
     *         EnsemblePlacementPolicy
     */
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieId> ensembleBookiesList,
            int writeQuorumSize, int ackQuorumSize) {
        return bkc.getPlacementPolicy().isEnsembleAdheringToPlacementPolicy(ensembleBookiesList, writeQuorumSize,
                ackQuorumSize);
    }

    public Map<Integer, BookieId> replaceNotAdheringPlacementPolicyBookie(List<BookieId> ensembleBookiesList,
            int writeQuorumSize, int ackQuorumSize) {
        try {
            EnsemblePlacementPolicy.PlacementResult<List<BookieId>> placementResult = bkc.getPlacementPolicy()
                    .replaceToAdherePlacementPolicy(ensembleBookiesList.size(), writeQuorumSize, ackQuorumSize,
                            new HashSet<>(), ensembleBookiesList);
            if (PlacementPolicyAdherence.FAIL != placementResult.getAdheringToPolicy()) {
                Map<Integer, BookieId> targetMap = new HashMap<>();
                List<BookieId> newEnsembles = placementResult.getResult();
                for (int i = 0; i < ensembleBookiesList.size(); i++) {
                    BookieId originBookie = ensembleBookiesList.get(i);
                    BookieId newBookie = newEnsembles.get(i);
                    if (!originBookie.equals(newBookie)) {
                        targetMap.put(i, newBookie);
                    }
                }
                return targetMap;
            }
        } catch (UnsupportedOperationException e) {
            LOG.warn("The placement policy: {} didn't support replaceToAdherePlacementPolicy, "
                    + "ignore replace not adhere bookie.", bkc.getPlacementPolicy().getClass().getName());
        }
        return Collections.emptyMap();
    }

    /**
     * Makes async request for getting list of entries of ledger from a bookie
     * and returns Future for the result.
     *
     * @param address
     *            BookieId of the bookie
     * @param ledgerId
     *            ledgerId
     * @return returns Future
     */
    public CompletableFuture<AvailabilityOfEntriesOfLedger> asyncGetListOfEntriesOfLedger(BookieId address,
            long ledgerId) {
        return bkc.getBookieClient().getListOfEntriesOfLedger(address, ledgerId);
    }

    public BookieId getCurrentAuditor() throws IOException, InterruptedException {
        return getLedgerAuditorManager().getCurrentAuditor();
    }
}
