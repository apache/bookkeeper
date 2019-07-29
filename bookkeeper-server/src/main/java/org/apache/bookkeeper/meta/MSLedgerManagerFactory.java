/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.meta;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.metastore.MetastoreTable.ALL_FIELDS;
import static org.apache.bookkeeper.metastore.MetastoreTable.NON_FIELDS;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.metastore.MSException;
import org.apache.bookkeeper.metastore.MSWatchedEvent;
import org.apache.bookkeeper.metastore.MetaStore;
import org.apache.bookkeeper.metastore.MetastoreCallback;
import org.apache.bookkeeper.metastore.MetastoreCursor;
import org.apache.bookkeeper.metastore.MetastoreCursor.ReadEntriesCallback;
import org.apache.bookkeeper.metastore.MetastoreException;
import org.apache.bookkeeper.metastore.MetastoreFactory;
import org.apache.bookkeeper.metastore.MetastoreScannableTable;
import org.apache.bookkeeper.metastore.MetastoreTable;
import org.apache.bookkeeper.metastore.MetastoreTableItem;
import org.apache.bookkeeper.metastore.MetastoreUtils;
import org.apache.bookkeeper.metastore.MetastoreWatcher;
import org.apache.bookkeeper.metastore.Value;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaStore Based Ledger Manager Factory.
 *
 * <p>MSLedgerManagerFactory is a legacy abstraction that mixing zookeeper with a metadata store
 * interface. It is not used by any production systems. It should be deprecated soon.
 *
 * @deprecated since 4.7.0
 */
@Slf4j
public class MSLedgerManagerFactory extends AbstractZkLedgerManagerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MSLedgerManagerFactory.class);

    private static final int MS_CONNECT_BACKOFF_MS = 200;

    public static final int CUR_VERSION = 1;
    public static final String NAME = "ms";

    public static final String TABLE_NAME = "LEDGER";
    public static final String META_FIELD = ".META";

    AbstractConfiguration conf;
    MetaStore metastore;

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(final AbstractConfiguration conf,
                                           final LayoutManager layoutManager,
                                           final int factoryVersion) throws IOException {
        checkArgument(layoutManager instanceof ZkLayoutManager);
        ZkLayoutManager zkLayoutManager = (ZkLayoutManager) layoutManager;

        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        this.conf = conf;
        this.zk = zkLayoutManager.getZk();

        // load metadata store
        String msName = conf.getMetastoreImplClass();
        try {
            metastore = MetastoreFactory.createMetaStore(msName);

            // TODO: should record version in somewhere. e.g. ZooKeeper
            // {@link https://github.com/apache/bookkeeper/issues/282}
            int msVersion = metastore.getVersion();
            metastore.init(conf, msVersion);
        } catch (Throwable t) {
            throw new IOException("Failed to initialize metastore " + msName + " : ", t);
        }

        return this;
    }

    @Override
    public void close() throws IOException {
        metastore.close();
    }

    static Long key2LedgerId(String key) {
        return null == key ? null : Long.parseLong(key, 10);
    }

    static String ledgerId2Key(Long lid) {
        return null == lid ? null : StringUtils.getZKStringId(lid);
    }

    static String rangeToString(Long firstLedger, boolean firstInclusive, Long lastLedger, boolean lastInclusive) {
        StringBuilder sb = new StringBuilder();
        sb.append(firstInclusive ? "[ " : "( ").append(firstLedger).append(" ~ ").append(lastLedger)
                .append(lastInclusive ? " ]" : " )");
        return sb.toString();
    }

    static SortedSet<Long> entries2Ledgers(Iterator<MetastoreTableItem> entries) {
        SortedSet<Long> ledgers = new TreeSet<Long>();
        while (entries.hasNext()) {
            MetastoreTableItem item = entries.next();
            try {
                ledgers.add(key2LedgerId(item.getKey()));
            } catch (NumberFormatException nfe) {
                LOG.warn("Found invalid ledger key {}", item.getKey());
            }
        }
        return ledgers;
    }

    static class SyncResult<T> {
        T value;
        int rc;
        boolean finished = false;

        public synchronized void complete(int rc, T value) {
            this.rc = rc;
            this.value = value;
            finished = true;

            notify();
        }

        public synchronized void block() {
            try {
                while (!finished) {
                    wait();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        public synchronized int getRetCode() {
            return rc;
        }

        public synchronized T getResult() {
            return value;
        }
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        return new ZkLedgerIdGenerator(
            zk,
            ZKMetadataDriverBase.resolveZkLedgersRootPath(conf),
            MsLedgerManager.IDGEN_ZNODE,
            zkAcls);
    }

    static class MsLedgerManager implements LedgerManager, MetastoreWatcher {
        final ZooKeeper zk;
        final AbstractConfiguration conf;
        private final LedgerMetadataSerDe serDe;
        final MetaStore metastore;
        final MetastoreScannableTable ledgerTable;
        final int maxEntriesPerScan;

        static final String IDGEN_ZNODE = "ms-idgen";

        // ledger metadata listeners
        protected final ConcurrentMap<Long, Set<LedgerMetadataListener>> listeners =
                new ConcurrentHashMap<Long, Set<LedgerMetadataListener>>();

        // we use this to prevent long stack chains from building up in
        // callbacks
        ScheduledExecutorService scheduler;

        protected class ReadLedgerMetadataTask implements Runnable {

            final long ledgerId;

            ReadLedgerMetadataTask(long ledgerId) {
                this.ledgerId = ledgerId;
            }

            @Override
            public void run() {
                if (null != listeners.get(ledgerId)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Re-read ledger metadata for {}.", ledgerId);
                    }
                    readLedgerMetadata(ledgerId).whenComplete(
                            (metadata, exception) -> handleMetadata(metadata, exception));
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ledger metadata listener for ledger {} is already removed.", ledgerId);
                    }
                }
            }

            private void handleMetadata(Versioned<LedgerMetadata> metadata, Throwable exception) {
                if (exception == null) {
                    final Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                    if (null != listenerSet) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ledger metadata is changed for {} : {}.", ledgerId, metadata.getValue());
                        }
                        scheduler.submit(() -> {
                                synchronized (listenerSet) {
                                    for (LedgerMetadataListener listener : listenerSet) {
                                        listener.onChanged(ledgerId, metadata);
                                    }
                                }
                            });
                    }
                } else if (BKException.getExceptionCode(exception)
                        == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                    // the ledger is removed, do nothing
                    Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                    if (null != listenerSet) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Removed ledger metadata listener set on ledger {} as its ledger is deleted : {}",
                                    ledgerId, listenerSet.size());
                        }
                    }
                } else {
                    LOG.warn("Failed on read ledger metadata of ledger {}: {}",
                             ledgerId, BKException.getExceptionCode(exception));
                    scheduler.schedule(this, MS_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
                }
            }
        }

        MsLedgerManager(final AbstractConfiguration conf, final ZooKeeper zk, final MetaStore metastore) {
            this.conf = conf;
            this.zk = zk;
            this.metastore = metastore;
            this.serDe = new LedgerMetadataSerDe();

            try {
                ledgerTable = metastore.createScannableTable(TABLE_NAME);
            } catch (MetastoreException mse) {
                LOG.error("Failed to instantiate table " + TABLE_NAME + " in metastore " + metastore.getName());
                throw new RuntimeException("Failed to instantiate table " + TABLE_NAME + " in metastore "
                        + metastore.getName());
            }
            // configuration settings
            maxEntriesPerScan = conf.getMetastoreMaxEntriesPerScan();

            ThreadFactoryBuilder tfb = new ThreadFactoryBuilder()
                    .setNameFormat("MSLedgerManagerScheduler-%d");
            this.scheduler = Executors.newSingleThreadScheduledExecutor(tfb
                    .build());
        }

        @Override
        public void process(MSWatchedEvent e){
            long ledgerId = key2LedgerId(e.getKey());
            switch(e.getType()) {
            case CHANGED:
                new ReadLedgerMetadataTask(key2LedgerId(e.getKey())).run();

                break;
            case REMOVED:
                Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (listenerSet != null) {
                    synchronized (listenerSet) {
                        for (LedgerMetadataListener l : listenerSet){
                            unregisterLedgerMetadataListener(ledgerId, l);
                            l.onChanged(ledgerId, null);
                        }
                    }
                }

                break;
            default:
                LOG.warn("Unknown type: {}", e.getType());
                break;
            }
        }

        @Override
        public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
            if (null != listener) {
                LOG.info("Registered ledger metadata listener {} on ledger {}.", listener, ledgerId);
                Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (listenerSet == null) {
                    Set<LedgerMetadataListener> newListenerSet = new HashSet<LedgerMetadataListener>();
                    Set<LedgerMetadataListener> oldListenerSet = listeners.putIfAbsent(ledgerId, newListenerSet);
                    if (null != oldListenerSet) {
                        listenerSet = oldListenerSet;
                    } else {
                        listenerSet = newListenerSet;
                    }
                }
                synchronized (listenerSet) {
                    listenerSet.add(listener);
                }
                new ReadLedgerMetadataTask(ledgerId).run();
            }
        }

        @Override
        public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
            Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
            if (listenerSet != null) {
                synchronized (listenerSet) {
                    if (listenerSet.remove(listener)) {
                        LOG.info("Unregistered ledger metadata listener {} on ledger {}.", listener, ledgerId);
                    }
                    if (listenerSet.isEmpty()) {
                        listeners.remove(ledgerId, listenerSet);
                    }
                }
            }
        }

        @Override
        public void close() {
            try {
                scheduler.shutdown();
            } catch (Exception e) {
                LOG.warn("Error when closing MsLedgerManager : ", e);
            }
            ledgerTable.close();
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long lid, LedgerMetadata metadata) {
            CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
            MetastoreCallback<Version> msCallback = new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version version, Object ctx) {
                    if (MSException.Code.BadVersion.getCode() == rc) {
                        promise.completeExceptionally(new BKException.BKMetadataVersionException());
                        return;
                    }
                    if (MSException.Code.OK.getCode() != rc) {
                        promise.completeExceptionally(new BKException.MetaStoreException());
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Create ledger {} with version {} successfully.", lid, version);
                    }
                    promise.complete(new Versioned<>(metadata, version));
                }
            };

            final byte[] bytes;
            try {
                bytes = serDe.serialize(metadata);
            } catch (IOException ioe) {
                promise.completeExceptionally(new BKException.BKMetadataSerializationException(ioe));
                return promise;
            }
            ledgerTable.put(ledgerId2Key(lid), new Value().setField(META_FIELD, bytes),
                            Version.NEW, msCallback, null);
            return promise;
        }

        @Override
        public CompletableFuture<Void> removeLedgerMetadata(final long ledgerId, final Version version) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            MetastoreCallback<Void> msCallback = new MetastoreCallback<Void>() {
                @Override
                public void complete(int rc, Void value, Object ctx) {
                    int bkRc;
                    if (MSException.Code.NoKey.getCode() == rc) {
                        LOG.warn("Ledger entry does not exist in meta table: ledgerId={}", ledgerId);
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    } else if (MSException.Code.OK.getCode() == rc) {
                        FutureUtils.complete(promise, null);
                    } else {
                        promise.completeExceptionally(new BKException.BKMetadataSerializationException());
                    }
                }
            };
            ledgerTable.remove(ledgerId2Key(ledgerId), version, msCallback, null);
            return promise;
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
            final String key = ledgerId2Key(ledgerId);
            CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
            MetastoreCallback<Versioned<Value>> msCallback = new MetastoreCallback<Versioned<Value>>() {
                @Override
                public void complete(int rc, Versioned<Value> value, Object ctx) {
                    if (MSException.Code.NoKey.getCode() == rc) {
                        LOG.error("No ledger metadata found for ledger " + ledgerId + " : ",
                                MSException.create(MSException.Code.get(rc), "No key " + key + " found."));
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                        return;
                    }
                    if (MSException.Code.OK.getCode() != rc) {
                        LOG.error("Could not read metadata for ledger " + ledgerId + " : ",
                                MSException.create(MSException.Code.get(rc), "Failed to get key " + key));
                        promise.completeExceptionally(new BKException.MetaStoreException());
                        return;
                    }
                    try {
                        LedgerMetadata metadata = serDe.parseConfig(
                                value.getValue().getField(META_FIELD), Optional.empty());
                        promise.complete(new Versioned<>(metadata, value.getVersion()));
                    } catch (IOException e) {
                        LOG.error("Could not parse ledger metadata for ledger " + ledgerId + " : ", e);
                        promise.completeExceptionally(new BKException.MetaStoreException());
                    }
                }
            };
            ledgerTable.get(key, this, msCallback, ALL_FIELDS);
            return promise;
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                                Version currentVersion) {

            CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
            final byte[] bytes;
            try {
                bytes = serDe.serialize(metadata);
            } catch (IOException ioe) {
                promise.completeExceptionally(new BKException.MetaStoreException(ioe));
                return promise;
            }

            Value data = new Value().setField(META_FIELD, bytes);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Writing ledger {} metadata, version {}", new Object[] { ledgerId, currentVersion });
            }

            final String key = ledgerId2Key(ledgerId);
            MetastoreCallback<Version> msCallback = new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version version, Object ctx) {
                    if (MSException.Code.BadVersion.getCode() == rc) {
                        LOG.info("Bad version provided to updat metadata for ledger {}", ledgerId);
                        promise.completeExceptionally(new BKException.BKMetadataVersionException());
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        LOG.warn("Ledger {} doesn't exist when writing its ledger metadata.", ledgerId);
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    } else if (MSException.Code.OK.getCode() == rc) {
                        promise.complete(new Versioned<>(metadata, version));
                    } else {
                        LOG.warn("Conditional update ledger metadata failed: ",
                                MSException.create(MSException.Code.get(rc), "Failed to put key " + key));
                        promise.completeExceptionally(new BKException.MetaStoreException());
                    }
                }
            };
            ledgerTable.put(key, data, currentVersion, msCallback, null);
            return promise;
        }

        @Override
        public void asyncProcessLedgers(final Processor<Long> processor, final AsyncCallback.VoidCallback finalCb,
                final Object context, final int successRc, final int failureRc) {
            MetastoreCallback<MetastoreCursor> openCursorCb = new MetastoreCallback<MetastoreCursor>() {
                @Override
                public void complete(int rc, MetastoreCursor cursor, Object ctx) {
                    if (MSException.Code.OK.getCode() != rc) {
                        finalCb.processResult(failureRc, null, context);
                        return;
                    }
                    if (!cursor.hasMoreEntries()) {
                        finalCb.processResult(successRc, null, context);
                        return;
                    }
                    asyncProcessLedgers(cursor, processor, finalCb, context, successRc, failureRc);
                }
            };
            ledgerTable.openCursor(NON_FIELDS, openCursorCb, null);
        }

        void asyncProcessLedgers(final MetastoreCursor cursor, final Processor<Long> processor,
                                 final AsyncCallback.VoidCallback finalCb, final Object context,
                                 final int successRc, final int failureRc) {
            scheduler.submit(new Runnable() {
                @Override
                public void run() {
                    doAsyncProcessLedgers(cursor, processor, finalCb, context, successRc, failureRc);
                }
            });
        }

        void doAsyncProcessLedgers(final MetastoreCursor cursor, final Processor<Long> processor,
                                   final AsyncCallback.VoidCallback finalCb, final Object context,
                                   final int successRc, final int failureRc) {
            // no entries now
            if (!cursor.hasMoreEntries()) {
                finalCb.processResult(successRc, null, context);
                return;
            }
            ReadEntriesCallback msCallback = new ReadEntriesCallback() {
                @Override
                public void complete(int rc, Iterator<MetastoreTableItem> entries, Object ctx) {
                    if (MSException.Code.OK.getCode() != rc) {
                        finalCb.processResult(failureRc, null, context);
                        return;
                    }

                    SortedSet<Long> ledgers = new TreeSet<Long>();
                    while (entries.hasNext()) {
                        MetastoreTableItem item = entries.next();
                        try {
                            ledgers.add(key2LedgerId(item.getKey()));
                        } catch (NumberFormatException nfe) {
                            LOG.warn("Found invalid ledger key {}", item.getKey());
                        }
                    }

                    if (0 == ledgers.size()) {
                        // process next batch of ledgers
                        asyncProcessLedgers(cursor, processor, finalCb, context, successRc, failureRc);
                        return;
                    }

                    final long startLedger = ledgers.first();
                    final long endLedger = ledgers.last();

                    AsyncSetProcessor<Long> setProcessor = new AsyncSetProcessor<Long>(scheduler);
                    // process set
                    setProcessor.process(ledgers, processor, new AsyncCallback.VoidCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx) {
                            if (successRc != rc) {
                                LOG.error("Failed when processing range "
                                        + rangeToString(startLedger, true, endLedger, true));
                                finalCb.processResult(failureRc, null, context);
                                return;
                            }
                            // process next batch of ledgers
                            asyncProcessLedgers(cursor, processor, finalCb, context, successRc, failureRc);
                        }
                    }, context, successRc, failureRc);
                }
            };
            cursor.asyncReadEntries(maxEntriesPerScan, msCallback, null);
        }

        class MSLedgerRangeIterator implements LedgerRangeIterator {
            final CountDownLatch openCursorLatch = new CountDownLatch(1);
            MetastoreCursor cursor = null;
            // last ledger id in previous range

            MSLedgerRangeIterator() {
                MetastoreCallback<MetastoreCursor> openCursorCb = new MetastoreCallback<MetastoreCursor>() {
                    @Override
                    public void complete(int rc, MetastoreCursor newCursor, Object ctx) {
                        if (MSException.Code.OK.getCode() != rc) {
                            LOG.error("Error opening cursor for ledger range iterator {}", rc);
                        } else {
                            cursor = newCursor;
                        }
                        openCursorLatch.countDown();
                    }
                };
                ledgerTable.openCursor(NON_FIELDS, openCursorCb, null);
            }

            @Override
            public boolean hasNext() throws IOException {
                try {
                    openCursorLatch.await();
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted waiting for cursor to open", ie);
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted waiting to read range", ie);
                }
                if (cursor == null) {
                    throw new IOException("Failed to open ledger range cursor, check logs");
                }
                return cursor.hasMoreEntries();
            }

            @Override
            public LedgerRange next() throws IOException {
                try {
                    SortedSet<Long> ledgerIds = new TreeSet<Long>();
                    Iterator<MetastoreTableItem> iter = cursor.readEntries(maxEntriesPerScan);
                    while (iter.hasNext()) {
                        ledgerIds.add(key2LedgerId(iter.next().getKey()));
                    }
                    return new LedgerRange(ledgerIds);
                } catch (MSException mse) {
                    LOG.error("Exception occurred reading from metastore", mse);
                    throw new IOException("Couldn't read from metastore", mse);
                }
            }
        }

        @Override
        public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
            return new MSLedgerRangeIterator();
        }

        /**
         * Whether the znode a special znode.
         *
         * @param znode
         *          Znode Name
         * @return true  if the znode is a special znode otherwise false
         */
        public static boolean isSpecialZnode(String znode) {
            return BookKeeperConstants.AVAILABLE_NODE.equals(znode)
                || BookKeeperConstants.COOKIE_NODE.equals(znode)
                || BookKeeperConstants.LAYOUT_ZNODE.equals(znode)
                || BookKeeperConstants.INSTANCEID.equals(znode)
                || BookKeeperConstants.UNDER_REPLICATION_NODE.equals(znode)
                || MsLedgerManager.IDGEN_ZNODE.equals(znode);
        }
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new MsLedgerManager(conf, zk, metastore);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager() throws KeeperException,
            InterruptedException, ReplicationException.CompatibilityException {
        // TODO: currently just use zk ledger underreplication manager
        return new ZkLedgerUnderreplicationManager(conf, zk);
    }

    /**
     * Process set one by one in asynchronize way. Process will be stopped
     * immediately when error occurred.
     */
    private static class AsyncSetProcessor<T> {
        // use this to prevent long stack chains from building up in callbacks
        ScheduledExecutorService scheduler;

        /**
         * Constructor.
         *
         * @param scheduler
         *            Executor used to prevent long stack chains
         */
        public AsyncSetProcessor(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        /**
         * Process set of items.
         *
         * @param data
         *            Set of data to process
         * @param processor
         *            Callback to process element of list when success
         * @param finalCb
         *            Final callback to be called after all elements in the list
         *            are processed
         * @param context
         *            Context of final callback
         * @param successRc
         *            RC passed to final callback on success
         * @param failureRc
         *            RC passed to final callback on failure
         */
        public void process(final Set<T> data, final Processor<T> processor, final AsyncCallback.VoidCallback finalCb,
                final Object context, final int successRc, final int failureRc) {
            if (data == null || data.size() == 0) {
                finalCb.processResult(successRc, null, context);
                return;
            }
            final Iterator<T> iter = data.iterator();
            AsyncCallback.VoidCallback stubCallback = new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (rc != successRc) {
                        // terminal immediately
                        finalCb.processResult(failureRc, null, context);
                        return;
                    }
                    if (!iter.hasNext()) { // reach the end of list
                        finalCb.processResult(successRc, null, context);
                        return;
                    }
                    // process next element
                    final T dataToProcess = iter.next();
                    final AsyncCallback.VoidCallback stub = this;
                    scheduler.submit(new Runnable() {
                        @Override
                        public void run() {
                            processor.process(dataToProcess, stub);
                        }
                    });
                }
            };
            T firstElement = iter.next();
            processor.process(firstElement, stubCallback);
        }
    }

    @Override
    public void format(AbstractConfiguration<?> conf, LayoutManager layoutManager)
        throws InterruptedException, KeeperException, IOException {
        MetastoreTable ledgerTable;
        try {
            ledgerTable = metastore.createScannableTable(TABLE_NAME);
        } catch (MetastoreException mse) {
            throw new IOException("Failed to instantiate table " + TABLE_NAME + " in metastore "
                    + metastore.getName());
        }
        try {
            MetastoreUtils.cleanTable(ledgerTable, conf.getMetastoreMaxEntriesPerScan());
        } catch (MSException mse) {
            throw new IOException("Exception when cleanning up table " + TABLE_NAME, mse);
        }
        LOG.info("Finished cleaning up table {}.", TABLE_NAME);
        // Delete and recreate the LAYOUT information.
        Class<? extends LedgerManagerFactory> factoryClass;
        try {
            factoryClass = conf.getLedgerManagerFactoryClass();
        } catch (ConfigurationException e) {
            throw new IOException("Failed to get ledger manager factory class from configuration : ", e);
        }

        layoutManager.deleteLedgerLayout();
        // Create new layout information again.
        createNewLMFactory(conf, layoutManager, factoryClass);
    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager layoutManager)
            throws InterruptedException, KeeperException, IOException {
        String zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        String zkServers = ZKMetadataDriverBase.resolveZkServers(conf);

        /*
         * before proceeding with nuking existing cluster, make sure there
         * are no unexpected znodes under ledgersRootPath
         */
        List<String> ledgersRootPathChildrenList = zk.getChildren(zkLedgersRootPath, false);
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if ((!MsLedgerManager.isSpecialZnode(ledgersRootPathChildren))) {
                log.error("Found unexpected znode : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, zkLedgersRootPath);
                return false;
            }
        }

        // formatting ledgermanager deletes ledger znodes
        format(conf, layoutManager);

        // now delete all the special nodes recursively
        ledgersRootPathChildrenList = zk.getChildren(zkLedgersRootPath, false);
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if (MsLedgerManager.isSpecialZnode(ledgersRootPathChildren)) {
                ZKUtil.deleteRecursive(zk, zkLedgersRootPath + "/" + ledgersRootPathChildren);
            } else {
                log.error("Found unexpected znode : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, zkLedgersRootPath);
                return false;
            }
        }

        // finally deleting the ledgers rootpath
        zk.delete(zkLedgersRootPath, -1);

        log.info("Successfully nuked existing cluster, ZKServers: {} ledger root path: {}",
                zkServers, zkLedgersRootPath);
        return true;
    }
}
