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

import static org.apache.bookkeeper.metastore.MetastoreTable.ALL_FIELDS;
import static org.apache.bookkeeper.metastore.MetastoreTable.NON_FIELDS;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.metastore.MSException;
import org.apache.bookkeeper.metastore.MetaStore;
import org.apache.bookkeeper.metastore.MetastoreCallback;
import org.apache.bookkeeper.metastore.MetastoreCursor;
import org.apache.bookkeeper.metastore.MetastoreCursor.ReadEntriesCallback;
import org.apache.bookkeeper.metastore.MetastoreException;
import org.apache.bookkeeper.metastore.MetastoreFactory;
import org.apache.bookkeeper.metastore.MetastoreScannableTable;
import org.apache.bookkeeper.metastore.MetastoreTableItem;
import org.apache.bookkeeper.metastore.Value;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaStore Based Ledger Manager Factory
 */
public class MSLedgerManagerFactory extends LedgerManagerFactory {

    static Logger LOG = LoggerFactory.getLogger(MSLedgerManagerFactory.class);

    public static final int CUR_VERSION = 1;

    public static final String TABLE_NAME = "LEDGER";
    public static final String META_FIELD = ".META";

    AbstractConfiguration conf;
    ZooKeeper zk;
    MetaStore metastore;

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(final AbstractConfiguration conf, final ZooKeeper zk,
            final int factoryVersion) throws IOException {
        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        this.conf = conf;
        this.zk = zk;

        // load metadata store
        String msName = conf.getMetastoreImplClass();
        try {
            metastore = MetastoreFactory.createMetaStore(msName);

            // TODO: should record version in somewhere. e.g. ZooKeeper
            int msVersion = metastore.getVersion();
            metastore.init(conf, msVersion);
        } catch (Throwable t) {
            throw new IOException("Failed to initialize metastore " + msName + " : ", t);
        }

        return this;
    }

    @Override
    public void uninitialize() throws IOException {
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
            }
        }

        public synchronized int getRetCode() {
            return rc;
        }

        public synchronized T getResult() {
            return value;
        }
    }

    static class MsLedgerManager implements LedgerManager {
        final ZooKeeper zk;
        final AbstractConfiguration conf;

        final MetaStore metastore;
        final MetastoreScannableTable ledgerTable;
        final int maxEntriesPerScan;

        static final String IDGEN_ZNODE = "ms-idgen";
        static final String IDGENERATION_PREFIX = "/" + IDGEN_ZNODE + "/ID-";

        // Path to generate global id
        private final String idGenPath;

        // we use this to prevent long stack chains from building up in
        // callbacks
        ScheduledExecutorService scheduler;

        MsLedgerManager(final AbstractConfiguration conf, final ZooKeeper zk, final MetaStore metastore) {
            this.conf = conf;
            this.zk = zk;
            this.metastore = metastore;

            try {
                ledgerTable = metastore.createScannableTable(TABLE_NAME);
            } catch (MetastoreException mse) {
                LOG.error("Failed to instantiate table " + TABLE_NAME + " in metastore " + metastore.getName());
                throw new RuntimeException("Failed to instantiate table " + TABLE_NAME + " in metastore "
                        + metastore.getName());
            }
            // configuration settings
            maxEntriesPerScan = conf.getMetastoreMaxEntriesPerScan();

            this.idGenPath = conf.getZkLedgersRootPath() + IDGENERATION_PREFIX;
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
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
        public void createLedger(final LedgerMetadata metadata, final GenericCallback<Long> ledgerCb) {
            ZkUtils.asyncCreateFullPathOptimistic(zk, idGenPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL, new StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, final String idPathName) {
                            if (rc != KeeperException.Code.OK.intValue()) {
                                LOG.error("Could not generate new ledger id",
                                        KeeperException.create(KeeperException.Code.get(rc), path));
                                ledgerCb.operationComplete(BKException.Code.ZKException, null);
                                return;
                            }
                            /*
                             * Extract ledger id from gen path
                             */
                            long ledgerId;
                            try {
                                ledgerId = getLedgerIdFromGenPath(idPathName);
                            } catch (IOException e) {
                                LOG.error("Could not extract ledger-id from id gen path:" + path, e);
                                ledgerCb.operationComplete(BKException.Code.ZKException, null);
                                return;
                            }

                            final long lid = ledgerId;
                            MetastoreCallback<Version> msCallback = new MetastoreCallback<Version>() {
                                @Override
                                public void complete(int rc, Version version, Object ctx) {
                                    if (MSException.Code.BadVersion.getCode() == rc) {
                                        ledgerCb.operationComplete(BKException.Code.MetadataVersionException, null);
                                        return;
                                    }
                                    if (MSException.Code.OK.getCode() != rc) {
                                        ledgerCb.operationComplete(BKException.Code.MetaStoreException, null);
                                        return;
                                    }
                                    LOG.debug("Create ledger {} with version {} successfuly.", new Object[] { lid,
                                            version });
                                    // update version
                                    metadata.setVersion(version);
                                    ledgerCb.operationComplete(BKException.Code.OK, lid);
                                }
                            };

                            ledgerTable.put(ledgerId2Key(lid), new Value().setField(META_FIELD, metadata.serialize()),
                                    Version.NEW, msCallback, null);
                            zk.delete(idPathName, -1, new AsyncCallback.VoidCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx) {
                                    if (rc != KeeperException.Code.OK.intValue()) {
                                        LOG.warn("Exception during deleting znode for id generation : ",
                                                KeeperException.create(KeeperException.Code.get(rc), path));
                                    } else {
                                        LOG.debug("Deleting znode for id generation : {}", idPathName);
                                    }
                                }
                            }, null);
                        }
                    }, null);
        }

        // get ledger id from generation path
        private long getLedgerIdFromGenPath(String nodeName) throws IOException {
            long ledgerId;
            try {
                String parts[] = nodeName.split(IDGENERATION_PREFIX);
                ledgerId = Long.parseLong(parts[parts.length - 1]);
            } catch (NumberFormatException e) {
                throw new IOException(e);
            }
            return ledgerId;
        }

        @Override
        public void removeLedgerMetadata(final long ledgerId, final Version version,
                                         final GenericCallback<Void> cb) {
            MetastoreCallback<Void> msCallback = new MetastoreCallback<Void>() {
                @Override
                public void complete(int rc, Void value, Object ctx) {
                    int bkRc;
                    if (MSException.Code.NoKey.getCode() == rc) {
                        LOG.warn("Ledger entry does not exist in meta table: ledgerId={}", ledgerId);
                        bkRc = BKException.Code.NoSuchLedgerExistsException;
                    } else if (MSException.Code.OK.getCode() == rc) {
                        bkRc = BKException.Code.OK;
                    } else {
                        bkRc = BKException.Code.MetaStoreException;
                    }
                    cb.operationComplete(bkRc, (Void) null);
                }
            };
            ledgerTable.remove(ledgerId2Key(ledgerId), version, msCallback, null);
        }

        @Override
        public void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb) {
            final String key = ledgerId2Key(ledgerId);
            MetastoreCallback<Versioned<Value>> msCallback = new MetastoreCallback<Versioned<Value>>() {
                @Override
                public void complete(int rc, Versioned<Value> value, Object ctx) {
                    if (MSException.Code.NoKey.getCode() == rc) {
                        LOG.error("No ledger metadata found for ledger " + ledgerId + " : ",
                                MSException.create(MSException.Code.get(rc), "No key " + key + " found."));
                        readCb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                        return;
                    }
                    if (MSException.Code.OK.getCode() != rc) {
                        LOG.error("Could not read metadata for ledger " + ledgerId + " : ",
                                MSException.create(MSException.Code.get(rc), "Failed to get key " + key));
                        readCb.operationComplete(BKException.Code.MetaStoreException, null);
                        return;
                    }
                    LedgerMetadata metadata;
                    try {
                        metadata = LedgerMetadata
                                .parseConfig(value.getValue().getField(META_FIELD), value.getVersion());
                    } catch (IOException e) {
                        LOG.error("Could not parse ledger metadata for ledger " + ledgerId + " : ", e);
                        readCb.operationComplete(BKException.Code.MetaStoreException, null);
                        return;
                    }
                    readCb.operationComplete(BKException.Code.OK, metadata);
                }
            };
            ledgerTable.get(key, msCallback, ALL_FIELDS);
        }

        @Override
        public void writeLedgerMetadata(final long ledgerId, final LedgerMetadata metadata,
                final GenericCallback<Void> cb) {
            Value data = new Value().setField(META_FIELD, metadata.serialize());

            LOG.debug("Writing ledger {} metadata, version {}", new Object[] { ledgerId, metadata.getVersion() });

            final String key = ledgerId2Key(ledgerId);
            MetastoreCallback<Version> msCallback = new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version version, Object ctx) {
                    int bkRc;
                    if (MSException.Code.BadVersion.getCode() == rc) {
                        LOG.info("Bad version provided to updat metadata for ledger {}", ledgerId);
                        bkRc = BKException.Code.MetadataVersionException;
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        LOG.warn("Ledger {} doesn't exist when writing its ledger metadata.", ledgerId);
                        bkRc = BKException.Code.NoSuchLedgerExistsException;
                    } else if (MSException.Code.OK.getCode() == rc) {
                        metadata.setVersion(version);
                        bkRc = BKException.Code.OK;
                    } else {
                        LOG.warn("Conditional update ledger metadata failed: ",
                                MSException.create(MSException.Code.get(rc), "Failed to put key " + key));
                        bkRc = BKException.Code.MetaStoreException;
                    }

                    cb.operationComplete(bkRc, null);
                }
            };
            ledgerTable.put(key, data, metadata.getVersion(), msCallback, null);
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
        public LedgerRangeIterator getLedgerRanges() {
            return new MSLedgerRangeIterator();
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
         * Constructor
         *
         * @param scheduler
         *            Executor used to prevent long stack chains
         */
        public AsyncSetProcessor(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        /**
         * Process set of items
         *
         * @param data
         *            Set of data to process
         * @param processor
         *            Callback to process element of list when success
         * @param finalCb
         *            Final callback to be called after all elements in the list
         *            are processed
         * @param contxt
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
                        public final void run() {
                            processor.process(dataToProcess, stub);
                        }
                    });
                }
            };
            T firstElement = iter.next();
            processor.process(firstElement, stubCallback);
        }
    }

}
