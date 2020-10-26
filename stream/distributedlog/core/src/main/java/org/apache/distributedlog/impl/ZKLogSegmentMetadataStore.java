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
package org.apache.distributedlog.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.callback.LogSegmentNamesListener;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.LogSegmentNotFoundException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.logsegment.LogSegmentMetadataStore;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Transaction.OpListener;
import org.apache.distributedlog.util.Utils;
import org.apache.distributedlog.zk.DefaultZKOp;
import org.apache.distributedlog.zk.ZKOp;
import org.apache.distributedlog.zk.ZKTransaction;
import org.apache.distributedlog.zk.ZKVersionedSetOp;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based log segment metadata store.
 */
public class ZKLogSegmentMetadataStore implements LogSegmentMetadataStore, Watcher, Children2Callback {

    private static final Logger logger = LoggerFactory.getLogger(ZKLogSegmentMetadataStore.class);

    private static final List<String> EMPTY_LIST = ImmutableList.of();

    private static class ReadLogSegmentsTask implements SafeRunnable, FutureEventListener<Versioned<List<String>>> {

        private final String logSegmentsPath;
        private final ZKLogSegmentMetadataStore store;
        private int currentZKBackOffMs;

        ReadLogSegmentsTask(String logSegmentsPath,
                            ZKLogSegmentMetadataStore metadataStore) {
            this.logSegmentsPath = logSegmentsPath;
            this.store = metadataStore;
            this.currentZKBackOffMs = store.minZKBackoffMs;
        }

        @Override
        public void onSuccess(final Versioned<List<String>> segments) {
            // reset the back off after a successful operation
            currentZKBackOffMs = store.minZKBackoffMs;
            store.notifyLogSegmentsUpdated(
                    logSegmentsPath,
                    store.listeners.get(logSegmentsPath),
                    segments);
        }

        @Override
        public void onFailure(Throwable cause) {
            int backoffMs;
            if (cause instanceof LogNotFoundException) {
                // the log segment has been deleted, remove all the registered listeners
                store.notifyLogStreamDeleted(logSegmentsPath,
                        store.listeners.remove(logSegmentsPath));
                return;
            } else {
                backoffMs = currentZKBackOffMs;
                currentZKBackOffMs = Math.min(2 * currentZKBackOffMs, store.maxZKBackoffMs);
            }
            store.scheduleTask(logSegmentsPath, this, backoffMs);
        }

        @Override
        public void safeRun() {
            if (null != store.listeners.get(logSegmentsPath)) {
                store.zkGetLogSegmentNames(logSegmentsPath, store).whenComplete(this);
            } else {
                logger.debug("Log segments listener for {} has been removed.", logSegmentsPath);
            }
        }
    }

    /**
     * A log segment names listener that keeps tracking the version of list of log segments that it has been notified.
     * It only notify the newer log segments.
     */
    static class VersionedLogSegmentNamesListener {

        private final LogSegmentNamesListener listener;
        private Versioned<List<String>> lastNotifiedLogSegments;

        VersionedLogSegmentNamesListener(LogSegmentNamesListener listener) {
            this.listener = listener;
            this.lastNotifiedLogSegments = new Versioned<List<String>>(EMPTY_LIST, Version.NEW);
        }

        synchronized void onSegmentsUpdated(Versioned<List<String>> logSegments) {
            if (lastNotifiedLogSegments.getVersion() == Version.NEW
                    || lastNotifiedLogSegments.getVersion()
                    .compare(logSegments.getVersion()) == Version.Occurred.BEFORE) {
                lastNotifiedLogSegments = logSegments;
                listener.onSegmentsUpdated(logSegments);
            }
        }

        @Override
        public int hashCode() {
            return listener.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof VersionedLogSegmentNamesListener)) {
                return false;
            }
            VersionedLogSegmentNamesListener other = (VersionedLogSegmentNamesListener) obj;
            return listener.equals(other.listener);
        }

        @Override
        public String toString() {
            return listener.toString();
        }
    }

    final DistributedLogConfiguration conf;
    // settings
    final int minZKBackoffMs;
    final int maxZKBackoffMs;
    final boolean skipMinVersionCheck;

    final ZooKeeperClient zkc;
    // log segment listeners
    final ConcurrentMap<String, Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener>> listeners;
    // scheduler
    final OrderedScheduler scheduler;
    final ReentrantReadWriteLock closeLock;
    boolean closed = false;

    public ZKLogSegmentMetadataStore(DistributedLogConfiguration conf,
                                     ZooKeeperClient zkc,
                                     OrderedScheduler scheduler) {
        this.conf = conf;
        this.zkc = zkc;
        this.listeners =
                new ConcurrentHashMap<String, Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener>>();
        this.scheduler = scheduler;
        this.closeLock = new ReentrantReadWriteLock();
        // settings
        this.minZKBackoffMs = conf.getZKRetryBackoffStartMillis();
        this.maxZKBackoffMs = conf.getZKRetryBackoffMaxMillis();
        this.skipMinVersionCheck = conf.getDLLedgerMetadataSkipMinVersionCheck();
    }

    protected void scheduleTask(Object key, SafeRunnable r, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            scheduler.scheduleOrdered(key, r, delayMs, TimeUnit.MILLISECONDS);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected void submitTask(Object key, SafeRunnable r) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            scheduler.executeOrdered(key, r);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    // max sequence number and max transaction id

    @Override
    public void storeMaxLogSegmentSequenceNumber(Transaction<Object> txn,
                                                 LogMetadata logMetadata,
                                                 Versioned<Long> lssn,
                                                 Transaction.OpListener<Version> listener) {
        Version version = lssn.getVersion();
        assert(version instanceof LongVersion);
        LongVersion zkVersion = (LongVersion) version;
        byte[] data = DLUtils.serializeLogSegmentSequenceNumber(lssn.getValue());
        Op setDataOp = Op.setData(logMetadata.getLogSegmentsPath(), data, (int) zkVersion.getLongVersion());
        ZKOp zkOp = new ZKVersionedSetOp(setDataOp, listener);
        txn.addOp(zkOp);
    }

    @Override
    public void storeMaxTxnId(Transaction<Object> txn,
                              LogMetadataForWriter logMetadata,
                              Versioned<Long> transactionId,
                              Transaction.OpListener<Version> listener) {
        Version version = transactionId.getVersion();
        assert(version instanceof LongVersion);
        LongVersion zkVersion = (LongVersion) version;
        byte[] data = DLUtils.serializeTransactionId(transactionId.getValue());
        Op setDataOp = Op.setData(logMetadata.getMaxTxIdPath(), data, (int) zkVersion.getLongVersion());
        ZKOp zkOp = new ZKVersionedSetOp(setDataOp, listener);
        txn.addOp(zkOp);
    }

    // updates

    @Override
    public Transaction<Object> transaction() {
        return new ZKTransaction(zkc);
    }

    @Override
    public void createLogSegment(Transaction<Object> txn,
                                 LogSegmentMetadata segment,
                                 OpListener<Void> listener) {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        Op createOp = Op.create(
                segment.getZkPath(),
                finalisedData,
                zkc.getDefaultACL(),
                CreateMode.PERSISTENT);
        txn.addOp(DefaultZKOp.of(createOp, listener));
    }

    @Override
    public void deleteLogSegment(Transaction<Object> txn,
                                 final LogSegmentMetadata segment,
                                 final OpListener<Void> listener) {
        Op deleteOp = Op.delete(
                segment.getZkPath(),
                -1);
        logger.info("Delete segment : {}", segment);
        txn.addOp(DefaultZKOp.of(deleteOp, new OpListener<Void>() {
            @Override
            public void onCommit(Void r) {
                if (null != listener) {
                    listener.onCommit(r);
                }
            }

            @Override
            public void onAbort(Throwable t) {
                logger.info("Aborted transaction on deleting segment {}", segment);
                KeeperException.Code kc;
                if (t instanceof KeeperException) {
                    kc = ((KeeperException) t).code();
                } else if (t instanceof ZKException) {
                    kc = ((ZKException) t).getKeeperExceptionCode();
                } else {
                    abortListener(t);
                    return;
                }
                if (KeeperException.Code.NONODE == kc) {
                    abortListener(new LogSegmentNotFoundException(segment.getZkPath()));
                    return;
                }
                abortListener(t);
            }

            private void abortListener(Throwable t) {
                if (null != listener) {
                    listener.onAbort(t);
                }
            }
        }));
    }

    @Override
    public void updateLogSegment(Transaction<Object> txn, LogSegmentMetadata segment) {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        Op updateOp = Op.setData(segment.getZkPath(), finalisedData, -1);
        txn.addOp(DefaultZKOp.of(updateOp, null));
    }

    // reads

    /**
     * Process the watched events for registered listeners.
     */
    @Override
    public void process(WatchedEvent event) {
        if (Event.EventType.None == event.getType()
                && Event.KeeperState.Expired == event.getState()) {
            Set<String> keySet = new HashSet<String>(listeners.keySet());
            for (String logSegmentsPath : keySet) {
                scheduleTask(logSegmentsPath, new ReadLogSegmentsTask(logSegmentsPath, this), 0L);
            }
            return;
        }
        String path = event.getPath();
        if (null == path) {
            return;
        }
        switch (event.getType()) {
            case NodeDeleted:
                notifyLogStreamDeleted(path, listeners.remove(path));
                break;
            case NodeChildrenChanged:
                new ReadLogSegmentsTask(path, this).run();
                break;
            default:
                break;
        }
    }

    @Override
    public CompletableFuture<LogSegmentMetadata> getLogSegment(String logSegmentPath) {
        return LogSegmentMetadata.read(zkc, logSegmentPath, skipMinVersionCheck);
    }

    CompletableFuture<Versioned<List<String>>> zkGetLogSegmentNames(String logSegmentsPath, Watcher watcher) {
        CompletableFuture<Versioned<List<String>>> result = new CompletableFuture<Versioned<List<String>>>();
        try {
            zkc.get().getChildren(logSegmentsPath, watcher, this, result);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            result.completeExceptionally(Utils.zkException(e, logSegmentsPath));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(Utils.zkException(e, logSegmentsPath));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        CompletableFuture<Versioned<List<String>>> result = ((CompletableFuture<Versioned<List<String>>>) ctx);
        if (KeeperException.Code.OK.intValue() == rc) {
            /** cversion: the number of changes to the children of this znode **/
            LongVersion zkVersion = new LongVersion(stat.getCversion());
            result.complete(new Versioned(children, zkVersion));
        } else if (KeeperException.Code.NONODE.intValue() == rc) {
            result.completeExceptionally(new LogNotFoundException("Log " + path + " not found"));
        } else {
            result.completeExceptionally(new ZKException("Failed to get log segments from " + path,
                    KeeperException.Code.get(rc)));
        }
    }

    @Override
    public CompletableFuture<Versioned<List<String>>> getLogSegmentNames(String logSegmentsPath,
                                                              LogSegmentNamesListener listener) {
        Watcher zkWatcher;
        if (null == listener) {
            zkWatcher = null;
        } else {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    zkWatcher = null;
                } else {
                    Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener> listenerSet =
                            listeners.get(logSegmentsPath);
                    if (null == listenerSet) {
                        Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener> newListenerSet =
                                new HashMap<LogSegmentNamesListener, VersionedLogSegmentNamesListener>();
                        Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener> oldListenerSet =
                                listeners.putIfAbsent(logSegmentsPath, newListenerSet);
                        if (null != oldListenerSet) {
                            listenerSet = oldListenerSet;
                        } else {
                            listenerSet = newListenerSet;
                        }
                    }
                    synchronized (listenerSet) {
                        listenerSet.put(listener, new VersionedLogSegmentNamesListener(listener));
                        if (!listeners.containsKey(logSegmentsPath)) {
                            // listener set has been removed, add it back
                            if (null != listeners.putIfAbsent(logSegmentsPath, listenerSet)) {
                                logger.debug("Listener set is already found for log segments path {}", logSegmentsPath);
                            }
                        }
                    }
                    zkWatcher = ZKLogSegmentMetadataStore.this;
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        CompletableFuture<Versioned<List<String>>> getLogSegmentNamesResult =
                zkGetLogSegmentNames(logSegmentsPath, zkWatcher);
        if (null != listener) {
            getLogSegmentNamesResult.whenComplete(new ReadLogSegmentsTask(logSegmentsPath, this));
        }
        return zkGetLogSegmentNames(logSegmentsPath, zkWatcher);
    }

    @Override
    public void unregisterLogSegmentListener(String logSegmentsPath,
                                             LogSegmentNamesListener listener) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener> listenerSet =
                    listeners.get(logSegmentsPath);
            if (null == listenerSet) {
                return;
            }
            synchronized (listenerSet) {
                listenerSet.remove(listener);
                if (listenerSet.isEmpty()) {
                    listeners.remove(logSegmentsPath, listenerSet);
                }
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    // Notifications

    void notifyLogStreamDeleted(String logSegmentsPath,
                                final Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener> listeners) {
        if (null == listeners) {
            return;
        }
        this.submitTask(logSegmentsPath, () -> {
            // the listener map might be updated in different threads (e.g. unregisterLogSegmentListener)
            // so access it under a synchronization block
            synchronized (listeners) {
                for (LogSegmentNamesListener listener : listeners.keySet()) {
                    listener.onLogStreamDeleted();
                }
            }
        });

    }

    void notifyLogSegmentsUpdated(String logSegmentsPath,
                                  final Map<LogSegmentNamesListener, VersionedLogSegmentNamesListener> listeners,
                                  final Versioned<List<String>> segments) {
        if (null == listeners) {
            return;
        }
        this.submitTask(logSegmentsPath, () -> {
            // the listener map might be updated in different threads (e.g. unregisterLogSegmentListener)
            // so access it under a synchronization block
            synchronized (listeners) {
                for (VersionedLogSegmentNamesListener listener : listeners.values()) {
                    listener.onSegmentsUpdated(segments);
                }
            }
        });
    }

}
