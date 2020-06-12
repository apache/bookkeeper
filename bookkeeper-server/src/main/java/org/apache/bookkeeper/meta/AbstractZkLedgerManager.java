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

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract ledger manager based on zookeeper, which provides common methods such as query zk nodes.
 */
public abstract class AbstractZkLedgerManager implements LedgerManager, Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractZkLedgerManager.class);

    @VisibleForTesting
    static final int ZK_CONNECT_BACKOFF_MS = 200;

    private final LedgerMetadataSerDe serDe;
    protected final AbstractConfiguration conf;
    protected final ZooKeeper zk;
    protected final String ledgerRootPath;

    // ledger metadata listeners
    protected final ConcurrentMap<Long, Set<LedgerMetadataListener>> listeners =
            new ConcurrentHashMap<Long, Set<LedgerMetadataListener>>();
    // we use this to prevent long stack chains from building up in callbacks
    protected ScheduledExecutorService scheduler;

    /**
     * ReadLedgerMetadataTask class.
     */
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
                readLedgerMetadata(ledgerId, AbstractZkLedgerManager.this)
                    .whenComplete((metadata, exception) -> handleMetadata(metadata, exception));
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ledger metadata listener for ledger {} is already removed.", ledgerId);
                }
            }
        }

        private void handleMetadata(Versioned<LedgerMetadata> result, Throwable exception) {
            if (exception == null) {
                final Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (null != listenerSet) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ledger metadata is changed for {} : {}.", ledgerId, result);
                    }
                    scheduler.submit(() -> {
                            synchronized (listenerSet) {
                                for (LedgerMetadataListener listener : listenerSet) {
                                    listener.onChanged(ledgerId, result);
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
                    // notify `null` as indicator that a ledger is deleted
                    // make this behavior consistent with `NodeDeleted` watched event.
                    synchronized (listenerSet) {
                        for (LedgerMetadataListener listener : listenerSet) {
                            listener.onChanged(ledgerId, null);
                        }
                    }
                }
            } else {
                LOG.warn("Failed on read ledger metadata of ledger {}: {}",
                         ledgerId, BKException.getExceptionCode(exception));
                scheduler.schedule(this, ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * ZooKeeper-based Ledger Manager Constructor.
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     */
    protected AbstractZkLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        this.serDe = new LedgerMetadataSerDe();
        this.conf = conf;
        this.zk = zk;
        this.ledgerRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        this.scheduler = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("ZkLedgerManagerScheduler"));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Using AbstractZkLedgerManager with root path : {}", ledgerRootPath);
        }
    }

    /**
     * Get the znode path that is used to store ledger metadata.
     *
     * @param ledgerId
     *          Ledger ID
     * @return ledger node path
     */
    public abstract String getLedgerPath(long ledgerId);

    /**
     * Get ledger id from its znode ledger path.
     *
     * @param ledgerPath
     *          Ledger path to store metadata
     * @return ledger id
     * @throws IOException when the ledger path is invalid
     */
    protected abstract long getLedgerId(String ledgerPath) throws IOException;

    @Override
    public void process(WatchedEvent event) {
        LOG.debug("Received watched event {} from zookeeper based ledger manager.", event);
        if (Event.EventType.None == event.getType()) {
            if (Event.KeeperState.Expired == event.getState()) {
                LOG.info("ZooKeeper client expired on ledger manager.");
                Set<Long> keySet = new HashSet<Long>(listeners.keySet());
                for (Long lid : keySet) {
                    scheduler.submit(new ReadLedgerMetadataTask(lid));
                    LOG.info("Re-read ledger metadata for {} after zookeeper session expired.", lid);
                }
            }
            return;
        }
        String path = event.getPath();
        if (null == path) {
            return;
        }
        final long ledgerId;
        try {
            ledgerId = getLedgerId(event.getPath());
        } catch (IOException ioe) {
            LOG.info("Received invalid ledger path {} : ", event.getPath(), ioe);
            return;
        }
        switch (event.getType()) {
        case NodeDeleted:
            Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
            if (null != listenerSet) {
                synchronized (listenerSet){
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Removed ledger metadata listeners on ledger {} : {}",
                                ledgerId, listenerSet);
                    }
                    for (LedgerMetadataListener l : listenerSet) {
                        l.onChanged(ledgerId, null);
                    }
                    listeners.remove(ledgerId, listenerSet);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No ledger metadata listeners to remove from ledger {} after it's deleted.", ledgerId);
                }
            }
            break;
        case NodeDataChanged:
            new ReadLedgerMetadataTask(ledgerId).run();
            break;
        default:
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received event {} on {}.", event.getType(), event.getPath());
            }
            break;
        }
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(long ledgerId,
                                                                             LedgerMetadata inputMetadata) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        /*
         * Create a random number and use it as creator token.
         */
        final long cToken = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        final LedgerMetadata metadata;
        if (inputMetadata.getMetadataFormatVersion() > LedgerMetadataSerDe.METADATA_FORMAT_VERSION_2) {
            metadata = LedgerMetadataBuilder.from(inputMetadata).withCToken(cToken).build();
        } else {
            metadata = inputMetadata;
        }
        String ledgerPath = getLedgerPath(ledgerId);
        StringCallback scb = new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (rc == Code.OK.intValue()) {
                    promise.complete(new Versioned<>(metadata, new LongVersion(0)));
                } else if (rc == Code.NODEEXISTS.intValue()) {
                    LOG.info("Ledger metadata for {} appears to already exist, checking cToken",
                            ledgerId);
                    if (metadata.getMetadataFormatVersion() > 2) {
                        CompletableFuture<Versioned<LedgerMetadata>> readFuture = readLedgerMetadata(ledgerId);
                        readFuture.handle((readMetadata, exception) -> {
                            if (exception == null) {
                                if (readMetadata.getValue().getCToken() == cToken) {
                                    FutureUtils.complete(promise, new Versioned<>(metadata, new LongVersion(0)));
                                } else {
                                    LOG.warn("Failed to create ledger metadata for {} which already exists", ledgerId);
                                    promise.completeExceptionally(new BKException.BKLedgerExistException());
                                }
                            } else if (exception instanceof KeeperException.NoNodeException) {
                                // This is a pretty strange case.  We tried to create the node, found that it
                                // already exists, but failed to find it when we reread it.  It's possible that
                                // we successfully created it, got an erroneous NODEEXISTS due to a resend,
                                // and then it got removed.  It's also possible that we actually lost the race
                                // and then it got removed.  I'd argue that returning an error here is the right
                                // path since recreating it is likely to cause problems.
                                LOG.warn("Ledger {} appears to have already existed and then been removed, failing"
                                        + " with LedgerExistException", ledgerId);
                                promise.completeExceptionally(new BKException.BKLedgerExistException());
                            } else {
                                LOG.error("Could not validate node for ledger {} after LedgerExistsException", ledgerId,
                                        exception);
                                promise.completeExceptionally(new BKException.ZKException());
                            }
                            return null;
                        });
                    } else {
                        LOG.warn("Failed to create ledger metadata for {} which already exists", ledgerId);
                        promise.completeExceptionally(new BKException.BKLedgerExistException());
                    }
                } else {
                    LOG.error("Could not create node for ledger {}", ledgerId,
                            KeeperException.create(Code.get(rc), path));
                    promise.completeExceptionally(new BKException.ZKException());
                }
            }
        };
        final byte[] data;
        try {
            data = serDe.serialize(metadata);
        } catch (IOException ioe) {
            promise.completeExceptionally(new BKException.BKMetadataSerializationException(ioe));
            return promise;
        }

        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        ZkUtils.asyncCreateFullPathOptimistic(zk, ledgerPath, data, zkAcls,
                                              CreateMode.PERSISTENT, scb, null);
        return promise;
    }

    @Override
    public CompletableFuture<Void> removeLedgerMetadata(final long ledgerId, final Version version) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        int znodeVersion = -1;
        if (Version.NEW == version) {
            LOG.error("Request to delete ledger {} metadata with version set to the initial one", ledgerId);
            promise.completeExceptionally(new BKException.BKMetadataVersionException());
            return promise;
        } else if (Version.ANY != version) {
            if (!(version instanceof LongVersion)) {
                LOG.info("Not an instance of ZKVersion: {}", ledgerId);
                promise.completeExceptionally(new BKException.BKMetadataVersionException());
                return promise;
            } else {
                znodeVersion = (int) ((LongVersion) version).getLongVersion();
            }
        }

        VoidCallback callbackForDelete = new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    LOG.warn("Ledger node does not exist in ZooKeeper: ledgerId={}.  Returning success.", ledgerId);
                    FutureUtils.complete(promise, null);
                } else if (rc == KeeperException.Code.OK.intValue()) {
                    // removed listener on ledgerId
                    Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                    if (null != listenerSet) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Remove registered ledger metadata listeners on ledger {} after ledger is deleted.",
                                    ledgerId);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("No ledger metadata listeners to remove from ledger {} when it's being deleted.",
                                    ledgerId);
                        }
                    }
                    FutureUtils.complete(promise, null);
                } else {
                    promise.completeExceptionally(new BKException.ZKException());
                }
            }
        };
        String ledgerZnodePath = getLedgerPath(ledgerId);
        if (this instanceof HierarchicalLedgerManager || this instanceof LongHierarchicalLedgerManager) {
            /*
             * do recursive deletes only for HierarchicalLedgerManager and
             * LongHierarchicalLedgerManager
             */
            ZkUtils.asyncDeleteFullPathOptimistic(zk, ledgerZnodePath, znodeVersion, callbackForDelete,
                    ledgerZnodePath);
        } else {
            zk.delete(ledgerZnodePath, znodeVersion, callbackForDelete, null);
        }
        return promise;
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        if (null != listener) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Registered ledger metadata listener {} on ledger {}.", listener, ledgerId);
            }
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
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Unregistered ledger metadata listener {} on ledger {}.", listener, ledgerId);
                    }
                }
                if (listenerSet.isEmpty()) {
                    listeners.remove(ledgerId, listenerSet);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
        return readLedgerMetadata(ledgerId, null);
    }

    protected CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId, Watcher watcher) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        zk.getData(getLedgerPath(ledgerId), watcher, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No such ledger: " + ledgerId,
                                  KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    return;
                }
                if (rc != KeeperException.Code.OK.intValue()) {
                    LOG.error("Could not read metadata for ledger: " + ledgerId,
                              KeeperException.create(KeeperException.Code.get(rc), path));
                    promise.completeExceptionally(new BKException.ZKException());
                    return;
                }
                if (stat == null) {
                    LOG.error("Could not parse ledger metadata for ledger: {}. Stat object is null", ledgerId);
                    promise.completeExceptionally(new BKException.ZKException());
                    return;
                }

                try {
                    LongVersion version = new LongVersion(stat.getVersion());
                    LedgerMetadata metadata = serDe.parseConfig(data, Optional.of(stat.getCtime()));
                    promise.complete(new Versioned<>(metadata, version));
                } catch (Throwable t) {
                    LOG.error("Could not parse ledger metadata for ledger: {}", ledgerId, t);
                    promise.completeExceptionally(new BKException.ZKException());
                }
            }
        }, null);
        return promise;
    }

    @Override
    public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                            Version currentVersion) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        if (!(currentVersion instanceof LongVersion)) {
            promise.completeExceptionally(new BKException.BKMetadataVersionException());
            return promise;
        }
        final LongVersion zv = (LongVersion) currentVersion;

        final byte[] data;
        try {
            data = serDe.serialize(metadata);
        } catch (IOException ioe) {
            promise.completeExceptionally(new BKException.BKMetadataSerializationException(ioe));
            return promise;
        }
        zk.setData(getLedgerPath(ledgerId),
                   data, (int) zv.getLongVersion(),
                   new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.BADVERSION.intValue() == rc) {
                    promise.completeExceptionally(new BKException.BKMetadataVersionException());
                } else if (KeeperException.Code.OK.intValue() == rc) {
                    // update metadata version
                    promise.complete(new Versioned<>(metadata, new LongVersion(stat.getVersion())));
                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                    LOG.warn("Ledger node does not exist in ZooKeeper: ledgerId={}", ledgerId);
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                } else {
                    LOG.warn("Conditional update ledger metadata failed: {}", KeeperException.Code.get(rc));
                    promise.completeExceptionally(new BKException.ZKException());
                }
            }
        }, null);
        return promise;
    }

    /**
     * Process ledgers in a single zk node.
     *
     * <p>
     * for each ledger found in this zk node, processor#process(ledgerId) will be triggerred
     * to process a specific ledger. after all ledgers has been processed, the finalCb will
     * be called with provided context object. The RC passed to finalCb is decided by :
     * <ul>
     * <li> All ledgers are processed successfully, successRc will be passed.
     * <li> Either ledger is processed failed, failureRc will be passed.
     * </ul>
     * </p>
     *
     * @param path
     *          Zk node path to store ledgers
     * @param processor
     *          Processor provided to process ledger
     * @param finalCb
     *          Callback object when all ledgers are processed
     * @param ctx
     *          Context object passed to finalCb
     * @param successRc
     *          RC passed to finalCb when all ledgers are processed successfully
     * @param failureRc
     *          RC passed to finalCb when either ledger is processed failed
     */
    protected void asyncProcessLedgersInSingleNode(
            final String path, final Processor<Long> processor,
            final AsyncCallback.VoidCallback finalCb, final Object ctx,
            final int successRc, final int failureRc) {
        ZkUtils.getChildrenInSingleNode(zk, path, new GenericCallback<List<String>>() {
            @Override
            public void operationComplete(int rc, List<String> ledgerNodes) {
                if (Code.NONODE.intValue() == rc) {
                    finalCb.processResult(successRc, null, ctx);
                    return;
                } else if (Code.OK.intValue() != rc) {
                    finalCb.processResult(failureRc, null, ctx);
                    return;
                }

                Set<Long> zkActiveLedgers = ledgerListToSet(ledgerNodes, path);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Processing ledgers: {}", zkActiveLedgers);
                }

                // no ledgers found, return directly
                if (zkActiveLedgers.size() == 0) {
                    finalCb.processResult(successRc, null, ctx);
                    return;
                }

                MultiCallback mcb = new MultiCallback(zkActiveLedgers.size(), finalCb, ctx,
                                                      successRc, failureRc);
                // start loop over all ledgers
                for (Long ledger : zkActiveLedgers) {
                    processor.process(ledger, mcb);
                }
            }
        });
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
            || isLeadgerIdGeneratorZnode(znode);
    }

    public static boolean isLeadgerIdGeneratorZnode(String znode) {
        return LegacyHierarchicalLedgerManager.IDGEN_ZNODE.equals(znode)
            || LongHierarchicalLedgerManager.IDGEN_ZNODE.equals(znode)
            || znode.startsWith(ZkLedgerIdGenerator.LEDGER_ID_GEN_PREFIX);
    }

    /**
     * regex expression for name of top level parent znode for ledgers (in
     * HierarchicalLedgerManager) or znode of a ledger (in FlatLedgerManager).
     *
     * @return
     */
    protected abstract String getLedgerParentNodeRegex();

    /**
     * whether the child of ledgersRootPath is a top level parent znode for
     * ledgers (in HierarchicalLedgerManager) or znode of a ledger (in
     * FlatLedgerManager).
     *
     * @param znode
     *            Znode Name
     * @return
     */
    public boolean isLedgerParentNode(String znode) {
        return znode.matches(getLedgerParentNodeRegex());
    }

    /**
     * Convert the ZK retrieved ledger nodes to a HashSet for easier comparisons.
     *
     * @param ledgerNodes
     *          zk ledger nodes
     * @param path
     *          the prefix path of the ledger nodes
     * @return ledger id hash set
     */
    protected NavigableSet<Long> ledgerListToSet(List<String> ledgerNodes, String path) {
        NavigableSet<Long> zkActiveLedgers = new TreeSet<Long>();
        for (String ledgerNode : ledgerNodes) {
            if (isSpecialZnode(ledgerNode)) {
                continue;
            }
            try {
                // convert the node path to ledger id according to different ledger manager implementation
                zkActiveLedgers.add(getLedgerId(path + "/" + ledgerNode));
            } catch (IOException e) {
                LOG.warn("Error extracting ledgerId from ZK ledger node: " + ledgerNode);
                // This is a pretty bad error as it indicates a ledger node in ZK
                // has an incorrect format. For now just continue and consider
                // this as a non-existent ledger.
                continue;
            }
        }
        return zkActiveLedgers;
    }

    @Override
    public void close() {
        try {
            scheduler.shutdown();
        } catch (Exception e) {
            LOG.warn("Error when closing zookeeper based ledger manager: ", e);
        }
    }
}
