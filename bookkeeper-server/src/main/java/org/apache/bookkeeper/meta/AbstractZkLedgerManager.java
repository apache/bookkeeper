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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
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
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Abstract ledger manager based on zookeeper, which provides common methods such as query zk nodes.
 */
abstract class AbstractZkLedgerManager implements LedgerManager, Watcher {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractZkLedgerManager.class);

    static int ZK_CONNECT_BACKOFF_MS = 200;

    protected final AbstractConfiguration conf;
    protected final ZooKeeper zk;
    protected final String ledgerRootPath;

    // ledger metadata listeners
    protected final ConcurrentMap<Long, Set<LedgerMetadataListener>> listeners =
            new ConcurrentHashMap<Long, Set<LedgerMetadataListener>>();
    // we use this to prevent long stack chains from building up in callbacks
    protected ScheduledExecutorService scheduler;

    protected class ReadLedgerMetadataTask implements Runnable, GenericCallback<LedgerMetadata> {

        final long ledgerId;

        ReadLedgerMetadataTask(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        @Override
        public void run() {
            if (null != listeners.get(ledgerId)) {
                LOG.debug("Re-read ledger metadata for {}.", ledgerId);
                readLedgerMetadata(ledgerId, this, AbstractZkLedgerManager.this);
            } else {
                LOG.debug("Ledger metadata listener for ledger {} is already removed.", ledgerId);
            }
        }

        @Override
        public void operationComplete(int rc, final LedgerMetadata result) {
            if (BKException.Code.OK == rc) {
                final Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (null != listenerSet) {
                    LOG.debug("Ledger metadata is changed for {} : {}.", ledgerId, result);
                    scheduler.submit(new Runnable() {
                        @Override
                        public void run() {
                            synchronized(listenerSet) {
                                for (LedgerMetadataListener listener : listenerSet) {
                                    listener.onChanged(ledgerId, result);
                                }
                            }
                        }
                    });
                }
            } else if (BKException.Code.NoSuchLedgerExistsException == rc) {
                // the ledger is removed, do nothing
                Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                if (null != listenerSet) {
                    LOG.debug("Removed ledger metadata listener set on ledger {} as its ledger is deleted : {}",
                            ledgerId, listenerSet.size());
                }
            } else {
                LOG.warn("Failed on read ledger metadata of ledger {} : {}", ledgerId, rc);
                scheduler.schedule(this, ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * ZooKeeper-based Ledger Manager Constructor
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     */
    protected AbstractZkLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        this.conf = conf;
        this.zk = zk;
        this.ledgerRootPath = conf.getZkLedgersRootPath();
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder().setNameFormat(
                "ZkLedgerManagerScheduler-%d");
        this.scheduler = Executors
                .newSingleThreadScheduledExecutor(tfb.build());
        LOG.debug("Using AbstractZkLedgerManager with root path : {}", ledgerRootPath);
    }

    /**
     * Get the znode path that is used to store ledger metadata
     *
     * @param ledgerId
     *          Ledger ID
     * @return ledger node path
     */
    protected abstract String getLedgerPath(long ledgerId);

    /**
     * Get ledger id from its znode ledger path
     *
     * @param ledgerPath
     *          Ledger path to store metadata
     * @return ledger id
     * @throws IOException when the ledger path is invalid
     */
    protected abstract long getLedgerId(String ledgerPath) throws IOException;

    @Override
    public void process(WatchedEvent event) {
        LOG.info("Received watched event {} from zookeeper based ledger manager.", event);
        if (Event.EventType.None == event.getType()) {
            /** TODO: BOOKKEEPER-537 to handle expire events.
            if (Event.KeeperState.Expired == event.getState()) {
                LOG.info("ZooKeeper client expired on ledger manager.");
                Set<Long> keySet = new HashSet<Long>(listeners.keySet());
                for (Long lid : keySet) {
                    scheduler.submit(new ReadLedgerMetadataTask(lid));
                    LOG.info("Re-read ledger metadata for {} after zookeeper session expired.", lid);
                }
            }
            **/
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
                synchronized(listenerSet){
                    LOG.debug("Removed ledger metadata listeners on ledger {} : {}",
                            ledgerId, listenerSet);
                    for(LedgerMetadataListener l : listenerSet) {
                        unregisterLedgerMetadataListener(ledgerId, l);
                        l.onChanged( ledgerId, null );
                    }
                }
            } else {
                LOG.debug("No ledger metadata listeners to remove from ledger {} after it's deleted.",
                        ledgerId);
            }
            break;
        case NodeDataChanged:
            new ReadLedgerMetadataTask(ledgerId).run();
            break;
        default:
            LOG.debug("Received event {} on {}.", event.getType(), event.getPath());
            break;
        }
    }

    @Override
    public void createLedgerMetadata(final long ledgerId, final LedgerMetadata metadata,
            final GenericCallback<Void> ledgerCb) {
        String ledgerPath = getLedgerPath(ledgerId);
        StringCallback scb = new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                if (rc == Code.OK.intValue()) {
                    // update version
                    metadata.setVersion(new ZkVersion(0));
                    ledgerCb.operationComplete(BKException.Code.OK, null);
                } else if (rc == Code.NODEEXISTS.intValue()) {
                    LOG.warn("Failed to create ledger metadata for {} which already exist", ledgerId);
                    ledgerCb.operationComplete(BKException.Code.LedgerExistException, null);
                } else {
                    LOG.error("Could not create node for ledger {}", ledgerId,
                            KeeperException.create(Code.get(rc), path));
                    ledgerCb.operationComplete(BKException.Code.ZKException, null);
                }
            }
        };
        ZkUtils.asyncCreateFullPathOptimistic(zk, ledgerPath, metadata.serialize(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, scb, null);
    }

    /**
     * Removes ledger metadata from ZooKeeper if version matches.
     *
     * @param   ledgerId    ledger identifier
     * @param   version     local version of metadata znode
     * @param   cb          callback object
     */
    @Override
    public void removeLedgerMetadata(final long ledgerId, final Version version,
            final GenericCallback<Void> cb) {
        int znodeVersion = -1;
        if (Version.NEW == version) {
            LOG.error("Request to delete ledger {} metadata with version set to the initial one", ledgerId);
            cb.operationComplete(BKException.Code.MetadataVersionException, (Void)null);
            return;
        } else if (Version.ANY != version) {
            if (!(version instanceof ZkVersion)) {
                LOG.info("Not an instance of ZKVersion: {}", ledgerId);
                cb.operationComplete(BKException.Code.MetadataVersionException, (Void)null);
                return;
            } else {
                znodeVersion = ((ZkVersion)version).getZnodeVersion();
            }
        }

        zk.delete(getLedgerPath(ledgerId), znodeVersion, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                int bkRc;
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    LOG.warn("Ledger node does not exist in ZooKeeper: ledgerId={}", ledgerId);
                    bkRc = BKException.Code.NoSuchLedgerExistsException;
                } else if (rc == KeeperException.Code.OK.intValue()) {
                    // removed listener on ledgerId
                    Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                    if (null != listenerSet) {
                        LOG.debug("Remove registered ledger metadata listeners on ledger {} after ledger is deleted.",
                                ledgerId, listenerSet);
                    } else {
                        LOG.debug("No ledger metadata listeners to remove from ledger {} when it's being deleted.",
                                ledgerId);
                    }
                    bkRc = BKException.Code.OK;
                } else {
                    bkRc = BKException.Code.ZKException;
                }
                cb.operationComplete(bkRc, (Void)null);
            }
        }, null);
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
    public void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb) {
        readLedgerMetadata(ledgerId, readCb, null);
    }

    protected void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb,
                                      Watcher watcher) {
        zk.getData(getLedgerPath(ledgerId), watcher, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No such ledger: " + ledgerId,
                                  KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                    readCb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                    return;
                }
                if (rc != KeeperException.Code.OK.intValue()) {
                    LOG.error("Could not read metadata for ledger: " + ledgerId,
                              KeeperException.create(KeeperException.Code.get(rc), path));
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }

                LedgerMetadata metadata;
                try {
                    metadata = LedgerMetadata.parseConfig(data, new ZkVersion(stat.getVersion()));
                } catch (IOException e) {
                    LOG.error("Could not parse ledger metadata for ledger: " + ledgerId, e);
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }
                readCb.operationComplete(BKException.Code.OK, metadata);
            }
        }, null);
    }

    @Override
    public void writeLedgerMetadata(final long ledgerId, final LedgerMetadata metadata,
                                    final GenericCallback<Void> cb) {
        Version v = metadata.getVersion();
        if (Version.NEW == v || !(v instanceof ZkVersion)) {
            cb.operationComplete(BKException.Code.MetadataVersionException, null);
            return;
        }
        final ZkVersion zv = (ZkVersion) v;
        zk.setData(getLedgerPath(ledgerId),
                   metadata.serialize(), zv.getZnodeVersion(),
                   new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.BADVERSION.intValue() == rc) {
                    cb.operationComplete(BKException.Code.MetadataVersionException, null);
                } else if (KeeperException.Code.OK.intValue() == rc) {
                    // update metadata version
                    metadata.setVersion(zv.setZnodeVersion(stat.getVersion()));
                    cb.operationComplete(BKException.Code.OK, null);
                } else {
                    LOG.warn("Conditional update ledger metadata failed: ", KeeperException.Code.get(rc));
                    cb.operationComplete(BKException.Code.ZKException, null);
                }
            }
        }, null);
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
                if (Code.OK.intValue() != rc) {
                    finalCb.processResult(failureRc, null, ctx);
                    return;
                }

                Set<Long> zkActiveLedgers = ledgerListToSet(ledgerNodes, path);
                LOG.debug("Processing ledgers: {}", zkActiveLedgers);

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
     * Whether the znode a special znode
     *
     * @param znode
     *          Znode Name
     * @return true  if the znode is a special znode otherwise false
     */
    protected boolean isSpecialZnode(String znode) {
        if (BookKeeperConstants.AVAILABLE_NODE.equals(znode)
                || BookKeeperConstants.COOKIE_NODE.equals(znode)
                || BookKeeperConstants.LAYOUT_ZNODE.equals(znode)
                || BookKeeperConstants.INSTANCEID.equals(znode)
                || BookKeeperConstants.UNDER_REPLICATION_NODE.equals(znode)) {
            return true;
        }
        return false;
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
