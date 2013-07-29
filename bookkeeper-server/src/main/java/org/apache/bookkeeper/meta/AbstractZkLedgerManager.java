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
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract ledger manager based on zookeeper, which provides common methods such as query zk nodes.
 */
abstract class AbstractZkLedgerManager implements LedgerManager {

    static Logger LOG = LoggerFactory.getLogger(AbstractZkLedgerManager.class);

    protected final AbstractConfiguration conf;
    protected final ZooKeeper zk;
    protected final String ledgerRootPath;

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
                    bkRc = BKException.Code.OK;
                } else {
                    bkRc = BKException.Code.ZKException;
                }
                cb.operationComplete(bkRc, (Void)null);
            }
        }, null);
    }

    @Override
    public void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb) {
        zk.getData(getLedgerPath(ledgerId), false, new DataCallback() {
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
                if (KeeperException.Code.BadVersion == rc) {
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
    }
}
