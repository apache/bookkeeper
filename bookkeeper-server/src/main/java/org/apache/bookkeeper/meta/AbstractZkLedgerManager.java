package org.apache.bookkeeper.meta;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;

/**
 * Abstract ledger manager based on zookeeper, which provides common methods such as query zk nodes.
 */
abstract class AbstractZkLedgerManager implements LedgerManager {

    static Logger LOG = LoggerFactory.getLogger(AbstractZkLedgerManager.class);

    // Ledger Node Prefix
    static public final String LEDGER_NODE_PREFIX = "L";
    static final String AVAILABLE_NODE = "available";
    static final String COOKIES_NODE = "cookies";

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
     * @param ledgerRootPath
     *          ZooKeeper Path to store ledger metadata
     */
    protected AbstractZkLedgerManager(AbstractConfiguration conf, ZooKeeper zk,
                                      String ledgerRootPath) {
        this.conf = conf;
        this.zk = zk;
        this.ledgerRootPath = ledgerRootPath;
    }

    /**
     * Get all the ledgers in a single zk node
     *
     * @param nodePath
     *          Zookeeper node path
     * @param getLedgersCallback
     *          callback function to process ledgers in a single node
     */
    protected void asyncGetLedgersInSingleNode(final String nodePath, final GenericCallback<HashSet<Long>> getLedgersCallback) {
        // First sync ZK to make sure we're reading the latest active/available ledger nodes.
        zk.sync(nodePath, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sync node path " + path + " return : " + rc);
                }
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error syncing the ledgers node when getting children: ", KeeperException
                            .create(KeeperException.Code.get(rc), path));
                    getLedgersCallback.operationComplete(rc, null);
                    return;
                }
                // Sync has completed successfully so now we can poll ZK
                // and read in the latest set of active ledger nodes.
                doAsyncGetLedgersInSingleNode(nodePath, getLedgersCallback);
            }
        }, null);
    }

    private void doAsyncGetLedgersInSingleNode(final String nodePath,
                                               final GenericCallback<HashSet<Long>> getLedgersCallback) {
        zk.getChildren(nodePath, false, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> ledgerNodes) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("Error polling ZK for the available ledger nodes: ", KeeperException
                            .create(KeeperException.Code.get(rc), path));
                    getLedgersCallback.operationComplete(rc, null);
                    return;
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retrieved current set of ledger nodes: " + ledgerNodes);
                }
                // Convert the ZK retrieved ledger nodes to a HashSet for easier comparisons.
                HashSet<Long> allActiveLedgers = new HashSet<Long>(ledgerNodes.size(), 1.0f);
                for (String ledgerNode : ledgerNodes) {
                    if (isSpecialZnode(ledgerNode)) {
                        continue;
                    }
                    try {
                        // convert the node path to ledger id according to different ledger manager implementation
                        allActiveLedgers.add(getLedgerId(path + "/" + ledgerNode));
                    } catch (IOException ie) {
                        LOG.warn("Error extracting ledgerId from ZK ledger node: " + ledgerNode);
                        // This is a pretty bad error as it indicates a ledger node in ZK
                        // has an incorrect format. For now just continue and consider
                        // this as a non-existent ledger.
                        continue;
                    }
                }

                getLedgersCallback.operationComplete(rc, allActiveLedgers);

            }
        }, null);
    }

    private class GetLedgersCtx {
        int rc;
        HashSet<Long> ledgers = null;
    }

    /**
     * Get all the ledgers in a single zk node
     *
     * @param nodePath
     *          Zookeeper node path
     * @throws IOException
     * @throws InterruptedException
     */
    protected HashSet<Long> getLedgersInSingleNode(final String nodePath)
        throws IOException, InterruptedException {
        final GetLedgersCtx ctx = new GetLedgersCtx();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to get ledgers of node : " + nodePath);
        }
        synchronized (ctx) {
            asyncGetLedgersInSingleNode(nodePath, new GenericCallback<HashSet<Long>>() {
                @Override
                public void operationComplete(int rc, HashSet<Long> zkActiveLedgers) {
                    synchronized (ctx) {
                        if (Code.OK.intValue() == rc) {
                            ctx.ledgers = zkActiveLedgers;
                        }
                        ctx.rc = rc;
                        ctx.notifyAll();
                    }
                }
            });
            ctx.wait();
        }
        if (Code.OK.intValue() != ctx.rc) {
            throw new IOException("Error on getting ledgers from node " + nodePath);
        }
        return ctx.ledgers;
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
        asyncGetLedgersInSingleNode(path, new GenericCallback<HashSet<Long>>() {
            @Override
            public void operationComplete(int rc, HashSet<Long> zkActiveLedgers) {
                if (Code.OK.intValue() != rc) {
                    finalCb.processResult(failureRc, null, ctx);
                    return;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Processing ledgers : " + zkActiveLedgers);
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
     * Whether the znode a special znode
     *
     * @param znode
     *          Znode Name
     * @return true  if the znode is a special znode otherwise false
     */
    protected boolean isSpecialZnode(String znode) {
        if (AVAILABLE_NODE.equals(znode)
            || COOKIES_NODE.equals(znode)
            || LedgerLayout.LAYOUT_ZNODE.equals(znode)) {
            return true;
        }
        return false;
    }

    @Override
    public void close() {
    }
}
