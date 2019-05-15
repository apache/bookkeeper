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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hierarchical Ledger Manager which manages ledger meta in zookeeper using 2-level hierarchical znodes.
 *
 * <p>LegacyHierarchicalLedgerManager splits the generated id into 3 parts (2-4-4):
 * <pre>&lt;level1 (2 digits)&gt;&lt;level2 (4 digits)&gt;&lt;level3 (4 digits)&gt;</pre>
 * These 3 parts are used to form the actual ledger node path used to store ledger metadata:
 * <pre>(ledgersRootPath)/level1/level2/L(level3)</pre>
 * E.g Ledger 0000000001 is split into 3 parts <i>00</i>, <i>0000</i>, <i>0001</i>, which is stored in
 * <i>(ledgersRootPath)/00/0000/L0001</i>. So each znode could have at most 10000 ledgers, which avoids
 * errors during garbage collection due to lists of children that are too long.
 */
class LegacyHierarchicalLedgerManager extends AbstractHierarchicalLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(LegacyHierarchicalLedgerManager.class);

    static final String IDGEN_ZNODE = "idgen";
    private static final String MAX_ID_SUFFIX = "9999";
    private static final String MIN_ID_SUFFIX = "0000";

    private static final ThreadLocal<StringBuilder> threadLocalNodeBuilder = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder();
        }
    };

    /**
     * Constructor.
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     */
    public LegacyHierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);
    }

    @Override
    public String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getShortHierarchicalLedgerPath(ledgerId);
    }

    @Override
    public long getLedgerId(String pathName) throws IOException {
        if (!pathName.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + pathName);
        }
        String hierarchicalPath = pathName.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToHierarchicalLedgerId(hierarchicalPath);
    }

    //
    // Active Ledger Manager
    //

    /**
     * Get the smallest cache id in a specified node /level1/level2.
     *
     * @param level1
     *          1st level node name
     * @param level2
     *          2nd level node name
     * @return the smallest ledger id
     */
    private long getStartLedgerIdByLevel(String level1, String level2) throws IOException {
        return getLedgerId(level1, level2, MIN_ID_SUFFIX);
    }

    /**
     * Get the largest cache id in a specified node /level1/level2.
     *
     * @param level1
     *          1st level node name
     * @param level2
     *          2nd level node name
     * @return the largest ledger id
     */
    private long getEndLedgerIdByLevel(String level1, String level2) throws IOException {
        return getLedgerId(level1, level2, MAX_ID_SUFFIX);
    }

    @Override
    public void asyncProcessLedgers(final Processor<Long> processor,
                                    final AsyncCallback.VoidCallback finalCb, final Object context,
                                    final int successRc, final int failureRc) {
        // process 1st level nodes
        asyncProcessLevelNodes(ledgerRootPath, new Processor<String>() {
            @Override
            public void process(final String l1Node, final AsyncCallback.VoidCallback cb1) {
                if (!isLedgerParentNode(l1Node)) {
                    cb1.processResult(successRc, null, context);
                    return;
                }
                final String l1NodePath = ledgerRootPath + "/" + l1Node;
                // process level1 path, after all children of level1 process
                // it callback to continue processing next level1 node
                asyncProcessLevelNodes(l1NodePath, new Processor<String>() {
                    @Override
                    public void process(String l2Node, AsyncCallback.VoidCallback cb2) {
                        // process level1/level2 path
                        String l2NodePath = ledgerRootPath + "/" + l1Node + "/" + l2Node;
                        // process each ledger
                        // after all ledger are processed, cb2 will be call to continue processing next level2 node
                        asyncProcessLedgersInSingleNode(l2NodePath, processor, cb2,
                                                        context, successRc, failureRc);
                    }
                }, cb1, context, successRc, failureRc);
            }
        }, finalCb, context, successRc, failureRc);
    }

    @Override
    protected String getLedgerParentNodeRegex() {
        return StringUtils.LEGACYHIERARCHICAL_LEDGER_PARENT_NODE_REGEX;
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
        return new LegacyHierarchicalLedgerRangeIterator(zkOpTimeoutMs);
    }

    /**
     * Iterator through each metadata bucket with hierarchical mode.
     */
    private class LegacyHierarchicalLedgerRangeIterator implements LedgerRangeIterator {
        private Iterator<String> l1NodesIter = null;
        private Iterator<String> l2NodesIter = null;
        private String curL1Nodes = "";
        private boolean iteratorDone = false;
        private LedgerRange nextRange = null;
        private final long zkOpTimeoutMs;

        public LegacyHierarchicalLedgerRangeIterator(long zkOpTimeoutMs) {
            this.zkOpTimeoutMs = zkOpTimeoutMs;
        }

        /**
         * Iterate next level1 znode.
         *
         * @return false if have visited all level1 nodes
         * @throws InterruptedException/KeeperException if error occurs reading zookeeper children
         */
        private boolean nextL1Node() throws KeeperException, InterruptedException {
            l2NodesIter = null;
            while (l2NodesIter == null) {
                if (l1NodesIter.hasNext()) {
                    curL1Nodes = l1NodesIter.next();
                } else {
                    return false;
                }
                // Top level nodes are always exactly 2 digits long. (Don't pick up long hierarchical top level nodes)
                if (!isLedgerParentNode(curL1Nodes)) {
                    continue;
                }
                List<String> l2Nodes = zk.getChildren(ledgerRootPath + "/" + curL1Nodes, null);
                Collections.sort(l2Nodes);
                l2NodesIter = l2Nodes.iterator();
                if (!l2NodesIter.hasNext()) {
                    l2NodesIter = null;
                    continue;
                }
            }
            return true;
        }

        private synchronized void preload() throws IOException {
            while (nextRange == null && !iteratorDone) {
                boolean hasMoreElements = false;
                try {
                    if (l1NodesIter == null) {
                        List<String> l1Nodes = zk.getChildren(ledgerRootPath, null);
                        Collections.sort(l1Nodes);
                        l1NodesIter = l1Nodes.iterator();
                        hasMoreElements = nextL1Node();
                    } else if (l2NodesIter == null || !l2NodesIter.hasNext()) {
                        hasMoreElements = nextL1Node();
                    } else {
                        hasMoreElements = true;
                    }
                } catch (KeeperException ke) {
                    throw new IOException("Error preloading next range", ke);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while preloading", ie);
                }
                if (hasMoreElements) {
                    nextRange = getLedgerRangeByLevel(curL1Nodes, l2NodesIter.next());
                    if (nextRange.size() == 0) {
                        nextRange = null;
                    }
                } else {
                    iteratorDone = true;
                }
            }
        }

        @Override
        public synchronized boolean hasNext() throws IOException {
            preload();
            return nextRange != null && !iteratorDone;
        }

        @Override
        public synchronized LedgerRange next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            LedgerRange r = nextRange;
            nextRange = null;
            return r;
        }

        /**
         * Get a single node level1/level2.
         *
         * @param level1
         *          1st level node name
         * @param level2
         *          2nd level node name
         * @throws IOException
         */
        LedgerRange getLedgerRangeByLevel(final String level1, final String level2)
                throws IOException {
            StringBuilder nodeBuilder = threadLocalNodeBuilder.get();
            nodeBuilder.setLength(0);
            nodeBuilder.append(ledgerRootPath).append("/")
                       .append(level1).append("/").append(level2);
            String nodePath = nodeBuilder.toString();
            List<String> ledgerNodes = null;
            try {
                ledgerNodes = ZkUtils.getChildrenInSingleNode(zk, nodePath, zkOpTimeoutMs);
            } catch (KeeperException.NoNodeException e) {
                /* If the node doesn't exist, we must have raced with a recursive node removal, just
                 * return an empty list. */
                ledgerNodes = new ArrayList<>();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Error when get child nodes from zk", e);
            }
            NavigableSet<Long> zkActiveLedgers = ledgerListToSet(ledgerNodes, nodePath);
            if (LOG.isDebugEnabled()) {
                LOG.debug("All active ledgers from ZK for hash node "
                          + level1 + "/" + level2 + " : " + zkActiveLedgers);
            }

            return new LedgerRange(zkActiveLedgers.subSet(getStartLedgerIdByLevel(level1, level2), true,
                                                          getEndLedgerIdByLevel(level1, level2), true));
        }
    }
}
