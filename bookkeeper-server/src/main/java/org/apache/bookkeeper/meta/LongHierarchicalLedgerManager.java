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
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LongHierarchical Ledger Manager which manages ledger meta in zookeeper using 4-level hierarchical znodes.
 *
 * <p>
 * LongHierarchicalLedgerManager splits the generated id into 5 parts (3-4-4-4-4):
 *
 * <pre>
 * &lt;level1 (3 digits)&gt;&lt;level2 (4 digits)&gt;&lt;level3 (4 digits)&gt;&lt;level4 (4 digits)&gt;
 * &lt;level5 (4 digits)&gt;
 * </pre>
 *
 * These 5 parts are used to form the actual ledger node path used to store ledger metadata:
 *
 * <pre>
 * (ledgersRootPath) / level1 / level2 / level3 / level4 / L(level5)
 * </pre>
 *
 * E.g Ledger 0000000000000000001 is split into 5 parts <i>000</i>, <i>0000</i>, <i>0000</i>, <i>0000</i>, <i>0001</i>,
 * which is stored in <i>(ledgersRootPath)/000/0000/0000/0000/L0001</i>. So each znode could have at most 10000 ledgers,
 * which avoids errors during garbage collection due to lists of children that are too long.
 */
class LongHierarchicalLedgerManager extends HierarchicalLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(LongHierarchicalLedgerManager.class);

    private static final String MAX_ID_SUFFIX = "9999";
    private static final String MIN_ID_SUFFIX = "0000";

    /**
     * Constructor
     *
     * @param conf
     *            Configuration object
     * @param zk
     *            ZooKeeper Client Handle
     */
    public LongHierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);
    }

    @Override
    public String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getLongHierarchicalLedgerPath(ledgerId);
    }

    @Override
    public long getLedgerId(String pathName) throws IOException {
        if (!pathName.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + pathName);
        }
        String hierarchicalPath = pathName.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToLongHierarchicalLedgerId(hierarchicalPath);
    }

    //
    // Active Ledger Manager
    //

    /**
     * Get the smallest cache id in a specified node /level1/level2/level3/level4
     *
     * @param level1
     *            1st level node name
     * @param level2
     *            2nd level node name
     * @param level3
     *            3rd level node name
     * @param level4
     *            4th level node name
     * @return the smallest ledger id
     */
    private long getStartLedgerIdByLevel(String level1, String level2, String level3, String level4)
            throws IOException {
        return getLedgerId(level1, level2, level3, level4, MIN_ID_SUFFIX);
    }

    /**
     * Get the largest cache id in a specified node /level1/level2/level3/level4
     *
     * @param level1
     *            1st level node name
     * @param level2
     *            2nd level node name
     * @param level3
     *            3rd level node name
     * @param level4
     *            4th level node name
     * @return the largest ledger id
     */
    private long getEndLedgerIdByLevel(String level1, String level2, String level3, String level4) throws IOException {
        return getLedgerId(level1, level2, level3, level4, MAX_ID_SUFFIX);
    }

    @Override
    public void asyncProcessLedgers(final Processor<Long> processor, final AsyncCallback.VoidCallback finalCb,
            final Object context, final int successRc, final int failureRc) {
        asyncProcessLevelNodes(ledgerRootPath,
                new RecursiveProcessor(0, ledgerRootPath, processor, context, successRc, failureRc), finalCb, context,
                successRc, failureRc);
    }

    private class RecursiveProcessor implements Processor<String> {
        private final int level;
        private final String path;
        private final Processor<Long> processor;
        private final Object context;
        private final int successRc;
        private final int failureRc;

        private RecursiveProcessor(int level, String path, Processor<Long> processor, Object context, int successRc,
                int failureRc) {
            this.level = level;
            this.path = path;
            this.processor = processor;
            this.context = context;
            this.successRc = successRc;
            this.failureRc = failureRc;
        }

        @Override
        public void process(String lNode, VoidCallback cb) {
            String nodePath = path + "/" + lNode;
            if ((level == 0) && isSpecialZnode(lNode)) {
                cb.processResult(successRc, null, context);
                return;
            } else if (level < 3) {
                asyncProcessLevelNodes(nodePath,
                        new RecursiveProcessor(level + 1, nodePath, processor, context, successRc, failureRc), cb,
                        context, successRc, failureRc);
            } else {
                // process each ledger after all ledger are processed, cb will be call to continue processing next
                // level5 node
                asyncProcessLedgersInSingleNode(nodePath, processor, cb, context, successRc, failureRc);
            }
        }
    }

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        return new LongHierarchicalLedgerRangeIterator();
    }

    /**
     * Iterator through each metadata bucket with hierarchical mode
     */
    private class LongHierarchicalLedgerRangeIterator implements LedgerRangeIterator {
        private List<Iterator<String>> levelNodesIter;
        private List<String> curLevelNodes;

        private boolean initialized = false;
        private boolean iteratorDone = false;
        private LedgerRange nextRange = null;

        private LongHierarchicalLedgerRangeIterator() {
            levelNodesIter = new ArrayList<Iterator<String>>(Collections.nCopies(4, (Iterator<String>) null));
            curLevelNodes = new ArrayList<String>(Collections.nCopies(4, (String) null));
        }

        private void initialize(String path, int level) throws KeeperException, InterruptedException, IOException {
            List<String> levelNodes = zk.getChildren(path, null);
            Collections.sort(levelNodes);
            if (level == 0) {
                Iterator<String> l0NodesIter = levelNodes.iterator();
                levelNodesIter.set(0, l0NodesIter);
                while (l0NodesIter.hasNext()) {
                    String curL0Node = l0NodesIter.next();
                    if (!isSpecialZnode(curL0Node)) {
                        curLevelNodes.set(0, curL0Node);
                        break;
                    }
                }
            } else {
                Iterator<String> lNodesIter = levelNodes.iterator();
                levelNodesIter.set(level, lNodesIter);
                if (lNodesIter.hasNext()) {
                    String curLNode = lNodesIter.next();
                    curLevelNodes.set(level, curLNode);
                }
            }
            String curLNode = curLevelNodes.get(level);
            if (curLNode != null) {
                if (level != 3) {
                    String nextLevelPath = path + "/" + curLNode;
                    initialize(nextLevelPath, level + 1);
                } else {
                    nextRange = getLedgerRangeByLevel(curLevelNodes);
                    initialized = true;
                }
            } else {
                iteratorDone = true;
            }
        }

        private boolean moveToNext(int level) throws KeeperException, InterruptedException {
            Iterator<String> curLevelNodesIter = levelNodesIter.get(level);
            boolean movedToNextNode = false;
            if (level == 0) {
                while (curLevelNodesIter.hasNext()) {
                    String nextNode = curLevelNodesIter.next();
                    if (isSpecialZnode(nextNode)) {
                        continue;
                    } else {
                        curLevelNodes.set(level, nextNode);
                        movedToNextNode = true;
                        break;
                    }
                }
            } else {
                if (curLevelNodesIter.hasNext()) {
                    String nextNode = curLevelNodesIter.next();
                    curLevelNodes.set(level, nextNode);
                    movedToNextNode = true;
                } else {
                    movedToNextNode = moveToNext(level - 1);
                    if (movedToNextNode) {
                        StringBuilder path = new StringBuilder(ledgerRootPath);
                        for (int i = 0; i < level; i++) {
                            path = path.append("/").append(curLevelNodes.get(i));
                        }
                        List<String> newCurLevelNodesList = zk.getChildren(path.toString(), null);
                        Collections.sort(newCurLevelNodesList);
                        Iterator<String> newCurLevelNodesIter = newCurLevelNodesList.iterator();
                        levelNodesIter.set(level, newCurLevelNodesIter);
                        if (newCurLevelNodesIter.hasNext()) {
                            curLevelNodes.set(level, newCurLevelNodesIter.next());
                            movedToNextNode = true;
                        }
                    }
                }
            }
            return movedToNextNode;
        }

        synchronized private void preload() throws IOException, KeeperException, InterruptedException {
            if (!iteratorDone && !initialized) {
                initialize(ledgerRootPath, 0);
            }
            while (((nextRange == null) || (nextRange.size() == 0)) && !iteratorDone) {
                boolean movedToNextNode = moveToNext(3);
                if (movedToNextNode) {
                    nextRange = getLedgerRangeByLevel(curLevelNodes);
                } else {
                    iteratorDone = true;
                }
            }
        }

        @Override
        synchronized public boolean hasNext() throws IOException {
            try {
                preload();
            } catch (KeeperException ke) {
                throw new IOException("Error preloading next range", ke);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while preloading", ie);
            }
            return nextRange != null && !iteratorDone;
        }

        @Override
        synchronized public LedgerRange next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            LedgerRange r = nextRange;
            nextRange = null;
            return r;
        }

        LedgerRange getLedgerRangeByLevel(List<String> curLevelNodes) throws IOException {
            String level1 = curLevelNodes.get(0);
            String level2 = curLevelNodes.get(1);
            String level3 = curLevelNodes.get(2);
            String level4 = curLevelNodes.get(3);

            StringBuilder nodeBuilder = new StringBuilder();
            nodeBuilder.append(ledgerRootPath).append("/").append(level1).append("/").append(level2).append("/")
                    .append(level3).append("/").append(level4);
            String nodePath = nodeBuilder.toString();
            List<String> ledgerNodes = null;
            try {
                ledgerNodes = ZkUtils.getChildrenInSingleNode(zk, nodePath);
            } catch (InterruptedException e) {
                throw new IOException("Error when get child nodes from zk", e);
            }
            NavigableSet<Long> zkActiveLedgers = ledgerListToSet(ledgerNodes, nodePath);
            if (LOG.isDebugEnabled()) {
                LOG.debug("All active ledgers from ZK for hash node " + level1 + "/" + level2 + "/" + level3 + "/"
                        + level4 + " : " + zkActiveLedgers);
            }
            return new LedgerRange(zkActiveLedgers.subSet(getStartLedgerIdByLevel(level1, level2, level3, level4), true,
                    getEndLedgerIdByLevel(level1, level2, level3, level4), true));
        }
    }
}
