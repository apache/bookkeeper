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
import java.util.NoSuchElementException;
import java.util.Set;

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
 * LongHierarchical Ledger Manager which manages ledger meta in zookeeper using 5-level hierarchical znodes.
 *
 * <p>LongHierarchicalLedgerManager splits the generated id into 5 parts (3-4-4-4-4):
 *
 * <pre>
 * &lt;level0 (3 digits)&gt;&lt;level1 (4 digits)&gt;&lt;level2 (4 digits)&gt;&lt;level3 (4 digits)&gt;
 * &lt;level4 (4 digits)&gt;
 * </pre>
 *
 * <p>These 5 parts are used to form the actual ledger node path used to store ledger metadata:
 *
 * <pre>
 * (ledgersRootPath) / level0 / level1 / level2 / level3 / L(level4)
 * </pre>
 *
 * <p>E.g Ledger 0000000000000000001 is split into 5 parts <i>000</i>, <i>0000</i>, <i>0000</i>, <i>0000</i>,
 * <i>0001</i>, which is stored in <i>(ledgersRootPath)/000/0000/0000/0000/L0001</i>. So each znode could have at most
 * 10000 ledgers, which avoids errors during garbage collection due to lists of children that are too long.
 */
class LongHierarchicalLedgerManager extends AbstractHierarchicalLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(LongHierarchicalLedgerManager.class);

    static final String IDGEN_ZNODE = "idgen-long";

    /**
     * Constructor.
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
    public long getLedgerId(String pathName) throws IOException {
        if (!pathName.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + pathName);
        }
        String hierarchicalPath = pathName.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToLongHierarchicalLedgerId(hierarchicalPath);
    }

    @Override
    public String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getLongHierarchicalLedgerPath(ledgerId);
    }

    //
    // Active Ledger Manager
    //

    @Override
    public void asyncProcessLedgers(final Processor<Long> processor, final AsyncCallback.VoidCallback finalCb,
            final Object context, final int successRc, final int failureRc) {

        // If it succeeds, proceed with our own recursive ledger processing for the 63-bit id ledgers
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
            if ((level == 0) && !isLedgerParentNode(lNode)) {
                cb.processResult(successRc, null, context);
                return;
            } else if (level < 3) {
                asyncProcessLevelNodes(nodePath,
                        new RecursiveProcessor(level + 1, nodePath, processor, context, successRc, failureRc), cb,
                        context, successRc, failureRc);
            } else {
                // process each ledger after all ledger are processed, cb will be call to continue processing next
                // level4 node
                asyncProcessLedgersInSingleNode(nodePath, processor, cb, context, successRc, failureRc);
            }
        }
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
        return new LongHierarchicalLedgerRangeIterator(zkOpTimeoutMs);
    }


    /**
     * Iterates recursively through each metadata bucket.
     */
    private class LongHierarchicalLedgerRangeIterator implements LedgerRangeIterator {
        LedgerRangeIterator rootIterator;
        final long zkOpTimeoutMs;

        /**
         * Returns all children with path as a parent.  If path is non-existent,
         * returns an empty list anyway (after all, there are no children there).
         * Maps all exceptions (other than NoNode) to IOException in keeping with
         * LedgerRangeIterator.
         *
         * @param path
         * @return Iterator into set of all children with path as a parent
         * @throws IOException
         */
        List<String> getChildrenAt(String path) throws IOException {
            try {
                List<String> children = ZkUtils.getChildrenInSingleNode(zk, path, zkOpTimeoutMs);
                Collections.sort(children);
                return children;
            } catch (KeeperException.NoNodeException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("NoNodeException at path {}, assumed race with deletion", path);
                }
                return new ArrayList<>();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading ledgers at path " + path, ie);
            }
        }

        /**
         * Represents the ledger range rooted at a leaf node, returns at most one LedgerRange.
         */
        class LeafIterator implements LedgerRangeIterator {
            // Null iff iteration is complete
            LedgerRange range;

            LeafIterator(String path) throws IOException {
                List<String> ledgerLeafNodes = getChildrenAt(path);
                Set<Long> ledgerIds = ledgerListToSet(ledgerLeafNodes, path);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("All active ledgers from ZK for hash node {}: {}", path, ledgerIds);
                }
                if (!ledgerIds.isEmpty()) {
                    range = new LedgerRange(ledgerIds);
                } // else, hasNext() should return false so that advance will skip us and move on
            }

            @Override
            public boolean hasNext() throws IOException {
                return range != null;
            }

            @Override
            public LedgerRange next() throws IOException {
                if (range == null) {
                    throw new NoSuchElementException(
                            "next() must only be called if hasNext() is true");
                }
                LedgerRange ret = range;
                range = null;
                return ret;
            }
        }


        /**
         * The main constraint is that between calls one of two things must be true.
         * 1) nextLevelIterator is null and thisLevelIterator.hasNext() == false: iteration complete, hasNext()
         *    returns false
         * 2) nextLevelIterator is non-null: nextLevelIterator.hasNext() must return true and nextLevelIterator.next()
         *    must return the next LedgerRange
         * The above means that nextLevelIterator != null ==> nextLevelIterator.hasNext()
         * It also means that hasNext() iff nextLevelIterator != null
         */
        private class InnerIterator implements LedgerRangeIterator {
            final String path;
            final int level;

            // Always non-null
            final Iterator<String> thisLevelIterator;
            // non-null iff nextLevelIterator.hasNext() is true
            LedgerRangeIterator nextLevelIterator;

            /**
             * Builds InnerIterator.
             *
             * @param path Subpath for thisLevelIterator
             * @param level Level of thisLevelIterator (must be <= 3)
             * @throws IOException
             */
            InnerIterator(String path, int level) throws IOException {
                this.path = path;
                this.level = level;
                thisLevelIterator = getChildrenAt(path).iterator();
                advance();
            }

            /**
             * Resolves the difference between cases 1 and 2 after nextLevelIterator is exhausted.
             * Pre-condition: nextLevelIterator == null, thisLevelIterator != null
             * Post-condition: nextLevelIterator == null && !thisLevelIterator.hasNext() OR
             *                 nextLevelIterator.hasNext() == true and nextLevelIterator.next()
             *                 yields the next result of next()
             * @throws IOException Exception representing error
             */
            void advance() throws IOException {
                while (thisLevelIterator.hasNext()) {
                    String node = thisLevelIterator.next();
                    if (level == 0 && !isLedgerParentNode(node)) {
                        continue;
                    }
                    LedgerRangeIterator nextIterator = level < 3
                            ? new InnerIterator(path + "/" + node, level + 1)
                            : new LeafIterator(path + "/" + node);
                    if (nextIterator.hasNext()) {
                        nextLevelIterator = nextIterator;
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() throws IOException {
                return nextLevelIterator != null;
            }

            @Override
            public LedgerRange next() throws IOException {
                LedgerRange ret = nextLevelIterator.next();
                if (!nextLevelIterator.hasNext()) {
                    nextLevelIterator = null;
                    advance();
                }
                return ret;
            }
        }

        private LongHierarchicalLedgerRangeIterator(long zkOpTimeoutMs) {
            this.zkOpTimeoutMs = zkOpTimeoutMs;
        }

        private void bootstrap() throws IOException {
            if (rootIterator == null) {
                rootIterator = new InnerIterator(ledgerRootPath, 0);
            }
        }

        @Override
        public synchronized boolean hasNext() throws IOException {
            bootstrap();
            return rootIterator.hasNext();
        }

        @Override
        public synchronized LedgerRange next() throws IOException {
            bootstrap();
            return rootIterator.next();
        }
    }

    @Override
    protected String getLedgerParentNodeRegex() {
        return StringUtils.LONGHIERARCHICAL_LEDGER_PARENT_NODE_REGEX;
    }
}
