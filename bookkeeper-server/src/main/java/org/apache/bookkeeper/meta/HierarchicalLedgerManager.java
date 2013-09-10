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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hierarchical Ledger Manager which manages ledger meta in zookeeper using 2-level hierarchical znodes.
 *
 * <p>
 * Hierarchical Ledger Manager first obtain a global unique id from zookeeper using a EPHEMERAL_SEQUENTIAL
 * znode <i>(ledgersRootPath)/ledgers/idgen/ID-</i>.
 * Since zookeeper sequential counter has a format of %10d -- that is 10 digits with 0 (zero) padding, i.e.
 * "&lt;path&gt;0000000001", HierarchicalLedgerManager splits the generated id into 3 parts (2-4-4):
 * <pre>&lt;level1 (2 digits)&gt;&lt;level2 (4 digits)&gt;&lt;level3 (4 digits)&gt;</pre>
 * These 3 parts are used to form the actual ledger node path used to store ledger metadata:
 * <pre>(ledgersRootPath)/level1/level2/L(level3)</pre>
 * E.g Ledger 0000000001 is split into 3 parts <i>00</i>, <i>0000</i>, <i>0001</i>, which is stored in
 * <i>(ledgersRootPath)/00/0000/L0001</i>. So each znode could have at most 10000 ledgers, which avoids
 * errors during garbage collection due to lists of children that are too long.
 */
class HierarchicalLedgerManager extends AbstractZkLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(HierarchicalLedgerManager.class);

    static final String IDGEN_ZNODE = "idgen";
    static final String IDGENERATION_PREFIX = "/" + IDGEN_ZNODE + "/ID-";
    private static final String MAX_ID_SUFFIX = "9999";
    private static final String MIN_ID_SUFFIX = "0000";

    // Path to generate global id
    private final String idGenPath;

    // we use this to prevent long stack chains from building up in callbacks
    ScheduledExecutorService scheduler;

    /**
     * Constructor
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     */
    public HierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);

        this.idGenPath = ledgerRootPath + IDGENERATION_PREFIX;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        LOG.debug("Using HierarchicalLedgerManager with root path : {}", ledgerRootPath);
    }

    @Override
    public void close() {
        try {
            scheduler.shutdown();
        } catch (Exception e) {
            LOG.warn("Error when closing HierarchicalLedgerManager : ", e);
        }
        super.close();
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
                String ledgerPath = getLedgerPath(ledgerId);
                final long lid = ledgerId;
                StringCallback scb = new StringCallback() {
                    @Override
                    public void processResult(int rc, String path,
                            Object ctx, String name) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            LOG.error("Could not create node for ledger",
                                      KeeperException.create(KeeperException.Code.get(rc), path));
                            ledgerCb.operationComplete(BKException.Code.ZKException, null);
                        } else {
                            // update version
                            metadata.setVersion(new ZkVersion(0));
                            ledgerCb.operationComplete(BKException.Code.OK, lid);
                        }
                    }
                };
                ZkUtils.asyncCreateFullPathOptimistic(zk, ledgerPath, metadata.serialize(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, scb, null);
                // delete the znode for id generation
                scheduler.submit(new Runnable() {
                    @Override
                    public void run() {
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
                });
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
    public String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getHierarchicalLedgerPath(ledgerId);
    }

    @Override
    public long getLedgerId(String pathName) throws IOException {
        if (!pathName.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + pathName);
        }
        String hierarchicalPath = pathName.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToHierarchicalLedgerId(hierarchicalPath);
    }

    // get ledger from all level nodes
    private long getLedgerId(String...levelNodes) throws IOException {
        return StringUtils.stringToHierarchicalLedgerId(levelNodes);
    }

    //
    // Active Ledger Manager
    //

    /**
     * Get the smallest cache id in a specified node /level1/level2
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
     * Get the largest cache id in a specified node /level1/level2
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
                if (isSpecialZnode(l1Node)) {
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

    /**
     * Process hash nodes in a given path
     */
    private void asyncProcessLevelNodes(
        final String path, final Processor<String> processor,
        final AsyncCallback.VoidCallback finalCb, final Object context,
        final int successRc, final int failureRc) {
        zk.sync(path, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("Error syncing path " + path + " when getting its chidren: ",
                              KeeperException.create(KeeperException.Code.get(rc), path));
                    finalCb.processResult(failureRc, null, context);
                    return;
                }

                zk.getChildren(path, false, new AsyncCallback.ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx,
                                              List<String> levelNodes) {
                        if (rc != Code.OK.intValue()) {
                            LOG.error("Error polling hash nodes of " + path,
                                      KeeperException.create(KeeperException.Code.get(rc), path));
                            finalCb.processResult(failureRc, null, context);
                            return;
                        }
                        AsyncListProcessor<String> listProcessor =
                                new AsyncListProcessor<String>(scheduler);
                        // process its children
                        listProcessor.process(levelNodes, processor, finalCb,
                                              context, successRc, failureRc);
                    }
                }, null);
            }
        }, null);
    }

    /**
     * Process list one by one in asynchronize way. Process will be stopped immediately
     * when error occurred.
     */
    private static class AsyncListProcessor<T> {
        // use this to prevent long stack chains from building up in callbacks
        ScheduledExecutorService scheduler;

        /**
         * Constructor
         *
         * @param scheduler
         *          Executor used to prevent long stack chains
         */
        public AsyncListProcessor(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        /**
         * Process list of items
         *
         * @param data
         *          List of data to process
         * @param processor
         *          Callback to process element of list when success
         * @param finalCb
         *          Final callback to be called after all elements in the list are processed
         * @param contxt
         *          Context of final callback
         * @param successRc
         *          RC passed to final callback on success
         * @param failureRc
         *          RC passed to final callback on failure
         */
        public void process(final List<T> data, final Processor<T> processor,
                            final AsyncCallback.VoidCallback finalCb, final Object context,
                            final int successRc, final int failureRc) {
            if (data == null || data.size() == 0) {
                finalCb.processResult(successRc, null, context);
                return;
            }
            final int size = data.size();
            final AtomicInteger current = new AtomicInteger(0);
            AsyncCallback.VoidCallback stubCallback = new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (rc != successRc) {
                        // terminal immediately
                        finalCb.processResult(failureRc, null, context);
                        return;
                    }
                    // process next element
                    int next = current.incrementAndGet();
                    if (next >= size) { // reach the end of list
                        finalCb.processResult(successRc, null, context);
                        return;
                    }
                    final T dataToProcess = data.get(next);
                    final AsyncCallback.VoidCallback stub = this;
                    scheduler.submit(new Runnable() {
                        @Override
                        public final void run() {
                            processor.process(dataToProcess, stub);
                        }
                    });
                }
            };
            T firstElement = data.get(0);
            processor.process(firstElement, stubCallback);
        }
    }

    @Override
    protected boolean isSpecialZnode(String znode) {
        return IDGEN_ZNODE.equals(znode) || super.isSpecialZnode(znode);
    }

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        return new HierarchicalLedgerRangeIterator();
    }

    /**
     * Iterator through each metadata bucket with hierarchical mode
     */
    private class HierarchicalLedgerRangeIterator implements LedgerRangeIterator {
        private Iterator<String> l1NodesIter = null;
        private Iterator<String> l2NodesIter = null;
        private String curL1Nodes = "";
        private boolean iteratorDone = false;
        private LedgerRange nextRange = null;

        /**
         * iterate next level1 znode
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
                if (isSpecialZnode(curL1Nodes)) {
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

        synchronized private void preload() throws IOException {
            while (nextRange == null && !iteratorDone) {
                boolean hasMoreElements = false;
                try {
                    if (l1NodesIter == null) {
                        l1NodesIter = zk.getChildren(ledgerRootPath, null).iterator();
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
        synchronized public boolean hasNext() throws IOException {
            preload();
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

        /**
         * Get a single node level1/level2
         *
         * @param level1
         *          1st level node name
         * @param level2
         *          2nd level node name
         * @throws IOException
         */
        LedgerRange getLedgerRangeByLevel(final String level1, final String level2)
                throws IOException {
            StringBuilder nodeBuilder = new StringBuilder();
            nodeBuilder.append(ledgerRootPath).append("/")
                       .append(level1).append("/").append(level2);
            String nodePath = nodeBuilder.toString();
            List<String> ledgerNodes = null;
            try {
                ledgerNodes = ZkUtils.getChildrenInSingleNode(zk, nodePath);
            } catch (InterruptedException e) {
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
