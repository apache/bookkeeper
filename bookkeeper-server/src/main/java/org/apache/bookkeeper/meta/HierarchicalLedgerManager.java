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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.Map;
import java.util.NavigableMap;
import java.util.List;

import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.BookKeeperConstants;
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
 * failed to get children list of a too big znode during garbage collection.
 * <p>
 * All actived ledgers found in bookie server is managed in a sorted map, which ease us to pick
 * up all actived ledgers belongs to (level1, level2).
 * </p>
 * <p>
 * Garbage collection in HierarchicalLedgerManager is processed node by node as below:
 * <ul>
 * fetching all level1 nodes, by calling zk#getChildren(ledgerRootPath).
 * <ul>
 * for each level1 node, fetching their level2 nodes, by calling zk#getChildren(ledgerRootPath + "/" + level1)
 * <li> fetch all existed ledgers from zookeeper in level1/level2 node, said <b>zkActiveLedgers</b>
 * <li> fetch all active ledgers from bookie server in level1/level2, said <b>bkActiveLedgers</b>
 * <li> loop over <b>bkActiveLedgers</b> to find those ledgers aren't existed in <b>zkActiveLedgers</b>, do garbage collection on them.
 * </ul>
 * </ul>
 * Since garbage collection is running in background, HierarchicalLedgerManager did gc on single hash
 * node at a time to avoid consuming too much resources.
 * </p>
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
        ZkUtils.createFullPathOptimistic(zk, idGenPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL, new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, final String idPathName) {
                if (rc != KeeperException.Code.OK.intValue()) {
                    LOG.error("Could not generate new ledger id",
                              KeeperException.create(KeeperException.Code.get(rc), path));
                    ledgerCb.operationComplete(rc, null);
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
                    ledgerCb.operationComplete(KeeperException.Code.SYSTEMERROR.intValue(), null);
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
                            ledgerCb.operationComplete(rc, null);
                        } else {
                            // update version
                            metadata.setVersion(new ZkVersion(0));
                            ledgerCb.operationComplete(rc, lid);
                        }
                    }
                };
                ZkUtils.createFullPathOptimistic(zk, ledgerPath, metadata.serialize(),
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
        String ledgerIdStr = StringUtils.getZKStringId(ledgerId);
        // do 2-4-4 split
        StringBuilder sb = new StringBuilder();
        sb.append(ledgerRootPath).append("/").append(
                ledgerIdStr.substring(0, 2)).append("/").append(
                ledgerIdStr.substring(2, 6)).append("/").append(
                BookKeeperConstants.LEDGER_NODE_PREFIX).append(
                ledgerIdStr.substring(6, 10));
        return sb.toString();
    }

    @Override
    public long getLedgerId(String pathName) throws IOException {
        if (!pathName.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + pathName);
        }
        String hierarchicalPath = pathName.substring(ledgerRootPath.length() + 1);
        String[] hierarchicalParts = hierarchicalPath.split("/");
        if (hierarchicalParts.length != 3) {
            throw new IOException("it is not a valid hierarchical path name : " + pathName);
        }
        hierarchicalParts[2] = hierarchicalParts[2]
                .substring(BookKeeperConstants.LEDGER_NODE_PREFIX.length());
        return getLedgerId(hierarchicalParts);
    }

    // get ledger from all level nodes
    private long getLedgerId(String...levelNodes) throws IOException {
        try {
            StringBuilder sb = new StringBuilder();
            for (String node : levelNodes) {
                sb.append(node);
            }
            return Long.parseLong(sb.toString());
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
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

    @Override
    public void garbageCollectLedgers(GarbageCollector gc) {
        if (null == zk) {
            LOG.warn("Skip garbage collecting ledgers because there is no ZooKeeper handle.");
            return;
        }
        // create a snapshot before garbage collection
        NavigableMap<Long, Boolean> snapshot = activeLedgers.snapshot();
        try {
            List<String> l1Nodes = zk.getChildren(ledgerRootPath, null);
            for (String l1Node : l1Nodes) {
                if (isSpecialZnode(l1Node)) {
                    continue;
                }
                try {
                    List<String> l2Nodes = zk.getChildren(ledgerRootPath + "/" + l1Node, null);
                    for (String l2Node : l2Nodes) {
                        doGcByLevel(gc, l1Node, l2Node, snapshot);
                    }
                } catch (Exception e) {
                    LOG.warn("Exception during garbage collecting ledgers for " + l1Node
                             + " of " + ledgerRootPath, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("Exception during garbage collecting inactive/deleted ledgers", e);
        }
    }

    /**
     * Garbage collection a single node level1/level2
     *
     * @param gc
     *          Garbage collector
     * @param level1
     *          1st level node name
     * @param level2
     *          2nd level node name
     * @param snapshot
     *          Snapshot of the active ledgers map.
     * @throws IOException
     * @throws InterruptedException
     */
    void doGcByLevel(GarbageCollector gc, final String level1, final String level2,
                     NavigableMap snapshot)
        throws IOException, InterruptedException {

        StringBuilder nodeBuilder = new StringBuilder();
        nodeBuilder.append(ledgerRootPath).append("/")
                   .append(level1).append("/").append(level2);
        String nodePath = nodeBuilder.toString();

        Set<Long> zkActiveLedgers = getLedgersInSingleNode(nodePath);
        // get hosted ledgers in /level1/level2
        long startLedgerId = getStartLedgerIdByLevel(level1, level2);
        long endLedgerId = getEndLedgerIdByLevel(level1, level2);
        Map<Long, Boolean> bkActiveLedgers = snapshot.subMap(startLedgerId, true, endLedgerId, true);
        if (LOG.isDebugEnabled()) {
            LOG.debug("For hash node: " + level1 + "/" + level2 + ": All active ledgers from ZK: "
                      + zkActiveLedgers + ". Current active ledgers from Bookie: "+ bkActiveLedgers);
        }

        doGc(gc, bkActiveLedgers, zkActiveLedgers);
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
}
