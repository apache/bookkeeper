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
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class for managing hierarchical ledgers.
 */
public abstract class AbstractHierarchicalLedgerManager extends AbstractZkLedgerManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHierarchicalLedgerManager.class);

    /**
     * Constructor.
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     */
    public AbstractHierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);
    }

    /**
     * Process hash nodes in a given path.
     */
    void asyncProcessLevelNodes(
        final String path, final Processor<String> processor,
        final AsyncCallback.VoidCallback finalCb, final Object context,
        final int successRc, final int failureRc) {
        zk.sync(path, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc == Code.NONODE.intValue()) {
                    // Raced with node removal
                    finalCb.processResult(successRc, null, context);
                    return;
                } else if (rc != Code.OK.intValue()) {
                    LOG.error("Error syncing path " + path + " when getting its chidren: ",
                              KeeperException.create(KeeperException.Code.get(rc), path));
                    finalCb.processResult(failureRc, null, context);
                    return;
                }

                zk.getChildren(path, false, new AsyncCallback.ChildrenCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx,
                                              List<String> levelNodes) {
                        if (rc == Code.NONODE.intValue()) {
                            // Raced with node removal
                            finalCb.processResult(successRc, null, context);
                            return;
                        } else if (rc != Code.OK.intValue()) {
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
         * Constructor.
         *
         * @param scheduler
         *          Executor used to prevent long stack chains
         */
        public AsyncListProcessor(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        /**
         * Process list of items.
         *
         * @param data
         *          List of data to process
         * @param processor
         *          Callback to process element of list when success
         * @param finalCb
         *          Final callback to be called after all elements in the list are processed
         * @param context
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
                        public void run() {
                            processor.process(dataToProcess, stub);
                        }
                    });
                }
            };
            T firstElement = data.get(0);
            processor.process(firstElement, stubCallback);
        }
    }

    // get ledger from all level nodes
    long getLedgerId(String...levelNodes) throws IOException {
        return StringUtils.stringToHierarchicalLedgerId(levelNodes);
    }

    /**
     * Get all ledger ids in the given zk path.
     *
     * @param ledgerNodes
     *          List of ledgers in the given path
     *          example:- {L1652, L1653, L1650}
     * @param path
     *          The zookeeper path of the ledger ids. The path should start with {@ledgerRootPath}
     *          example (with ledgerRootPath = /ledgers):- /ledgers/00/0053
     */
    @Override
    protected NavigableSet<Long> ledgerListToSet(List<String> ledgerNodes, String path) {
        NavigableSet<Long> zkActiveLedgers = new TreeSet<Long>();

        if (!path.startsWith(ledgerRootPath)) {
            LOG.warn("Ledger path [{}] is not a valid path name, it should start wth {}", path, ledgerRootPath);
            return zkActiveLedgers;
        }

        long ledgerIdPrefix = 0;
        char ch;
        for (int i = ledgerRootPath.length() + 1; i < path.length(); i++) {
            ch = path.charAt(i);
            if (ch < '0' || ch > '9') {
                continue;
            }
            ledgerIdPrefix = ledgerIdPrefix * 10 + (ch - '0');
        }

        for (String ledgerNode : ledgerNodes) {
            if (isSpecialZnode(ledgerNode)) {
                continue;
            }
            long ledgerId = ledgerIdPrefix;
            for (int i = 0; i < ledgerNode.length(); i++) {
                ch = ledgerNode.charAt(i);
                if (ch < '0' || ch > '9') {
                    continue;
                }
                ledgerId = ledgerId * 10 + (ch - '0');
            }
            zkActiveLedgers.add(ledgerId);
        }
        return zkActiveLedgers;
    }
}
