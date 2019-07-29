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
import java.util.NoSuchElementException;
import java.util.Set;

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
 * Manage all ledgers in a single zk node.
 *
 * <p>
 * All ledgers' metadata are put in a single zk node, created using zk sequential node.
 * Each ledger node is prefixed with 'L'.
 * </p>
 */
class FlatLedgerManager extends AbstractZkLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(FlatLedgerManager.class);
    // path prefix to store ledger znodes
    private final String ledgerPrefix;

    /**
     * Constructor.
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     * @throws IOException when version is not compatible
     */
    public FlatLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);

        ledgerPrefix = ledgerRootPath + "/" + StringUtils.LEDGER_NODE_PREFIX;
    }

    @Override
    public String getLedgerPath(long ledgerId) {
        StringBuilder sb = new StringBuilder();
        sb.append(ledgerPrefix)
          .append(StringUtils.getZKStringId(ledgerId));
        return sb.toString();
    }

    @Override
    public long getLedgerId(String nodeName) throws IOException {
        long ledgerId;
        try {
            String[] parts = nodeName.split(ledgerPrefix);
            ledgerId = Long.parseLong(parts[parts.length - 1]);
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return ledgerId;
    }

    @Override
    public void asyncProcessLedgers(final Processor<Long> processor,
                                    final AsyncCallback.VoidCallback finalCb, final Object ctx,
                                    final int successRc, final int failureRc) {
        asyncProcessLedgersInSingleNode(ledgerRootPath, processor, finalCb, ctx, successRc, failureRc);
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
        return new LedgerRangeIterator() {
            // single iterator, can visit only one time
            boolean nextCalled = false;
            LedgerRange nextRange = null;

            private synchronized void preload() throws IOException {
                if (nextRange != null) {
                    return;
                }
                Set<Long> zkActiveLedgers = null;

                try {
                    zkActiveLedgers = ledgerListToSet(
                            ZkUtils.getChildrenInSingleNode(zk, ledgerRootPath, zkOpTimeoutMs),
                            ledgerRootPath);
                    nextRange = new LedgerRange(zkActiveLedgers);
                } catch (KeeperException.NoNodeException e) {
                    throw new IOException("Path does not exist: " + ledgerRootPath, e);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Error when get child nodes from zk", ie);
                }
            }

            @Override
            public synchronized boolean hasNext() throws IOException {
                preload();
                return nextRange != null && nextRange.size() > 0 && !nextCalled;
            }

            @Override
            public synchronized LedgerRange next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                nextCalled = true;
                return nextRange;
            }
        };
    }

    @Override
    protected String getLedgerParentNodeRegex() {
        return StringUtils.FLAT_LEDGER_NODE_REGEX;
    }
}
