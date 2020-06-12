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

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HierarchicalLedgerManager makes use of both LongHierarchicalLedgerManager and LegacyHierarchicalLedgerManager
 * to extend the 31-bit ledger id range of the LegacyHierarchicalLedgerManager to that of the
 * LongHierarchicalLedgerManager while remaining backwards-compatible with the legacy manager.
 *
 * <p>In order to achieve backwards-compatibility, the HierarchicalLedgerManager forwards requests relating to ledger
 * IDs which are < Integer.MAX_INT to the LegacyHierarchicalLedgerManager. The new 5-part directory structure will not
 * appear until a ledger with an ID >= Integer.MAX_INT is created.
 *
 * @see LongHierarchicalLedgerManager
 * @see LegacyHierarchicalLedgerManager
 */
class HierarchicalLedgerManager extends AbstractHierarchicalLedgerManager {
    static final Logger LOG = LoggerFactory.getLogger(HierarchicalLedgerManager.class);

    LegacyHierarchicalLedgerManager legacyLM;
    LongHierarchicalLedgerManager longLM;

    public HierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);
        legacyLM = new LegacyHierarchicalLedgerManager(conf, zk);
        longLM = new LongHierarchicalLedgerManager (conf, zk);
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context, int successRc,
            int failureRc) {
        // Process the old 31-bit id ledgers first.
        legacyLM.asyncProcessLedgers(processor, new VoidCallback(){

            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc == failureRc) {
                    // If it fails, return the failure code to the callback
                    finalCb.processResult(rc, path, ctx);
                } else {
                    // If it succeeds, proceed with our own recursive ledger processing for the 63-bit id ledgers
                    longLM.asyncProcessLedgers(processor, finalCb, context, successRc, failureRc);
                }
            }

        }, context, successRc, failureRc);
    }

    @Override
    public String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getHybridHierarchicalLedgerPath(ledgerId);
    }

    @Override
    protected long getLedgerId(String ledgerPath) throws IOException {
        if (!ledgerPath.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + ledgerPath);
        }
        String hierarchicalPath = ledgerPath.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToLongHierarchicalLedgerId(hierarchicalPath);
    }

    @Override
    public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
        LedgerRangeIterator legacyLedgerRangeIterator = legacyLM.getLedgerRanges(zkOpTimeoutMs);
        LedgerRangeIterator longLedgerRangeIterator = longLM.getLedgerRanges(zkOpTimeoutMs);
        return new HierarchicalLedgerRangeIterator(legacyLedgerRangeIterator, longLedgerRangeIterator);
    }

    private static class HierarchicalLedgerRangeIterator implements LedgerRangeIterator {

        LedgerRangeIterator legacyLedgerRangeIterator;
        LedgerRangeIterator longLedgerRangeIterator;

        HierarchicalLedgerRangeIterator(LedgerRangeIterator legacyLedgerRangeIterator,
                LedgerRangeIterator longLedgerRangeIterator) {
            this.legacyLedgerRangeIterator = legacyLedgerRangeIterator;
            this.longLedgerRangeIterator = longLedgerRangeIterator;
        }

        @Override
        public boolean hasNext() throws IOException {
            return legacyLedgerRangeIterator.hasNext() || longLedgerRangeIterator.hasNext();
        }

        @Override
        public LedgerRange next() throws IOException {
            if (legacyLedgerRangeIterator.hasNext()) {
                return legacyLedgerRangeIterator.next();
            }
            return longLedgerRangeIterator.next();
        }

    }

    @Override
    protected String getLedgerParentNodeRegex() {
        return StringUtils.HIERARCHICAL_LEDGER_PARENT_NODE_REGEX;
    }
}
