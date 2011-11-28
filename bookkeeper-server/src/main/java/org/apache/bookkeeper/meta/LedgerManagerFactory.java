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

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

/**
 * <code>LedgerManagerFactory</code> takes responsibility of creating new ledger manager.
 */
public class LedgerManagerFactory {
    /**
     * Create new Ledger Manager.
     *
     * @param conf
     *          Configuration Object.
     * @param zk
     *          ZooKeeper Client Handle, talk to zk to know which ledger manager is used.
     * @return new ledger manager
     * @throws IOException
     */
    public static LedgerManager newLedgerManager(
        final AbstractConfiguration conf, final ZooKeeper zk)
            throws IOException, KeeperException, InterruptedException {
        String lmType = conf.getLedgerManagerType();
        String ledgerRootPath = conf.getZkLedgersRootPath();
            
        if (null == ledgerRootPath || ledgerRootPath.length() == 0) {
            throw new IOException("Empty Ledger Root Path.");
        }
        
        // if zk is null, return the default ledger manager
        if (zk == null) {
            return new FlatLedgerManager(conf, zk, 
                    ledgerRootPath, FlatLedgerManager.CUR_VERSION);
        }

        // check that the configured ledger manager is
        // compatible with the existing layout
        LedgerLayout layout = LedgerLayout.readLayout(zk, ledgerRootPath);
        if (layout == null) { // no existing layout
            if (lmType == null 
                || lmType.equals(FlatLedgerManager.NAME)) {
                layout = new LedgerLayout(FlatLedgerManager.NAME, 
                                          FlatLedgerManager.CUR_VERSION);
            } else if (lmType.equals(HierarchicalLedgerManager.NAME)) {
                layout = new LedgerLayout(HierarchicalLedgerManager.NAME, 
                                          HierarchicalLedgerManager.CUR_VERSION);
            } else {
                throw new IOException("Unknown ledger manager type " + lmType);
            }
            try {
                layout.store(zk, ledgerRootPath);
            } catch (KeeperException.NodeExistsException nee) {
                LedgerLayout layout2 = LedgerLayout.readLayout(zk, ledgerRootPath);
                if (!layout2.equals(layout)) {
                    throw new IOException("Contention writing to layout to zookeeper, "
                            + " other layout " + layout2 + " is incompatible with our "
                            + "layout " + layout);
                }
            }
        } else if (lmType != null && !layout.getManagerType().equals(lmType)) {
            throw new IOException("Configured layout " + lmType
                    + " does not match existing layout " + layout.getManagerType());
        }

        // create the ledger manager
        if (FlatLedgerManager.NAME.equals(layout.getManagerType())) {
            return new FlatLedgerManager(conf, zk, ledgerRootPath, 
                                         layout.getManagerVersion());
        } else if (HierarchicalLedgerManager.NAME.equals(layout.getManagerType())) {
            return new HierarchicalLedgerManager(conf, zk, ledgerRootPath,
                                                 layout.getManagerVersion());
        } else {
            throw new IOException("Unknown ledger manager type: " + lmType);
        }
    }

}
