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
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.zookeeper.ZooKeeper;

/**
 * Hierarchical Ledger Manager Factory
 */
class HierarchicalLedgerManagerFactory extends LedgerManagerFactory {

    public static final String NAME = "hierarchical";
    public static final int CUR_VERSION = 1;

    AbstractConfiguration conf;
    ZooKeeper zk;

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(final AbstractConfiguration conf,
                                           final ZooKeeper zk,
                                           final int factoryVersion)
    throws IOException {
        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : "
                                + factoryVersion);
        }
        this.conf = conf;
        this.zk = zk;
        return this;
    }

    @Override
    public void uninitialize() throws IOException {
        // since zookeeper instance is passed from outside
        // we don't need to close it here
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new HierarchicalLedgerManager(conf, zk);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException{
        return new ZkLedgerUnderreplicationManager(conf, zk);
    }

    @Override
    public void format(AbstractConfiguration conf, ZooKeeper zk)
            throws InterruptedException, KeeperException, IOException {
        HierarchicalLedgerManager ledgerManager = (HierarchicalLedgerManager) newLedgerManager();
        String ledgersRootPath = conf.getZkLedgersRootPath();
        List<String> children = zk.getChildren(ledgersRootPath, false);
        for (String child : children) {
            if (!HierarchicalLedgerManager.IDGEN_ZNODE.equals(child)
                    && ledgerManager.isSpecialZnode(child)) {
                continue;
            }
            ZKUtil.deleteRecursive(zk, ledgersRootPath + "/" + child);
        }
        // Delete and recreate the LAYOUT information.
        super.format(conf, zk);
    }
}
