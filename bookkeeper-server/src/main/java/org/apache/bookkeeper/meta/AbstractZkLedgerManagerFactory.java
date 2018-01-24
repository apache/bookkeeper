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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

/**
 * Abstract ledger manager factory based on zookeeper, which provides common
 * methods such as format and validateAndNukeExistingCluster.
 */
@Slf4j
public abstract class AbstractZkLedgerManagerFactory extends LedgerManagerFactory {
    protected ZooKeeper zk;

    @Override
    public void format(AbstractConfiguration conf, LayoutManager layoutManager)
            throws InterruptedException, KeeperException, IOException {
        try (AbstractZkLedgerManager ledgerManager = (AbstractZkLedgerManager) newLedgerManager()) {
            String ledgersRootPath = conf.getZkLedgersRootPath();
            List<String> children = zk.getChildren(ledgersRootPath, false);
            for (String child : children) {
                if (!AbstractZkLedgerManager.isSpecialZnode(child) && ledgerManager.isLedgerParentNode(child)) {
                    ZKUtil.deleteRecursive(zk, ledgersRootPath + "/" + child);
                }
            }
        }
        // Delete and recreate the LAYOUT information.
        super.format(conf, layoutManager);
    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager layoutManager)
            throws InterruptedException, KeeperException, IOException {
        String zkLedgersRootPath = conf.getZkLedgersRootPath();
        String zkServers = conf.getZkServers();
        AbstractZkLedgerManager zkLedgerManager = (AbstractZkLedgerManager) newLedgerManager();

        /*
         * before proceeding with nuking existing cluster, make sure there
         * are no unexpected znodes under ledgersRootPath
         */
        List<String> ledgersRootPathChildrenList = zk.getChildren(zkLedgersRootPath, false);
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if ((!AbstractZkLedgerManager.isSpecialZnode(ledgersRootPathChildren))
                    && (!zkLedgerManager.isLedgerParentNode(ledgersRootPathChildren))) {
                log.error("Found unexpected znode : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, zkLedgersRootPath);
                return false;
            }
        }

        // formatting ledgermanager deletes ledger znodes
        format(conf, layoutManager);

        // now delete all the special nodes recursively
        ledgersRootPathChildrenList = zk.getChildren(zkLedgersRootPath, false);
        for (String ledgersRootPathChildren : ledgersRootPathChildrenList) {
            if (AbstractZkLedgerManager.isSpecialZnode(ledgersRootPathChildren)) {
                ZKUtil.deleteRecursive(zk, zkLedgersRootPath + "/" + ledgersRootPathChildren);
            } else {
                log.error("Found unexpected znode : {} under ledgersRootPath : {} so exiting nuke operation",
                        ledgersRootPathChildren, zkLedgersRootPath);
                return false;
            }
        }

        // finally deleting the ledgers rootpath
        zk.delete(zkLedgersRootPath, -1);

        log.info("Successfully nuked existing cluster, ZKServers: {} ledger root path: {}",
                zkServers, zkLedgersRootPath);
        return true;
    }
}
