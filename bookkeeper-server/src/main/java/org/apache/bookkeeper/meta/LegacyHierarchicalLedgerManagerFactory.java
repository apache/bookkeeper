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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

/**
 * Hierarchical Ledger Manager Factory.
 */
public class LegacyHierarchicalLedgerManagerFactory extends AbstractZkLedgerManagerFactory {

    public static final String NAME = "legacyhierarchical";
    public static final int CUR_VERSION = 1;

    AbstractConfiguration conf;

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(final AbstractConfiguration conf,
                                           final LayoutManager lm,
                                           final int factoryVersion)
            throws IOException {
        checkArgument(lm instanceof ZkLayoutManager);

        ZkLayoutManager zkLayoutManager = (ZkLayoutManager) lm;

        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : "
                                + factoryVersion);
        }
        this.conf = conf;
        this.zk = zkLayoutManager.getZk();
        return this;
    }

    @Override
    public void close() throws IOException {
        // since zookeeper instance is passed from outside
        // we don't need to close it here
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        return new ZkLedgerIdGenerator(
            zk,
            ZKMetadataDriverBase.resolveZkLedgersRootPath(conf),
            LegacyHierarchicalLedgerManager.IDGEN_ZNODE,
            zkAcls);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new LegacyHierarchicalLedgerManager(conf, zk);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException{
        return new ZkLedgerUnderreplicationManager(conf, zk);
    }
}
