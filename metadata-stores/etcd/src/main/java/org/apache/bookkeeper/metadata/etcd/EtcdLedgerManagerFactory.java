/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.metadata.etcd;

import static com.google.common.base.Preconditions.checkArgument;

import com.coreos.jetcd.KV;
import java.io.IOException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.zookeeper.KeeperException;

/**
 * Etcd based ledger manager factory.
 */
public class EtcdLedgerManagerFactory extends LedgerManagerFactory {

    private String scope;
    private KV kvClient;

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf,
                                           LayoutManager layoutManager,
                                           int factoryVersion) throws IOException {
        checkArgument(layoutManager instanceof EtcdLayoutManager);

        EtcdLayoutManager etcdLayoutManager = (EtcdLayoutManager) layoutManager;

        this.scope = conf.getZkLedgersRootPath();
        this.kvClient = etcdLayoutManager.getKvClient();

        return this;
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        return new EtcdIdGenerator(kvClient, scope);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new EtcdLedgerManager(kvClient, scope);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager() throws KeeperException, InterruptedException, CompatibilityException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager lm) throws InterruptedException, KeeperException, IOException {
        throw new UnsupportedOperationException();
    }
}
