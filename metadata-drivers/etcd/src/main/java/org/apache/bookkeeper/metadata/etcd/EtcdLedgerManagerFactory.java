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

import com.coreos.jetcd.Client;
import java.io.IOException;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;

/**
 * Etcd based ledger manager factory.
 */
class EtcdLedgerManagerFactory implements LedgerManagerFactory {

    static final int VERSION = 0;

    private String scope;
    private Client client;

    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf,
                                           LayoutManager layoutManager,
                                           int factoryVersion) throws IOException {
        checkArgument(layoutManager instanceof EtcdLayoutManager);

        EtcdLayoutManager etcdLayoutManager = (EtcdLayoutManager) layoutManager;

        if (VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        try {
            ServiceURI uri = ServiceURI.create(conf.getMetadataServiceUri());
            this.scope = uri.getServicePath();
        } catch (ConfigurationException e) {
            throw new IOException("Invalid metadata service uri", e);
        }
        this.client = etcdLayoutManager.getClient();
        return this;
    }

    @Override
    public void close() {
        // since layout manager is passed from outside.
        // we don't need to close it here
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        return new Etcd64bitIdGenerator(client.getKVClient(), scope);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new EtcdLedgerManager(client, scope);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager()
        throws KeeperException, InterruptedException, CompatibilityException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void format(AbstractConfiguration<?> conf, LayoutManager lm)
        throws InterruptedException, KeeperException, IOException {
        try {
            EtcdRegistrationManager.format(client.getKVClient(), scope);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
    }

    @Override
    public boolean validateAndNukeExistingCluster(AbstractConfiguration<?> conf, LayoutManager lm)
        throws InterruptedException, KeeperException, IOException {
        try {
            return EtcdRegistrationManager.nukeExistingCluster(client.getKVClient(), scope);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
    }
}
