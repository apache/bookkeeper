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

import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Lease.KeepAliveListener;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.PutOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Etcd registration manager.
 */
public class EtcdRegistrationManager implements RegistrationManager {

    private String scope;
    private Client client;
    private KV kvClient;
    private Lease leaseClient;
    private Watch watchClient;
    private LayoutManager layoutManager;
    private long leaseId;
    private ScheduledExecutorService listenerExecutor;

    // registration paths
    private String bookieRegistrationPath;
    private String bookieReadonlyRegistrationPath;

    @Override
    public RegistrationManager initialize(ServerConfiguration conf, RegistrationListener listener, StatsLogger statsLogger) throws BookieException {
        // initialize etcd kv and lease client
        this.scope = conf.getZkLedgersRootPath();

        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath() + "/writable";
        this.bookieReadonlyRegistrationPath = conf.getZkAvailableBookiesPath() + "/" + READONLY;

        // TODO: initialize etcd
        this.client = Client.builder()
            .endpoints(conf.getZkServers()) // TODO: make a more general name
            .build();
        this.kvClient = client.getKVClient();
        this.leaseClient = client.getLeaseClient();
        this.watchClient = client.getWatchClient();

        this.layoutManager = new EtcdLayoutManager(kvClient, leaseClient, scope);
        return this;
    }

    @Override
    public void close() {
        kvClient.close();
        leaseClient.close();
        watchClient.close();
        client.close();
    }

    @Override
    public String getClusterInstanceId() throws BookieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerBookie(String bookieId, boolean readOnly) throws BookieException {
        // create 30 seconds lease
        try {
            this.leaseId = this.leaseClient.grant(30).get().getID();
        } catch (InterruptedException e) {
            // TODO:
        } catch (ExecutionException e) {
            // TODO:
        }
        // keep the lease alive
        KeepAliveListener keepAliveListener = this.leaseClient.keepAlive(leaseId);
        // TODO: process the keep alive listener, and propagate keep alive responses to listener
        //       when a lease is expired.

        String regPath;
        if (readOnly) {
            regPath = bookieReadonlyRegistrationPath + "/" + bookieId;
        } else {
            regPath = bookieRegistrationPath + "/" + bookieId;
        }
        try {
            kvClient.txn()
                .If(new Cmp(
                    ByteSequence.fromString(regPath),
                    Op.EQUAL,
                    CmpTarget.value(ByteSequence.fromBytes(new byte[0]))))
                .Then(com.coreos.jetcd.op.Op.put(
                    ByteSequence.fromString(regPath),
                    ByteSequence.fromBytes(new byte[0]),
                    PutOption.newBuilder()
                        .withLeaseId(leaseId) // use the lease id
                        .build()))
                .commit()
                .get();
        } catch (InterruptedException e) {
            // TODO:
        } catch (ExecutionException e) {
            // TODO:
        }

    }


    @Override
    public void unregisterBookie(String bookieId, boolean readOnly) throws BookieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBookieRegistered(String bookieId) throws BookieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeCookie(String bookieId, Versioned<byte[]> cookieData) throws BookieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Versioned<byte[]> readCookie(String bookieId) throws BookieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeCookie(String bookieId, Version version) throws BookieException {
        throw new UnsupportedOperationException();
    }

    @Override
    public LayoutManager getLayoutManager() {
        return layoutManager;
    }

    @Override
    public boolean prepareFormat() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean initNewCluster() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean format() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nukeExistingCluster() throws Exception {
        throw new UnsupportedOperationException();
    }
}
