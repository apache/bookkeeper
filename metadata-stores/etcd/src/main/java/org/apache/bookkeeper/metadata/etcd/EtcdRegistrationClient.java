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
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.GetOption.SortTarget;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.ZooKeeper;

/**
 * Etcd based registration client.
 */
public class EtcdRegistrationClient implements RegistrationClient {

    private String scope;
    private Client client;
    private KV kvClient;
    private Lease leaseClient;
    private Watch watchClient;
    private LayoutManager layoutManager;

    // registration paths
    private String bookieRegistrationPath;
    private String bookieReadonlyRegistrationPath;

    @Override
    public RegistrationClient initialize(ClientConfiguration conf,
                                         ScheduledExecutorService scheduler,
                                         StatsLogger statsLogger,
                                         Optional<ZooKeeper> zkOptional) throws BKException {
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
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getWritableBookies() {
        return kvClient.get(
            ByteSequence.fromString(this.bookieRegistrationPath),
            GetOption.newBuilder()
                .withRange(ByteSequence.fromString(this.bookieRegistrationPath + "_/"))
                .withSortField(SortTarget.KEY)
                .withLimit(-1)
                .build())
            .thenApply(response -> {
                // TODO: process the response and convert it to BookieSocketAddress

                return null;
            });
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieSocketAddress>>> getReadOnlyBookies() {
        return null;
    }

    @Override
    public CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        Watcher watcher = watchClient.watch(
            ByteSequence.fromString(this.bookieRegistrationPath),
            WatchOption.newBuilder()
                .withRange(ByteSequence.fromString(this.bookieRegistrationPath + "_/"))
                .build());
        // TODO: need a better async interface for continous listening on watch response.
        // TODO: need to put listener into a loop
        try {
            WatchResponse response = watcher.listen();
            for (WatchEvent event : response.getEvents()) {
                switch (event.getEventType()) {
                    case PUT:
                        // TODO: process a new node
                        break;
                    case DELETE:
                        // TODO: process when an old node is removed
                        break;
                    default:
                        break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return FutureUtils.Void();
    }

    @Override
    public void unwatchWritableBookies(RegistrationListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unwatchReadOnlyBookies(RegistrationListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LayoutManager getLayoutManager() {
        return layoutManager;
    }
}
