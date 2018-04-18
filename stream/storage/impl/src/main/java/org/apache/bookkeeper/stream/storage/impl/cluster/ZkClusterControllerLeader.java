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
package org.apache.bookkeeper.stream.storage.impl.cluster;

import java.util.Set;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

/**
 * This is the controller leader which watches the servers in the cluster and handles server level failures.
 *
 * <p>The controller leader is responsible for making sure all storage containers are assigned to servers, and load
 * balancing storage containers if necessary.
 */
@Slf4j
public class ZkClusterControllerLeader implements LeaderSelectorListener, RegistrationListener {

    // the metadata store for reading and writing cluster metadata.
    private final ClusterMetadataStore clusterMetadataStore;

    private final Semaphore serverChanges;

    private final RegistrationClient regClient;

    private volatile Set<BookieSocketAddress> availableServers;

    private final Object suspensionLock = new Object();
    private volatile boolean suspended = false;

    public ZkClusterControllerLeader(ClusterMetadataStore clusterMetadataStore,
                                     RegistrationClient regClient) {
        this.clusterMetadataStore = clusterMetadataStore;
        this.regClient = regClient;
        this.serverChanges = new Semaphore(0);
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        log.info("Become controller leader to monitor servers for assigning storage containers.");

        serverChanges.release();

        // monitoring the servers
        try {
            this.regClient.watchWritableBookies(this).get();
        } catch (Exception e) {
            log.warn("Controller leader fails to watch servers : {}, giving up leadership", e.getMessage());
            throw e;
        }

        // the leader is looping here to react to changes
        while (true) {
            try {
                checkSuspension();

                processServerChange();
            } catch (InterruptedException ie) {
                log.warn("Controller leader is interrupted, giving up leadership");

                // stop monitoring the servers
                this.regClient.unwatchWritableBookies(this);
                throw ie;
            } catch (Exception e) {
                // if the leader is suspended due to losing connection to zookeeper
                // we don't give leadership until it becomes leader again or being interrupted by curator
                if (!suspended) {
                    log.warn("Controller leader encountered exceptions on processing server changes," +
                        " giving up leadership");

                    // stop monitoring the servers
                    this.regClient.unwatchWritableBookies(this);
                    throw e;
                }
            }
        }
    }

    private void checkSuspension() throws InterruptedException {
        synchronized (suspensionLock) {
            while (suspended) {
                suspensionLock.wait();
            }
        }
    }

    private void processServerChange() throws InterruptedException {
        // wait for server changes
        serverChanges.acquire();

        // TODO:

        serverChanges.drainPermits();

        // TODO:
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        log.info("zookeeper connection state changed to {} for cluster controller", newState);
    }

    @Override
    public void onBookiesChanged(Versioned<Set<BookieSocketAddress>> bookies) {
        // when bookies are changed, notify the leader to take actions
        this.availableServers = bookies.getValue();
        serverChanges.release();
    }
}
