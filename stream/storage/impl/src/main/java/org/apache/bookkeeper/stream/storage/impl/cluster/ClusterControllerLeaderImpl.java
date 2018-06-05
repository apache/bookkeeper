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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeader;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerController;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * A default implementation of {@link ClusterControllerLeader}.
 */
@Slf4j
public class ClusterControllerLeaderImpl implements ClusterControllerLeader, RegistrationListener {

    // the metadata store for reading and writing cluster metadata.
    private final ClusterMetadataStore clusterMetadataStore;

    // the controller logic for assigning containers
    private final StorageContainerController scController;

    // permits that the controller leader can perform server changes.
    @Getter(AccessLevel.PACKAGE)
    private final Semaphore performServerChangesPermits;

    // registration client that watch/unwatch registered servers.
    private final RegistrationClient regClient;

    // keep a reference to a set of available servers
    private volatile Set<BookieSocketAddress> availableServers;

    // variables for suspending controller
    @Getter(AccessLevel.PACKAGE)
    private final Object suspensionLock = new Object();
    private volatile boolean suspended = false;

    // last successful assignment happened at (timestamp)
    @Getter(AccessLevel.PACKAGE)
    private long lastSuccessfulAssigmentAt;

    // the min interval that controller is scheduled to assign containers
    private final Duration scheduleDuration;

    ClusterControllerLeaderImpl(ClusterMetadataStore clusterMetadataStore,
                                StorageContainerController scController,
                                RegistrationClient regClient,
                                Duration scheduleDuration) {
        this.clusterMetadataStore = clusterMetadataStore;
        this.scController = scController;
        this.regClient = regClient;
        this.performServerChangesPermits = new Semaphore(0);
        this.lastSuccessfulAssigmentAt = -1L;
        this.scheduleDuration = scheduleDuration;
    }

    /**
     * Suspend the controller if the leader disconnects from zookeeper.
     */
    @Override
    public void suspend() {
        synchronized (suspensionLock) {
            suspended = true;
            suspensionLock.notifyAll();
        }
    }

    boolean isSuspended() {
        return suspended;
    }

    /**
     * Resume the controller.
     */
    @Override
    public void resume() {
        synchronized (suspensionLock) {
            suspended = false;
            suspensionLock.notifyAll();
        }
    }

    @Override
    public void processAsLeader() throws Exception {
        log.info("Become controller leader to monitor servers for assigning storage containers.");

        performServerChangesPermits.release();

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
                    log.warn("Controller leader encountered exceptions on processing server changes,"
                        + " giving up leadership");

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
                log.info("Controller leader is suspended, waiting for to be resumed");
                suspensionLock.wait();
                log.info("Controller leader is woke up from suspension");
            }
        }
    }

    private void processServerChange() throws InterruptedException {
        // check if the leader can perform server changes
        performServerChangesPermits.acquire();

        long elapsedMs = System.currentTimeMillis() - lastSuccessfulAssigmentAt;
        long remainingMs = scheduleDuration.toMillis() - elapsedMs;
        if (remainingMs > 0) {
            log.info("Waiting {} milliseconds for controller to assign containers", remainingMs);
            TimeUnit.MILLISECONDS.sleep(remainingMs);
        }

        // now, the controller has permits and meet the time requirement for assigning containers.
        performServerChangesPermits.drainPermits();

        Set<BookieSocketAddress> availableServersSnapshot = availableServers;
        if (null == availableServersSnapshot || availableServersSnapshot.isEmpty()) {
            // haven't received any servers from registration service, wait for 200ms and retry.
            if (lastSuccessfulAssigmentAt < 0) {
                log.info("No servers is alive yet. Backoff 200ms and retry.");
                TimeUnit.MILLISECONDS.sleep(200);
                performServerChangesPermits.release();
                return;
            } else {
                // there was already a successful assignment but all the servers are gone.
                // it can be a registration service issue, so don't attempt to reassign the containers.
                // return here direct to wait next server change
                return;
            }
        }

        ClusterMetadata clusterMetadata = clusterMetadataStore.getClusterMetadata();
        ClusterAssignmentData currentState = clusterMetadataStore.getClusterAssignmentData();

        // servers are changed, process the change.
        ClusterAssignmentData newState = scController.computeIdealState(
            clusterMetadata,
            currentState,
            availableServersSnapshot);

        if (newState.equals(currentState)) {
            // no assignment state is changed, so do nothing
            if (log.isDebugEnabled()) {
                log.debug("Assignment state is unchanged - {}", newState);
            }
        } else {
            // update the assignment state
            lastSuccessfulAssigmentAt = System.currentTimeMillis();
            clusterMetadataStore.updateClusterAssignmentData(newState);
        }
    }

    @Override
    public void onBookiesChanged(Versioned<Set<BookieSocketAddress>> bookies) {
        log.info("Cluster topology is changed - new cluster : {}", bookies);
        // when bookies are changed, notify the leader to take actions
        this.availableServers = bookies.getValue();
        performServerChangesPermits.release();
    }
}
