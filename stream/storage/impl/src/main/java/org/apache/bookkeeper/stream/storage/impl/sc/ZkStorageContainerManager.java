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
package org.apache.bookkeeper.stream.storage.impl.sc;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ServerAssignmentData;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManager;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;

/**
 * A zookeeper based implementation of {@link StorageContainerManager}.
 */
@Slf4j
public class ZkStorageContainerManager
    extends AbstractLifecycleComponent<StorageConfiguration>
    implements StorageContainerManager, Consumer<Void> {

    private final Endpoint endpoint;
    private final ClusterMetadataStore metadataStore;
    private final StorageContainerRegistry registry;
    private final ScheduledExecutorService executor;

    // for routing purpose
    private volatile ClusterAssignmentData clusterAssignmentData;
    private volatile Map<Endpoint, ServerAssignmentData> clusterAssignmentMap;
    private volatile ServerAssignmentData myAssignmentData;
    private volatile ConcurrentLongHashMap<Endpoint> containerAssignmentMap;

    // a probe task to probe containers and make sure this manager running containers as assigned
    private ScheduledFuture<?> containerProbeTask;
    private final Duration probeInterval;

    @Getter(AccessLevel.PACKAGE)
    private final Map<Long, StorageContainer> liveContainers;
    @Getter(AccessLevel.PACKAGE)
    private final Set<Long> pendingStartStopContainers;

    public ZkStorageContainerManager(Endpoint myEndpoint,
                                     StorageConfiguration conf,
                                     ClusterMetadataStore clusterMetadataStore,
                                     StorageContainerRegistry registry,
                                     StatsLogger statsLogger) {
        super("zk-storage-container-manager", conf, statsLogger);
        this.endpoint = myEndpoint;
        this.metadataStore = clusterMetadataStore;
        this.registry = registry;
        this.executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("zk-storage-container-manager").build());
        this.liveContainers = Collections.synchronizedMap(Maps.newConcurrentMap());
        this.pendingStartStopContainers = Collections.synchronizedSet(Sets.newConcurrentHashSet());
        this.containerAssignmentMap = new ConcurrentLongHashMap<>();
        this.clusterAssignmentMap = Maps.newHashMap();
        // probe the containers every 1/2 of controller scheduling interval. this ensures the manager
        // can attempt to start containers before controller reassign them.
        this.probeInterval = Duration.ofMillis(conf.getClusterControllerScheduleIntervalMs() / 2);
    }

    @Override
    protected void doStart() {
        // watch the cluster assignment data
        metadataStore.watchClusterAssignmentData(this, executor);
        log.info("Watched cluster assignment data.");

        // schedule the container probe task
        containerProbeTask = executor.scheduleAtFixedRate(
            this::probeContainers, 0, probeInterval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("Scheduled storage container probe task at every {} ms", probeInterval.toMillis());
    }

    @Override
    protected void doStop() {
        // unwatch the cluster assignment data
        metadataStore.unwatchClusterAssignmentData(this);

        // cancel the probe task
        if (!containerProbeTask.cancel(true)) {
            log.warn("Failed to cancel the container probe task.");
        }

        stopContainers();
    }

    @Override
    protected void doClose() throws IOException {
        // close the registry to shutdown the containers
        registry.close();
        // shutdown the scheduler
        executor.shutdown();
    }

    @Override
    public Endpoint getStorageContainer(long scId) {
        return containerAssignmentMap.get(scId);
    }

    void probeContainers() {
        boolean isMyAssignmentRefreshed = refreshMyAssignment();
        if (!isMyAssignmentRefreshed) {
            // no change to my assignment, quitting
            return;
        }

        if (myAssignmentData == null) {
            // I don't have any containers assigned to me, so stop containers that I am running.
            stopContainers();
        } else {
            processMyAssignment(myAssignmentData);
        }
    }

    private boolean refreshMyAssignment() {
        ClusterAssignmentData clusterAssignmentData = metadataStore.getClusterAssignmentData();

        if (null == clusterAssignmentData) {
            log.info("Cluster assignment data is empty, so skip refreshing");
            return false;
        }

        Map<Endpoint, ServerAssignmentData> newAssignmentMap = clusterAssignmentData.getServersMap().entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> NetUtils.parseEndpoint(e.getKey()),
                e -> e.getValue()));

        Set<Endpoint> oldAssignedServers = clusterAssignmentMap.keySet();
        Set<Endpoint> newAssignedServers = newAssignmentMap.keySet();

        Set<Endpoint> serversJoined = Sets.difference(newAssignedServers, oldAssignedServers).immutableCopy();
        Set<Endpoint> serversLeft = Sets.difference(oldAssignedServers, newAssignedServers).immutableCopy();
        Set<Endpoint> commonServers = Sets.intersection(newAssignedServers, oldAssignedServers).immutableCopy();

        processServersLeft(serversLeft, clusterAssignmentMap);
        processServersJoined(serversJoined, newAssignmentMap);
        processServersAssignmentChanged(commonServers, clusterAssignmentMap, newAssignmentMap);

        this.clusterAssignmentMap = newAssignmentMap;
        myAssignmentData = newAssignmentMap.get(endpoint);
        return true;
    }

    private void processServersJoined(Set<Endpoint> serversJoined,
                                      Map<Endpoint, ServerAssignmentData> newAssignmentMap) {
        if (!serversJoined.isEmpty()) {
            log.info("Servers joined : {}", serversJoined);
        }
        serversJoined.forEach(ep -> {
            ServerAssignmentData sad = newAssignmentMap.get(ep);
            if (null != sad) {
                sad.getContainersList().forEach(container -> containerAssignmentMap.put(container, ep));
            }
        });
    }

    private void processServersLeft(Set<Endpoint> serversLeft,
                                    Map<Endpoint, ServerAssignmentData> oldAssignmentMap) {
        if (!serversLeft.isEmpty()) {
            log.info("Servers left : {}", serversLeft);
        }
        serversLeft.forEach(ep -> {
            ServerAssignmentData sad = oldAssignmentMap.get(ep);
            if (null != sad) {
                sad.getContainersList().forEach(container -> containerAssignmentMap.remove(container, ep));
            }
        });
    }

    private void processServersAssignmentChanged(Set<Endpoint> commonServers,
                                                 Map<Endpoint, ServerAssignmentData> oldAssignmentMap,
                                                 Map<Endpoint, ServerAssignmentData> newAssignmentMap) {
        commonServers.forEach(ep -> {

            ServerAssignmentData oldSad = oldAssignmentMap.getOrDefault(ep, ServerAssignmentData.getDefaultInstance());
            ServerAssignmentData newSad = newAssignmentMap.getOrDefault(ep, ServerAssignmentData.getDefaultInstance());

            if (oldSad.equals(newSad)) {
                return;
            } else {
                log.info("Server assignment is change for {}:\nold assignment: {}\nnew assignment: {}",
                    NetUtils.endpointToString(ep), oldSad, newSad);
                oldSad.getContainersList().forEach(container -> containerAssignmentMap.remove(container, ep));
                newSad.getContainersList().forEach(container -> containerAssignmentMap.put(container, ep));
            }

        });
    }


    private void stopContainers() {
        Set<Long> liveContainerSet = ImmutableSet.copyOf(liveContainers.keySet());
        liveContainerSet.forEach(this::stopStorageContainer);
    }

    private void processMyAssignment(ServerAssignmentData myAssignment) {
        Set<Long> assignedContainerSet = myAssignment.getContainersList().stream().collect(Collectors.toSet());
        Set<Long> liveContainerSet = Sets.newHashSet(liveContainers.keySet());

        Set<Long> containersToStart =
            Sets.newHashSet(Sets.difference(assignedContainerSet, liveContainerSet).immutableCopy());
        Set<Long> containersToStop =
            Sets.newHashSet(Sets.difference(liveContainerSet, assignedContainerSet).immutableCopy());

        // if the containers are already in the pending start/stop list, we don't touch it until they are completed.

        containersToStart =
            Sets.filter(containersToStart, container -> !pendingStartStopContainers.contains(container));
        containersToStop =
            Sets.filter(containersToStop, container -> !pendingStartStopContainers.contains(container));

        if (!containersToStart.isEmpty() || !containersToStop.isEmpty()) {
            log.info("Process container changes:\n\tIdeal = {}\n\tLive = {}\n\t"
                    + "Pending = {}\n\tToStart = {}\n\tToStop = {}",
                assignedContainerSet,
                liveContainerSet,
                pendingStartStopContainers,
                containersToStart,
                containersToStop);
        }

        containersToStart.forEach(this::startStorageContainer);
        containersToStop.forEach(this::stopStorageContainer);
    }

    private CompletableFuture<StorageContainer> startStorageContainer(long scId) {
        log.info("Starting storage container ({})", scId);
        StorageContainer sc = liveContainers.get(scId);
        if (null != sc) {
            log.warn("Storage container ({}) is already started", scId);
            return FutureUtils.value(sc);
        } else {
            // register the container to pending list
            pendingStartStopContainers.add(scId);
            return registry
                .startStorageContainer(scId)
                .whenComplete((container, cause) -> {
                    try {
                        if (null != cause) {
                            log.warn("Failed to start storage container ({})", scId, cause);
                        } else {
                            log.info("Successfully started storage container ({})", scId);
                            addStorageContainer(scId, container);
                        }
                    } finally {
                        pendingStartStopContainers.remove(scId);
                    }
                });
        }
    }

    private CompletableFuture<Void> stopStorageContainer(long scId) {
        log.info("Stopping storage container ({})", scId);
        StorageContainer sc = liveContainers.get(scId);
        if (null == sc) {
            log.warn("Storage container ({}) is not alive anymore", scId);
            return FutureUtils.Void();
        } else {
            // register the container to pending list
            pendingStartStopContainers.add(scId);
            return registry
                .stopStorageContainer(scId, sc)
                .whenComplete((container, cause) -> {
                    try {
                        if (cause != null) {
                            log.warn("Failed to stop storage container ({})", scId, cause);
                        } else {
                            log.info("Successfully stopped storage container ({})", scId);
                            removeStorageContainer(scId, sc);
                        }

                    } finally {
                        pendingStartStopContainers.remove(scId);
                    }
                });
        }
    }

    private StorageContainer addStorageContainer(long scId, StorageContainer sc) {
        StorageContainer oldSc = liveContainers.putIfAbsent(scId, sc);
        if (null == oldSc) {
            log.info("Storage container ({}) is added to live set.", sc);
            return sc;
        } else {
            log.warn("Storage container ({}) has already been added to live set", sc);
            sc.stop();
            return oldSc;
        }
    }

    private void removeStorageContainer(long scId, StorageContainer sc) {
        if (liveContainers.remove(scId, sc)) {
            log.info("Storage container ({}) is removed from live set.", scId);
        } else {
            log.warn("Storage container ({}) can't be removed from live set.", scId);
        }
    }

    //
    // Callback on cluster assignment data changed.
    //

    @Override
    public void accept(Void aVoid) {
        // any time if the cluster assignment data is changed, schedule a probe task
        this.executor.submit(() -> probeContainers());
    }
}
