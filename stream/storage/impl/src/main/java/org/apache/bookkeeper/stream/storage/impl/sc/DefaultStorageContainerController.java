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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.proto.cluster.ServerAssignmentData;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The default implementation of storage container controller.
 *
 * <p>The goal of this controller is uniformly distributing storage containers across all alive servers in
 * the cluster.
 *
 * <p>The algorithm here is based on the count-based stream balancer in distributedlog-proxy-server.
 */
@Slf4j
public class DefaultStorageContainerController implements StorageContainerController {

    static final class ServerAssignmentDataComparator
        implements Comparator<Pair<BookieSocketAddress, LinkedList<Long>>> {

        @Override
        public int compare(Pair<BookieSocketAddress, LinkedList<Long>> o1,
                           Pair<BookieSocketAddress, LinkedList<Long>> o2) {
            int res = Integer.compare(o1.getValue().size(), o2.getValue().size());
            if (0 == res) {
                // two servers have same number of container
                // the order of these two servers doesn't matter, so use any attribute than can provide deterministic
                // ordering during state computation is good enough
                return String.CASE_INSENSITIVE_ORDER.compare(
                    o1.getKey().toString(),
                    o2.getKey().toString());
            } else {
                return res;
            }
        }
    }

    @Override
    public ClusterAssignmentData computeIdealState(ClusterMetadata clusterMetadata,
                                                   ClusterAssignmentData currentState,
                                                   Set<BookieSocketAddress> currentCluster) {

        if (currentCluster.isEmpty()) {
            log.info("Current cluster is empty. No alive server is found.");
            return currentState;
        }

        // 1. get current server assignments
        Map<BookieSocketAddress, Set<Long>> currentServerAssignments;
        try {
            currentServerAssignments = currentState.getServersMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e1 -> {
                        try {
                            return new BookieSocketAddress(e1.getKey());
                        } catch (UnknownHostException uhe) {
                            log.error("Invalid cluster ");
                            throw new UncheckedExecutionException("Invalid server found in current assignment map"
                                + e1.getKey(), uhe);
                        }
                    },
                    e2 -> e2.getValue().getContainersList().stream().collect(Collectors.toSet())
                ));
        } catch (UncheckedExecutionException uee) {
            log.warn("Invalid cluster assignment data is found : {} - {}. Recompute assignment from empty state",
                currentState, uee.getCause().getMessage());
            currentServerAssignments = Maps.newHashMap();
        }
        Set<BookieSocketAddress> currentServersAssigned = currentServerAssignments.keySet();

        // 2. if no servers is assigned, initialize the ideal state
        if (currentServersAssigned.isEmpty()) {
            return initializeIdealState(clusterMetadata, currentCluster);
        }

        // 3. get the cluster diffs
        Set<BookieSocketAddress> serversAdded =
            Sets.difference(currentCluster, currentServersAssigned).immutableCopy();
        Set<BookieSocketAddress> serversRemoved =
            Sets.difference(currentServersAssigned, currentCluster).immutableCopy();

        if (serversAdded.isEmpty() && serversRemoved.isEmpty()) {
            // cluster is unchanged, assuming the current state is ideal, no re-assignment is required.
            return currentState;
        }

        log.info("Storage container controller detects cluster changed:\n"
                + "\t {} servers added: {}\n\t {} servers removed: {}",
            serversAdded.size(), serversAdded, serversRemoved.size(), serversRemoved);

        // 4. compute the containers that owned by servers removed. these containers are needed to be reassigned.
        Set<Long> containersToReassign = currentServerAssignments.entrySet().stream()
            .filter(serverEntry -> !currentCluster.contains(serverEntry.getKey()))
            .flatMap(serverEntry -> serverEntry.getValue().stream())
            .collect(Collectors.toSet());

        // 5. use an ordered set as priority deque to sort the servers by the number of assigned containers
        TreeSet<Pair<BookieSocketAddress, LinkedList<Long>>> assignmentQueue =
            new TreeSet<>(new ServerAssignmentDataComparator());
        for (Map.Entry<BookieSocketAddress, Set<Long>> entry : currentServerAssignments.entrySet()) {
            BookieSocketAddress host = entry.getKey();

            if (!currentCluster.contains(host)) {
                if (log.isTraceEnabled()) {
                    log.trace("Host {} is not in current cluster anymore", host);
                }
                continue;
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("Adding host {} to assignment queue", host);
                }
                assignmentQueue.add(Pair.of(host, Lists.newLinkedList(entry.getValue())));
            }
        }

        // 6. add new servers
        for (BookieSocketAddress server : serversAdded) {
            assignmentQueue.add(Pair.of(server, Lists.newLinkedList()));
        }

        // 7. assign the containers that are needed to be reassigned.
        for (Long containerId : containersToReassign) {
            Pair<BookieSocketAddress, LinkedList<Long>> leastLoadedServer = assignmentQueue.pollFirst();
            leastLoadedServer.getValue().add(containerId);
            assignmentQueue.add(leastLoadedServer);
        }

        // 8. rebalance the containers if needed
        int diffAllowed;
        if (assignmentQueue.size() > clusterMetadata.getNumStorageContainers()) {
            diffAllowed = 1;
        } else {
            diffAllowed = clusterMetadata.getNumStorageContainers() % assignmentQueue.size() == 0 ? 0 : 1;
        }

        Pair<BookieSocketAddress, LinkedList<Long>> leastLoaded = assignmentQueue.first();
        Pair<BookieSocketAddress, LinkedList<Long>> mostLoaded = assignmentQueue.last();
        while (mostLoaded.getValue().size() - leastLoaded.getValue().size() > diffAllowed) {
            leastLoaded = assignmentQueue.pollFirst();
            mostLoaded = assignmentQueue.pollLast();

            // move container from mostLoaded to leastLoaded
            Long containerId = mostLoaded.getValue().removeFirst();
            // add the container to the end to avoid balancing this container again.
            leastLoaded.getValue().addLast(containerId);

            assignmentQueue.add(leastLoaded);
            assignmentQueue.add(mostLoaded);

            leastLoaded = assignmentQueue.first();
            mostLoaded = assignmentQueue.last();
        }

        // 9. the new ideal state is computed, finalize it
        Map<String, ServerAssignmentData> newAssignmentMap = Maps.newHashMap();
        assignmentQueue.forEach(assignment -> newAssignmentMap.put(
            assignment.getKey().toString(),
            ServerAssignmentData.newBuilder()
                .addAllContainers(assignment.getValue())
                .build()));
        return ClusterAssignmentData.newBuilder()
            .putAllServers(newAssignmentMap)
            .build();
    }

    static ClusterAssignmentData initializeIdealState(ClusterMetadata clusterMetadata,
                                                      Set<BookieSocketAddress> currentCluster) {
        List<BookieSocketAddress> serverList = Lists.newArrayListWithExpectedSize(currentCluster.size());
        serverList.addAll(currentCluster);
        Collections.shuffle(serverList);

        int numServers = currentCluster.size();
        int numTotalContainers = (int) clusterMetadata.getNumStorageContainers();
        int numContainersPerServer = numTotalContainers / currentCluster.size();

        Map<String, ServerAssignmentData> assignmentMap = Maps.newHashMap();
        for (int serverIdx = 0; serverIdx < serverList.size(); serverIdx++) {
            BookieSocketAddress server = serverList.get(serverIdx);

            int finalServerIdx = serverIdx;
            ServerAssignmentData assignmentData = ServerAssignmentData.newBuilder()
                .addAllContainers(
                    LongStream.rangeClosed(0, numContainersPerServer).boxed()
                        .map(j -> j * numServers + finalServerIdx)
                        .filter(containerId -> containerId < numTotalContainers)
                        .collect(Collectors.toSet()))
                .build();
            assignmentMap.put(server.toString(), assignmentData);
        }

        return ClusterAssignmentData.newBuilder()
            .putAllServers(assignmentMap)
            .build();
    }

}
