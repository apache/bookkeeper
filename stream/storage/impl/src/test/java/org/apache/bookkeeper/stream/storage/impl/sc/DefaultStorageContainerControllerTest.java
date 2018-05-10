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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.proto.cluster.ServerAssignmentData;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerController.ServerAssignmentDataComparator;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Unit test {@link DefaultStorageContainerController}.
 */
@Slf4j
public class DefaultStorageContainerControllerTest {

    private static final int NUM_STORAGE_CONTAINERS = 32;

    private final ClusterMetadata clusterMetadata;
    private final StorageContainerController controller;
    private final ClusterAssignmentData currentAssignment;

    public DefaultStorageContainerControllerTest() {
        this.controller = new DefaultStorageContainerController();
        this.clusterMetadata = ClusterMetadata.newBuilder()
            .setNumStorageContainers(NUM_STORAGE_CONTAINERS)
            .build();
        this.currentAssignment = ClusterAssignmentData.newBuilder()
            .putServers("default-server", ServerAssignmentData.newBuilder()
                .addContainers(0L)
                .addContainers(1L)
                .addContainers(3L)
                .build())
            .build();
    }

    @Test
    public void testServerAssignmentDataComparator() {
        ServerAssignmentDataComparator comparator = new ServerAssignmentDataComparator();

        LinkedList<Long> serverList1 = new LinkedList<>();
        serverList1.add(1L);
        LinkedList<Long> serverList2 = new LinkedList<>();
        serverList2.add(2L);
        serverList2.add(3L);

        BookieSocketAddress address1 = new BookieSocketAddress("127.0.0.1", 4181);
        BookieSocketAddress address2 = new BookieSocketAddress("127.0.0.1", 4182);

        Pair<BookieSocketAddress, LinkedList<Long>> pair1 = Pair.of(address1, serverList1);
        Pair<BookieSocketAddress, LinkedList<Long>> pair2 = Pair.of(address1, serverList2);
        Pair<BookieSocketAddress, LinkedList<Long>> pair3 = Pair.of(address2, serverList2);

        assertEquals(-1, comparator.compare(pair1, pair2));
        assertEquals(-1, comparator.compare(pair1, pair2));
        assertEquals(
            Integer.compare(address1.hashCode(), address2.hashCode()),
            comparator.compare(pair2, pair3));
    }

    @Test
    public void testComputeIdealStateEmptyCluster() {
        assertSame(
            currentAssignment,
            controller.computeIdealState(
                clusterMetadata,
                currentAssignment,
                Collections.emptySet()));
    }

    private static Set<BookieSocketAddress> newCluster(int numServers) {
        Set<BookieSocketAddress> cluster = IntStream.range(0, numServers)
            .mapToObj(idx -> new BookieSocketAddress("127.0.0.1", 4181 + idx))
            .collect(Collectors.toSet());
        return ImmutableSet.copyOf(cluster);
    }

    private static Set<BookieSocketAddress> newCluster(int numServers, int startServerIdx) {
        Set<BookieSocketAddress> cluster = IntStream.range(0, numServers)
            .mapToObj(idx -> new BookieSocketAddress("127.0.0.1", 4181 + startServerIdx + idx))
            .collect(Collectors.toSet());
        return ImmutableSet.copyOf(cluster);
    }

    private static void verifyAssignmentData(ClusterAssignmentData newAssignment,
                                             Set<BookieSocketAddress> currentCluster,
                                             boolean isInitialIdealState)
            throws Exception {
        int numServers = currentCluster.size();

        assertEquals(numServers, newAssignment.getServersCount());
        Set<Long> assignedContainers = Sets.newHashSet();
        Set<BookieSocketAddress> assignedServers = Sets.newHashSet();

        int numContainersPerServer = NUM_STORAGE_CONTAINERS / numServers;
        int serverIdx = 0;
        for (Map.Entry<String, ServerAssignmentData> entry : newAssignment.getServersMap().entrySet()) {
            log.info("Check assignment for server {} = {}", entry.getKey(), entry.getValue());

            BookieSocketAddress address = new BookieSocketAddress(entry.getKey());
            assignedServers.add(address);
            assertEquals(serverIdx + 1, assignedServers.size());

            ServerAssignmentData serverData = entry.getValue();
            assertEquals(numContainersPerServer, serverData.getContainersCount());
            List<Long> containers = Lists.newArrayList(serverData.getContainersList());
            Collections.sort(containers);
            assignedContainers.addAll(containers);

            if (isInitialIdealState) {
                long startContainerId = containers.get(0);
                for (int i = 0; i < containers.size(); i++) {
                    assertEquals(startContainerId + i * numServers, containers.get(i).longValue());
                }
            }
            ++serverIdx;
        }

        // each server should be assigned with equal number of containers
        assertTrue(Sets.difference(currentCluster, assignedServers).isEmpty());
        // all containers should be assigned
        Set<Long> expectedContainers = LongStream.range(0L, NUM_STORAGE_CONTAINERS)
            .mapToObj(scId -> Long.valueOf(scId))
            .collect(Collectors.toSet());
        assertTrue(Sets.difference(expectedContainers, assignedContainers).isEmpty());
    }

    private static void verifyAssignmentDataWhenHasMoreServers(ClusterAssignmentData newAssignment,
                                                               Set<BookieSocketAddress> currentCluster)
            throws Exception {
        int numServers = currentCluster.size();

        assertEquals(numServers, newAssignment.getServersCount());
        Set<Long> assignedContainers = Sets.newHashSet();
        Set<BookieSocketAddress> assignedServers = Sets.newHashSet();

        int numEmptyServers = 0;
        int numAssignedServers = 0;
        int serverIdx = 0;
        for (Map.Entry<String, ServerAssignmentData> entry : newAssignment.getServersMap().entrySet()) {
            log.info("Check assignment for server {} = {}", entry.getKey(), entry.getValue());

            BookieSocketAddress address = new BookieSocketAddress(entry.getKey());
            assignedServers.add(address);
            assertEquals(serverIdx + 1, assignedServers.size());

            ServerAssignmentData serverData = entry.getValue();
            if (serverData.getContainersCount() > 0) {
                assertEquals(1, serverData.getContainersCount());
                ++numAssignedServers;
            } else {
                ++numEmptyServers;
            }
            List<Long> containers = Lists.newArrayList(serverData.getContainersList());
            Collections.sort(containers);
            assignedContainers.addAll(containers);

            ++serverIdx;
        }

        assertEquals(numServers / 2, numEmptyServers);
        assertEquals(numServers / 2, numAssignedServers);

        // each server should be assigned with equal number of containers
        assertTrue(Sets.difference(currentCluster, assignedServers).isEmpty());
        // all containers should be assigned
        Set<Long> expectedContainers = LongStream.range(0L, NUM_STORAGE_CONTAINERS)
            .mapToObj(scId -> Long.valueOf(scId))
            .collect(Collectors.toSet());
        assertTrue(Sets.difference(expectedContainers, assignedContainers).isEmpty());
    }

    @Test
    public void testComputeIdealStateFromEmptyAssignment() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 8;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);

        ClusterAssignmentData newAssignment = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);

        verifyAssignmentData(newAssignment, currentCluster, true);
    }

    @Test
    public void testComputeIdealStateIfClusterUnchanged() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 8;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);
        ClusterAssignmentData newAssignment = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);
        verifyAssignmentData(newAssignment, currentCluster, true);

        ClusterAssignmentData newAssignment2 = controller.computeIdealState(
            clusterMetadata,
            newAssignment,
            currentCluster);

        // the state should not change if cluster is unchanged.
        assertSame(newAssignment, newAssignment2);
    }

    @Test
    public void testComputeIdealStateWhenHostsRemoved() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 8;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);

        ClusterAssignmentData assignmentData = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);
        verifyAssignmentData(assignmentData, currentCluster, true);

        int newNumServers = 4;
        Set<BookieSocketAddress> newCluster = newCluster(newNumServers);

        ClusterAssignmentData newAssignmentData = controller.computeIdealState(
            clusterMetadata,
            assignmentData,
            newCluster);
        verifyAssignmentData(newAssignmentData, newCluster, false);
    }

    @Test
    public void testComputeIdealStateWhenHostsAdded() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 4;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);

        ClusterAssignmentData assignmentData = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);
        verifyAssignmentData(assignmentData, currentCluster, true);

        int newNumServers = 8;
        Set<BookieSocketAddress> newCluster = newCluster(newNumServers);

        ClusterAssignmentData newAssignmentData = controller.computeIdealState(
            clusterMetadata,
            assignmentData,
            newCluster);
        verifyAssignmentData(newAssignmentData, newCluster, false);
    }

    @Test
    public void testComputeIdealStateWhenHostsRemovedAdded() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 4;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);

        ClusterAssignmentData assignmentData = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);
        verifyAssignmentData(assignmentData, currentCluster, true);

        Set<BookieSocketAddress> serversToAdd = newCluster(6, numServers);
        Set<BookieSocketAddress> serversToRemove = newCluster(2);

        Set<BookieSocketAddress> newCluster = Sets.newHashSet(currentCluster);
        newCluster.addAll(serversToAdd);
        serversToRemove.forEach(newCluster::remove);

        ClusterAssignmentData newAssignmentData = controller.computeIdealState(
            clusterMetadata,
            assignmentData,
            newCluster);
        verifyAssignmentData(newAssignmentData, newCluster, false);
    }

    @Test
    public void testComputeIdealStateWhenHasMoreServers() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 2 * NUM_STORAGE_CONTAINERS;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);

        ClusterAssignmentData assignmentData = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);
        verifyAssignmentDataWhenHasMoreServers(assignmentData, currentCluster);
    }

    @Test
    public void testComputeIdealStateWhenScaleToMoreServers() throws Exception {
        ClusterAssignmentData emptyAssignment = ClusterAssignmentData.newBuilder().build();

        int numServers = 4;
        Set<BookieSocketAddress> currentCluster = newCluster(numServers);

        ClusterAssignmentData assignmentData = controller.computeIdealState(
            clusterMetadata,
            emptyAssignment,
            currentCluster);
        verifyAssignmentData(assignmentData, currentCluster, true);

        numServers = 2 * NUM_STORAGE_CONTAINERS;
        Set<BookieSocketAddress> newCluster = newCluster(numServers);
        ClusterAssignmentData newAssignment = controller.computeIdealState(
            clusterMetadata,
            assignmentData,
            newCluster);
        verifyAssignmentDataWhenHasMoreServers(newAssignment, newCluster);
    }

}
