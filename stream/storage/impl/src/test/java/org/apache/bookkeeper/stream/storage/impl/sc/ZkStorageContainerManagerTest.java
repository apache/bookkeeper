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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.testing.MoreAsserts;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ServerAssignmentData;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerFactory;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRegistry;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterMetadataStore;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link ZkStorageContainerManager}.
 */
public class ZkStorageContainerManagerTest extends ZooKeeperClusterTestCase {

    private static final int NUM_STORAGE_CONTAINERS = 32;

    @Rule
    public final TestName runtime = new TestName();

    private final Endpoint myEndpoint = Endpoint.newBuilder()
        .setHostname("127.0.0.1")
        .setPort(4181)
        .build();

    private CuratorFramework curatorClient;
    private StorageContainerFactory mockScFactory;
    private StorageContainerRegistry scRegistry;
    private ZkClusterMetadataStore clusterMetadataStore;
    private ZkStorageContainerManager scManager;

    @Before
    public void setup() {
        curatorClient = CuratorFrameworkFactory.newClient(
            zkServers,
            new ExponentialBackoffRetry(200, 10, 5000));
        curatorClient.start();

        clusterMetadataStore = spy(new ZkClusterMetadataStore(
            curatorClient, zkServers, "/" + runtime.getMethodName()));
        clusterMetadataStore.initializeCluster(NUM_STORAGE_CONTAINERS);

        mockScFactory = mock(StorageContainerFactory.class);
        scRegistry = spy(new StorageContainerRegistryImpl(mockScFactory));

        scManager = new ZkStorageContainerManager(
            myEndpoint,
            new StorageConfiguration(new CompositeConfiguration())
                .setClusterControllerScheduleInterval(1, TimeUnit.SECONDS),
            clusterMetadataStore,
            scRegistry,
            NullStatsLogger.INSTANCE);
    }

    @After
    public void teardown() {
        if (null != scManager) {
            scManager.close();
        }

        if (null != curatorClient) {
            curatorClient.close();
        }

        if (null != clusterMetadataStore) {
            clusterMetadataStore.close();
        }
    }

    private static StorageContainer createStorageContainer(long scId,
                                                           CompletableFuture<StorageContainer> startFuture,
                                                           CompletableFuture<Void> stopFuture) {
        StorageContainer sc = mock(StorageContainer.class);
        when(sc.getId()).thenReturn(scId);
        when(sc.start()).thenReturn(startFuture);
        when(sc.stop()).thenReturn(stopFuture);
        return sc;
    }

    /**
     * Test basic operations such as starting or stopping containers.
     */
    @Test
    public void testBasicOps() throws Exception {
        // start the storage container manager
        scManager.start();

        long containerId = 11L;
        long containerId2 = 22L;

        // mock a container and start it in the registry
        CompletableFuture<StorageContainer> startFuture = new CompletableFuture<>();
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        CompletableFuture<StorageContainer> startFuture2 = new CompletableFuture<>();
        CompletableFuture<Void> stopFuture2 = new CompletableFuture<>();

        StorageContainer mockSc = createStorageContainer(containerId, startFuture, stopFuture);
        when(mockScFactory.createStorageContainer(eq(containerId)))
            .thenReturn(mockSc);

        StorageContainer mockSc2 = createStorageContainer(containerId2, startFuture2, stopFuture2);
        when(mockScFactory.createStorageContainer(eq(containerId2)))
            .thenReturn(mockSc2);

        // update assignment map
        ClusterAssignmentData cad = ClusterAssignmentData.newBuilder()
            .putServers(
                NetUtils.endpointToString(myEndpoint),
                ServerAssignmentData.newBuilder()
                    .addContainers(containerId)
                    .build())
            .build();
        clusterMetadataStore.updateClusterAssignmentData(cad);

        // notify the container to complete startup
        startFuture.complete(mockSc);
        verify(scRegistry, timeout(10000).times(1)).startStorageContainer(eq(containerId));
        MoreAsserts.assertUtil(
            ignored -> scManager.getLiveContainers().size() >= 1,
            () -> null);
        assertEquals(1, scManager.getLiveContainers().size());
        assertTrue(scManager.getLiveContainers().containsKey(containerId));


        // update assignment map to remove containerId and add containerId2
        ClusterAssignmentData newCad = ClusterAssignmentData.newBuilder()
            .putServers(
                NetUtils.endpointToString(myEndpoint),
                ServerAssignmentData.newBuilder()
                    .addContainers(22L)
                    .build())
            .build();
        clusterMetadataStore.updateClusterAssignmentData(newCad);

        // notify the container1 to stop and container2 to start
        FutureUtils.complete(stopFuture, null);
        startFuture2.complete(mockSc2);
        verify(scRegistry, timeout(10000).times(1)).stopStorageContainer(eq(containerId), same(mockSc));
        verify(scRegistry, timeout(10000).times(1)).startStorageContainer(eq(containerId2));
        MoreAsserts.assertUtil(
            ignored -> !scManager.getLiveContainers().containsKey(containerId)
                && scManager.getLiveContainers().containsKey(containerId2),
            () -> null);
        assertEquals(1, scManager.getLiveContainers().size());
        assertFalse(scManager.getLiveContainers().containsKey(containerId));
        assertTrue(scManager.getLiveContainers().containsKey(containerId2));
    }

    @Test
    public void testShutdownPendingStartStorageContainer() throws Exception {
        // start the storage container manager
        scManager.start();

        long containerId = 11L;

        // mock a container and start it in the registry
        CompletableFuture<StorageContainer> startFuture = new CompletableFuture<>();
        CompletableFuture<Void> stopFuture = new CompletableFuture<>();

        StorageContainer mockSc = createStorageContainer(
            containerId, startFuture, stopFuture);
        when(mockScFactory.createStorageContainer(eq(containerId)))
            .thenReturn(mockSc);

        // update assignment map
        ClusterAssignmentData cad = ClusterAssignmentData.newBuilder()
            .putServers(
                NetUtils.endpointToString(myEndpoint),
                ServerAssignmentData.newBuilder()
                    .addContainers(containerId)
                    .build())
            .build();
        clusterMetadataStore.updateClusterAssignmentData(cad);

        // wait until container start is called
        verify(scRegistry, timeout(10000).times(1)).startStorageContainer(eq(containerId));
        assertEquals(0, scManager.getLiveContainers().size());
        assertEquals(1, scManager.getPendingStartStopContainers().size());
        assertTrue(scManager.getPendingStartStopContainers().contains(containerId));

        // now shutting the manager down
        cad = ClusterAssignmentData.newBuilder().build();
        clusterMetadataStore.updateClusterAssignmentData(cad);

        // the container should not be stopped since it is pending starting.
        Thread.sleep(200);
        verify(scRegistry, timeout(10000).times(0)).stopStorageContainer(eq(containerId), same(mockSc));
        assertEquals(1, scManager.getPendingStartStopContainers().size());
        assertTrue(scManager.getPendingStartStopContainers().contains(containerId));

        // now complete the start future and the container is eventually going to shutdown
        FutureUtils.complete(startFuture, mockSc);
        FutureUtils.complete(stopFuture, null);

        verify(scRegistry, timeout(10000).times(1)).stopStorageContainer(eq(containerId), same(mockSc));
        MoreAsserts.assertUtil(
            ignored -> scManager.getPendingStartStopContainers().size() == 0,
            () -> null);
        assertEquals(0, scManager.getLiveContainers().size());
        assertEquals(0, scManager.getPendingStartStopContainers().size());
    }

    @Test
    public void testStartContainerOnFailures() throws Exception {
        scManager.close();

        long containerId = 11L;
        AtomicBoolean returnGoodContainer = new AtomicBoolean(false);

        CompletableFuture<StorageContainer> startFuture = new CompletableFuture<>();
        StorageContainer goodSc = createStorageContainer(containerId, startFuture, FutureUtils.Void());
        mockScFactory = (scId) -> {
            if (returnGoodContainer.get()) {
                return goodSc;
            } else {
                return createStorageContainer(
                    scId,
                    FutureUtils.exception(new Exception("Failed to start")),
                    FutureUtils.Void()
                );
            }
        };
        scRegistry = spy(new StorageContainerRegistryImpl(mockScFactory));

        scManager = new ZkStorageContainerManager(
            myEndpoint,
            new StorageConfiguration(new CompositeConfiguration())
                .setClusterControllerScheduleInterval(1, TimeUnit.SECONDS),
            clusterMetadataStore,
            scRegistry,
            NullStatsLogger.INSTANCE);


        // start the storage container manager
        scManager.start();

        // update assignment map
        ClusterAssignmentData cad = ClusterAssignmentData.newBuilder()
            .putServers(
                NetUtils.endpointToString(myEndpoint),
                ServerAssignmentData.newBuilder()
                    .addContainers(containerId)
                    .build())
            .build();
        clusterMetadataStore.updateClusterAssignmentData(cad);

        // wait until container start is called and verify it is not started.
        verify(scRegistry, timeout(10000).atLeastOnce()).startStorageContainer(eq(containerId));
        assertEquals(0, scManager.getLiveContainers().size());

        // flip the flag to return a good container to simulate successful startup
        returnGoodContainer.set(true);
        FutureUtils.complete(startFuture, goodSc);

        // wait until container start is called again and the container is started
        MoreAsserts.assertUtil(
            ignored -> scManager.getLiveContainers().size() >= 1,
            () -> null);
        assertEquals(1, scManager.getLiveContainers().size());
        assertTrue(scManager.getLiveContainers().containsKey(containerId));
    }


}
