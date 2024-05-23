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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.proto.cluster.ServerAssignmentData;
import org.apache.bookkeeper.stream.storage.exceptions.StorageRuntimeException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test {@link ZkClusterMetadataStore}.
 */
@Slf4j
public class ZkClusterMetadataStoreTest extends ZooKeeperClusterTestCase {

    private static final int NUM_STORAGE_CONTAINERS = 1024;

    @Rule
    public final TestName runtime = new TestName();

    private CuratorFramework curatorClient;
    private ZkClusterMetadataStore store;

    @Before
    public void setup() {
        curatorClient = CuratorFrameworkFactory.newClient(
            zkServers,
            new ExponentialBackoffRetry(200, 10, 5000));
        curatorClient.start();
        store = new ZkClusterMetadataStore(curatorClient, zkServers, "/" + runtime.getMethodName());
        assertTrue(store.initializeCluster(NUM_STORAGE_CONTAINERS));
    }

    @After
    public void teardown() {
        if (null != store) {
            store.close();
        }
        if (null != curatorClient) {
            curatorClient.close();
        }
    }

    @Test
    public void testUninitialized() {
        ZkClusterMetadataStore newStore = new ZkClusterMetadataStore(
            curatorClient, zkServers, "/" + runtime.getMethodName() + "-new");

        try {
            newStore.getClusterMetadata();
            fail("Should fail to get cluster metadata if not initialized");
        } catch (StorageRuntimeException sre) {
            assertTrue(sre.getCause() instanceof KeeperException);
            KeeperException cause = (KeeperException) sre.getCause();
            assertEquals(Code.NONODE, cause.code());
        }

        try {
            newStore.getClusterAssignmentData();
            fail("Should fail to get cluster assignment data if not initialized");
        } catch (StorageRuntimeException sre) {
            assertTrue(sre.getCause() instanceof KeeperException);
            KeeperException cause = (KeeperException) sre.getCause();
            assertEquals(Code.NONODE, cause.code());
        }
    }

    @Test
    public void testInitialize() {
        int numStorageContainers = 2048;
        assertFalse(store.initializeCluster(numStorageContainers));
    }

    @Test
    public void testUpdateClusterMetadata() {
       int numStorageContainers = 4096;
       ClusterMetadata metadata = ClusterMetadata.newBuilder()
           .setNumStorageContainers(numStorageContainers)
           .build();
       store.updateClusterMetadata(metadata);
       assertEquals(metadata, store.getClusterMetadata());
    }

    @Test
    public void testUpdateClusterAssignmentData() {
        ClusterAssignmentData assignmentData = ClusterAssignmentData.newBuilder()
            .putServers(
                "server-0",
                ServerAssignmentData.newBuilder()
                    .addContainers(1L)
                    .addContainers(2L)
                    .build())
            .build();
        store.updateClusterAssignmentData(assignmentData);
        assertEquals(assignmentData, store.getClusterAssignmentData());
    }

    @Test
    public void testWatchClusterAssignmentData() {
        ClusterAssignmentData assignmentData = ClusterAssignmentData.newBuilder()
            .putServers(
                "server-0",
                ServerAssignmentData.newBuilder()
                    .addContainers(1L)
                    .addContainers(2L)
                    .build())
            .build();

        @Cleanup("shutdown")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> watchFuture = new CompletableFuture<>();

        store.watchClusterAssignmentData(data -> {
            FutureUtils.complete(watchFuture, null);
        }, executor);

        store.updateClusterAssignmentData(assignmentData);

        watchFuture.join();
        assertEquals(assignmentData, store.getClusterAssignmentData());
    }

    @Test
    public void testUnwatchClusterAssignmentData() throws Exception {
        ClusterAssignmentData assignmentData = ClusterAssignmentData.newBuilder()
            .putServers(
                "server-0",
                ServerAssignmentData.newBuilder()
                    .addContainers(1L)
                    .addContainers(2L)
                    .build())
            .build();

        @Cleanup("shutdown")
        ExecutorService executor = Executors.newSingleThreadExecutor();

        CompletableFuture<Void> watchFuture = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(2);

        Consumer<Void> dataConsumer = ignored -> {
            log.info("Notify cluster assignment data changed");
            latch.countDown();
            FutureUtils.complete(watchFuture, null);
        };

        assertEquals(0, store.getNumWatchers());
        store.watchClusterAssignmentData(dataConsumer, executor);
        assertEquals(1, store.getNumWatchers());
        store.updateClusterAssignmentData(assignmentData);

        watchFuture.join();
        assertEquals(1, latch.getCount());
        assertEquals(assignmentData, store.getClusterAssignmentData());

        store.unwatchClusterAssignmentData(dataConsumer);
        assertEquals(0, store.getNumWatchers());
    }
}
