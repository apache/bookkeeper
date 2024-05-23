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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerController;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerController;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

/**
 * Unit test {@link ClusterControllerLeaderImpl}.
 */
@Slf4j
public class ClusterControllerLeaderImplTest {

    private static final int NUM_STORAGE_CONTAINERS = 32;

    private ClusterMetadataStore metadataStore;
    private ClusterControllerLeaderImpl clusterController;
    private ExecutorService leaderExecutor;

    private final Semaphore coordSem = new Semaphore(0);
    private final AtomicReference<RegistrationListener> regListenerRef =
        new AtomicReference<>(null);
    private final CompletableFuture<Void> watchFuture = new CompletableFuture<>();

    @Before
    public void setup() {
        this.metadataStore = spy(new InMemClusterMetadataStore(NUM_STORAGE_CONTAINERS));
        this.metadataStore.initializeCluster(NUM_STORAGE_CONTAINERS);
        // wrap the metadata store with the coord sem with coordinating testing
        ClusterMetadataStore originalStore = metadataStore;
        this.metadataStore = new ClusterMetadataStore() {
            @Override
            public boolean initializeCluster(int numStorageContainers, Optional<String> segmentStorePath) {
                return originalStore.initializeCluster(numStorageContainers);
            }

            @Override
            public ClusterAssignmentData getClusterAssignmentData() {
                return originalStore.getClusterAssignmentData();
            }

            @Override
            public void updateClusterAssignmentData(ClusterAssignmentData assignmentData) {
                originalStore.updateClusterAssignmentData(assignmentData);

                // notify when cluster assignment data is updated.
                coordSem.release();
            }

            @Override
            public void watchClusterAssignmentData(Consumer<Void> watcher, Executor executor) {
                originalStore.watchClusterAssignmentData(watcher, executor);
            }

            @Override
            public void unwatchClusterAssignmentData(Consumer<Void> watcher) {
                originalStore.unwatchClusterAssignmentData(watcher);
            }

            @Override
            public ClusterMetadata getClusterMetadata() {
                return originalStore.getClusterMetadata();
            }

            @Override
            public void updateClusterMetadata(ClusterMetadata clusterMetadata) {
                originalStore.updateClusterMetadata(clusterMetadata);
            }

            @Override
            public void close() {
                originalStore.close();
            }
        };

        StorageContainerController scController = spy(new DefaultStorageContainerController());
        RegistrationClient mockRegClient = mock(RegistrationClient.class);
        when(mockRegClient.watchWritableBookies(any(RegistrationListener.class)))
            .thenAnswer(invocationOnMock -> {
                RegistrationListener listener = invocationOnMock.getArgument(0);
                regListenerRef.set(listener);
                return watchFuture;
            });
        doAnswer(invocationOnMock -> {
            RegistrationListener listener = invocationOnMock.getArgument(0);
            regListenerRef.compareAndSet(listener, null);
            return null;
        }).when(mockRegClient).unwatchWritableBookies(any(RegistrationListener.class));

        this.clusterController = new ClusterControllerLeaderImpl(
            metadataStore,
            scController,
            mockRegClient,
            Duration.ofMillis(10));
        this.leaderExecutor = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardown() {
        if (null != metadataStore) {
            metadataStore.close();
        }
        if (null != leaderExecutor) {
            leaderExecutor.shutdown();
        }
    }

    @Test
    public void testProcessAsLeader() throws Exception {
        clusterController.suspend();
        assertTrue(clusterController.isSuspended());

        // start the leader controller
        leaderExecutor.submit(() -> {
            try {
                clusterController.processAsLeader();
            } catch (Exception e) {
                log.info("Encountered exception when cluster controller processes as a leader", e);
            }
        });

        // resume the controller
        clusterController.resume();
        assertFalse(clusterController.isSuspended());

        // simulate `watchWritableBookies` is done, the listener should be registered
        FutureUtils.complete(watchFuture, null);
        assertNotNull(regListenerRef);

        // once the controller is resumed, it should start processing server change
        // but since there is no servers available, the storage controller will not compute any ideal state
        // for the assignment and `lastSuccessfulAssignmentAt` will remain negative.
        assertFalse(coordSem.tryAcquire(1, TimeUnit.SECONDS));
        assertTrue(clusterController.getLastSuccessfulAssignmentAt() < 0);

        // notify the registration client that a new host is added
        Set<BookieId> cluster = Sets.newSet(BookieId.parse("127.0.0.1:4181"));
        Version version = new LongVersion(0L);

        regListenerRef.get().onBookiesChanged(new Versioned<>(cluster, version));
        // the cluster controller will be notified with cluster change and storage controller will compute
        // the assignment state. cluster metadata store should be used for updating cluster assignment data.
        coordSem.acquire();
        assertTrue(clusterController.getLastSuccessfulAssignmentAt() > 0);
        long lastSuccessfulAssignmentAt = clusterController.getLastSuccessfulAssignmentAt();

        // notify the cluster controller with same cluster, cluster controller should not attempt to update
        // the assignment
        regListenerRef.get().onBookiesChanged(new Versioned<>(cluster, version));
        assertFalse(coordSem.tryAcquire(200, TimeUnit.MILLISECONDS));
        assertEquals(lastSuccessfulAssignmentAt, clusterController.getLastSuccessfulAssignmentAt());

        // multiple hosts added and removed
        cluster.add(BookieId.parse("127.0.0.1:4182"));
        cluster.add(BookieId.parse("127.0.0.1:4183"));
        cluster.add(BookieId.parse("127.0.0.1:4184"));
        cluster.add(BookieId.parse("127.0.0.1:4185"));
        version = new LongVersion(1L);

        regListenerRef.get().onBookiesChanged(new Versioned<>(cluster, version));
        // the cluster controller should update assignment data if cluster is changed
        coordSem.acquire();
        assertTrue(clusterController.getLastSuccessfulAssignmentAt() > lastSuccessfulAssignmentAt);
        lastSuccessfulAssignmentAt = clusterController.getLastSuccessfulAssignmentAt();

        // if cluster information is changed to empty, cluster controller should not be eager to change
        // the assignment.
        regListenerRef.get().onBookiesChanged(new Versioned<>(Collections.emptySet(), new LongVersion(2L)));
        assertFalse(coordSem.tryAcquire(1, TimeUnit.SECONDS));
        assertEquals(lastSuccessfulAssignmentAt, clusterController.getLastSuccessfulAssignmentAt());
    }

}
