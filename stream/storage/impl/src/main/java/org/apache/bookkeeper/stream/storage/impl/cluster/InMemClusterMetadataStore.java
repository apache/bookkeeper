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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import lombok.Data;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;

/**
 * An in-memory implementation of {@link ClusterMetadataStore}.
 */
public class InMemClusterMetadataStore implements ClusterMetadataStore {

    @Data
    private static class WatcherAndExecutor {
        private final Consumer<Void> watcher;
        private final Executor executor;
    }

    private final Map<Consumer<Void>, WatcherAndExecutor> watchers;

    private ClusterMetadata metadata;
    private ClusterAssignmentData assignmentData;

    InMemClusterMetadataStore(int numStorageContainers) {
        this.watchers = Maps.newHashMap();
        initializeCluster(numStorageContainers);
    }

    synchronized int getNumWatchers() {
        return watchers.size();
    }

    @Override
    public synchronized boolean initializeCluster(int numStorageContainers,
                                               Optional<String> segmentStorePath) {
        this.metadata = ClusterMetadata.newBuilder()
            .setNumStorageContainers(numStorageContainers)
            .build();
        this.assignmentData = ClusterAssignmentData.newBuilder().build();
        return true;
    }

    @Override
    public synchronized ClusterAssignmentData getClusterAssignmentData() {
        return assignmentData;
    }

    @Override
    public synchronized void updateClusterAssignmentData(ClusterAssignmentData assignmentData) {
        this.assignmentData = assignmentData;
        watchers.values().forEach(wae -> wae.executor.execute(() -> wae.watcher.accept(null)));
    }

    @Override
    public synchronized void watchClusterAssignmentData(Consumer<Void> watcher, Executor executor) {
        WatcherAndExecutor wae = watchers.get(watcher);
        if (null == wae) {
            wae = new WatcherAndExecutor(watcher, executor);
            watchers.put(watcher, wae);
        }
    }

    @Override
    public synchronized void unwatchClusterAssignmentData(Consumer<Void> watcher) {
        watchers.remove(watcher);
    }

    @Override
    public synchronized ClusterMetadata getClusterMetadata() {
        return metadata;
    }

    @Override
    public synchronized void updateClusterMetadata(ClusterMetadata clusterMetadata) {
        this.metadata = clusterMetadata;
    }

    @Override
    public void close() {
        // no-op
    }
}
