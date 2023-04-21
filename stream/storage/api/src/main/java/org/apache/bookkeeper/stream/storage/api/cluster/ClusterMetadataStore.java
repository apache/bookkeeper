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
package org.apache.bookkeeper.stream.storage.api.cluster;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;

/**
 * Store the cluster related metadata, such as the number of storage containers, the mapping between servers
 * to storage containers.
 */
public interface ClusterMetadataStore extends AutoCloseable {


    default boolean initializeCluster(int numStorageContainers) {
        return initializeCluster(numStorageContainers, Optional.empty());
    }

    /**
     * Initialize the cluster metadata with the provided <i>numStorageContainers</i>.
     *
     * @param numStorageContainers number of storage containers.
     * @param segmentStorePath segment store path
     * @return true if successfully initialized cluster, otherwise false.
     */
    boolean initializeCluster(int numStorageContainers, Optional<String> segmentStorePath);

    /**
     * Get the current cluster assignment data.
     *
     * @return the cluster assignment data.
     */
    ClusterAssignmentData getClusterAssignmentData();

    /**
     * Update the current cluster assignment data.
     *
     * @param assignmentData cluster assignment data
     */
    void updateClusterAssignmentData(ClusterAssignmentData assignmentData);

    /**
     * Watch the current cluster assignment data.
     *
     * @param watcher current cluster assignment data watcher
     * @param executor the executor to run the <tt>watcher</tt>
     */
    void watchClusterAssignmentData(Consumer<Void> watcher, Executor executor);

    /**
     * Unwatch the current cluster assignment data.
     *
     * @param watcher current cluster assignment data watcher to remove
     */
    void unwatchClusterAssignmentData(Consumer<Void> watcher);

    /**
     * Returns the cluster metadata presents in the system.
     *
     * @return cluster metadata.
     */
    ClusterMetadata getClusterMetadata();

    /**
     * Update the current cluster metadata.
     *
     * @param clusterMetadata cluster metadata to update.
     */
    void updateClusterMetadata(ClusterMetadata clusterMetadata);

    /**
     * Close the metadata store.
     */
    @Override
    void close();

}
