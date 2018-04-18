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

import org.apache.bookkeeper.stream.proto.cluster.ClusterAssigmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;

/**
 * An in-memory implementation of {@link ClusterMetadataStore}.
 */
public class InMemClusterMetadataStore implements ClusterMetadataStore {

    private ClusterMetadata metadata;
    private ClusterAssigmentData assigmentData;

    InMemClusterMetadataStore(int numStorageContainers) {
        initializeCluster(numStorageContainers);
    }

    @Override
    public void initializeCluster(int numStorageContainers) {
        this.metadata = ClusterMetadata.newBuilder()
            .setNumStorageContainers(numStorageContainers)
            .build();
        this.assigmentData = ClusterAssigmentData.newBuilder().build();
    }

    @Override
    public synchronized ClusterAssigmentData getClusterAssignmentData() {
        return assigmentData;
    }

    @Override
    public synchronized void updateClusterAssignmentData(ClusterAssigmentData assigmentData) {
        this.assigmentData = assigmentData;
    }

    @Override
    public synchronized ClusterMetadata getClusterMetadata() {
        return metadata;
    }

    @Override
    public synchronized void updateClusterMetadata(ClusterMetadata clusterMetadata) {
        this.metadata = clusterMetadata;
    }
}
