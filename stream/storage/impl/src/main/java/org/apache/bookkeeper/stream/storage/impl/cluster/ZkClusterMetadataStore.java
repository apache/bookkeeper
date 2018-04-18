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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssigmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.exceptions.StorageRuntimeException;
import org.apache.curator.framework.CuratorFramework;

/**
 * A zookeeper based implementation of cluster metadata store.
 */
class ZkClusterMetadataStore implements ClusterMetadataStore {

    private static final String METADATA = "metadata";
    private static final String ASSIGNMENT = "assignment";

    private final CuratorFramework client;

    private final String zkRootPath;
    private final String zkClusterMetadataPath;
    private final String zkClusterAssignmentPath;

    ZkClusterMetadataStore(CuratorFramework client, String zkRootPath) {
        this.client = client;
        this.zkRootPath = zkRootPath;
        this.zkClusterMetadataPath = zkRootPath + "/" + METADATA;
        this.zkClusterAssignmentPath = zkRootPath + "/" + ASSIGNMENT;
    }

    @Override
    public void initializeCluster(int numStorageContainers) {
        ClusterMetadata metadata = ClusterMetadata.newBuilder()
            .setNumStorageContainers(numStorageContainers)
            .build();
        ClusterAssigmentData assigmentData = ClusterAssigmentData.newBuilder()
            .build();
        try {
            client.transaction()
                .forOperations(
                    client.transactionOp().create().forPath(zkRootPath),
                    client.transactionOp().create().forPath(zkClusterMetadataPath, metadata.toByteArray()),
                    client.transactionOp().create().forPath(zkClusterAssignmentPath, assigmentData.toByteArray()));
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to initialize storage cluster with "
                + numStorageContainers + " storage containers", e);
        }
    }

    @Override
    public ClusterAssigmentData getClusterAssignmentData() {
        try {
            byte[] data = client.getData().forPath(zkClusterAssignmentPath);
            return ClusterAssigmentData.parseFrom(data);
        } catch (InvalidProtocolBufferException ie) {
            throw new StorageRuntimeException("The cluster assignment data from zookeeper @"
                + zkClusterAssignmentPath + " is corrupted", ie);
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to fetch cluster assignment data from zookeeper @"
                + zkClusterAssignmentPath, e);
        }
    }

    @Override
    public void updateClusterAssignmentData(ClusterAssigmentData assigmentData) {
        byte[] data = assigmentData.toByteArray();
        try {
            client.setData().forPath(zkClusterAssignmentPath, data);
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to update cluster assignment data to zookeeper @"
                + zkClusterAssignmentPath, e);
        }
    }

    @Override
    public ClusterMetadata getClusterMetadata() {
        try {
            byte[] data = client.getData().forPath(zkClusterMetadataPath);
            return ClusterMetadata.parseFrom(data);
        } catch (InvalidProtocolBufferException ie) {
            throw new StorageRuntimeException("The cluster metadata from zookeeper @"
                + zkClusterMetadataPath + " is corrupted", ie);
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to fetch cluster metadata from zookeeper @"
                + zkClusterMetadataPath, e);
        }
    }

    @Override
    public void updateClusterMetadata(ClusterMetadata metadata) {
        byte[] data = metadata.toByteArray();
        try {
            client.setData().forPath(zkClusterMetadataPath, data);
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to update cluster metadata to zookeeper @"
                + zkClusterMetadataPath, e);
        }
    }
}
