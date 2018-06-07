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

import static org.apache.bookkeeper.stream.storage.StorageConstants.getClusterAssignmentPath;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getClusterMetadataPath;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getSegmentsRootPath;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getServersPath;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getStoragePath;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getWritableServersPath;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.proto.cluster.ClusterAssignmentData;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.exceptions.StorageRuntimeException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.zookeeper.KeeperException;

/**
 * A zookeeper based implementation of cluster metadata store.
 */
@Slf4j
public class ZkClusterMetadataStore implements ClusterMetadataStore {

    private final CuratorFramework client;

    private final String zkServers;
    private final String zkRootPath;
    private final String zkClusterMetadataPath;
    private final String zkClusterAssignmentPath;

    private final Map<Consumer<Void>, NodeCacheListener> assignmentDataConsumers;
    private NodeCache assignmentDataCache;

    private volatile boolean closed = false;

    public ZkClusterMetadataStore(CuratorFramework client, String zkServers, String zkRootPath) {
        this.client = client;
        this.zkServers = zkServers;
        this.zkRootPath = zkRootPath;
        this.zkClusterMetadataPath = getClusterMetadataPath(zkRootPath);
        this.zkClusterAssignmentPath = getClusterAssignmentPath(zkRootPath);
        this.assignmentDataConsumers = new HashMap<>();
    }

    synchronized int getNumWatchers() {
        return assignmentDataConsumers.size();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;

            if (null != assignmentDataCache) {
                try {
                    assignmentDataCache.close();
                } catch (IOException e) {
                    log.warn("Failed to close assignment data cache", e);
                }
            }
        }
    }

    @Override
    public boolean initializeCluster(int numStorageContainers, Optional<String> segmentStorePath) {
        ClusterMetadata metadata = ClusterMetadata.newBuilder()
            .setNumStorageContainers(numStorageContainers)
            .build();
        ClusterAssignmentData assigmentData = ClusterAssignmentData.newBuilder()
            .build();
        try {
            // we are using dlog for the storage backend, so we need to initialize the dlog namespace
            BKDLConfig dlogConfig = new BKDLConfig(
                zkServers, segmentStorePath.orElse(getSegmentsRootPath(zkRootPath)));
            DLMetadata dlogMetadata = DLMetadata.create(dlogConfig);

            client.transaction()
                .forOperations(
                    client.transactionOp().create().forPath(zkRootPath),
                    client.transactionOp().create().forPath(zkClusterMetadataPath, metadata.toByteArray()),
                    client.transactionOp().create().forPath(zkClusterAssignmentPath, assigmentData.toByteArray()),
                    client.transactionOp().create().forPath(getServersPath(zkRootPath)),
                    client.transactionOp().create().forPath(getWritableServersPath(zkRootPath)),
                    client.transactionOp().create().forPath(getStoragePath(zkRootPath), dlogMetadata.serialize()));
            return true;
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException) {
                // the cluster already exists.
                log.info("Stream storage cluster is already initialized.");
                return false;
            }
            throw new StorageRuntimeException("Failed to initialize storage cluster with "
                + numStorageContainers + " storage containers", e);
        }
    }

    @Override
    public ClusterAssignmentData getClusterAssignmentData() {
        try {
            byte[] data = client.getData().forPath(zkClusterAssignmentPath);
            return ClusterAssignmentData.parseFrom(data);
        } catch (InvalidProtocolBufferException ie) {
            throw new StorageRuntimeException("The cluster assignment data from zookeeper @"
                + zkClusterAssignmentPath + " is corrupted", ie);
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to fetch cluster assignment data from zookeeper @"
                + zkClusterAssignmentPath, e);
        }
    }

    @Override
    public void updateClusterAssignmentData(ClusterAssignmentData assigmentData) {
        byte[] data = assigmentData.toByteArray();
        try {
            client.setData().forPath(zkClusterAssignmentPath, data);
        } catch (Exception e) {
            throw new StorageRuntimeException("Failed to update cluster assignment data to zookeeper @"
                + zkClusterAssignmentPath, e);
        }
    }

    @Override
    public void watchClusterAssignmentData(Consumer<Void> watcher, Executor executor) {
        synchronized (this) {
            if (assignmentDataCache == null) {
                assignmentDataCache = new NodeCache(client, zkClusterAssignmentPath);
                try {
                    assignmentDataCache.start();
                } catch (Exception e) {
                    throw new StorageRuntimeException("Failed to watch cluster assignment data", e);
                }
            }
            NodeCacheListener listener = assignmentDataConsumers.get(watcher);
            if (null == listener) {
                listener = () -> watcher.accept(null);
                assignmentDataConsumers.put(watcher, listener);
                assignmentDataCache.getListenable().addListener(listener, executor);
            }
        }
    }

    @Override
    public void unwatchClusterAssignmentData(Consumer<Void> watcher) {
        synchronized (this) {
            NodeCacheListener listener = assignmentDataConsumers.remove(watcher);
            if (null != listener && null != assignmentDataCache) {
                assignmentDataCache.getListenable().removeListener(listener);
            }
            if (assignmentDataConsumers.isEmpty() && null != assignmentDataCache) {
                try {
                    assignmentDataCache.close();
                } catch (IOException e) {
                    log.warn("Failed to close assignment data cache when there is no watcher", e);
                }
            }
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
