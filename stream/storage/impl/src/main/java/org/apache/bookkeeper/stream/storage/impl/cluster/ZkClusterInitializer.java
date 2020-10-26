/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.cluster;

import static org.apache.bookkeeper.stream.storage.StorageConstants.ZK_METADATA_ROOT_PATH;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getSegmentsRootPath;

import com.google.common.base.Strings;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stream.proto.cluster.ClusterMetadata;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterInitializer;
import org.apache.bookkeeper.stream.storage.exceptions.StorageRuntimeException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper Based Cluster Initializer.
 */
@Slf4j
public class ZkClusterInitializer implements ClusterInitializer  {

    private final String zkExternalConnectString;

    public ZkClusterInitializer(String zkServers) {
        this.zkExternalConnectString = zkServers;
    }

    @Override
    public boolean acceptsURI(URI metadataServiceUri) {
        return metadataServiceUri.getScheme().toLowerCase().startsWith("zk");
    }

    @Override
    public boolean initializeCluster(URI metadataServiceUri, int numStorageContainers) {
        String zkInternalConnectString = ZKMetadataDriverBase.getZKServersFromServiceUri(metadataServiceUri);
        // 1) `zkExternalConnectString` are the public endpoints, where the tool can interact with.
        //    It allows the tools running outside of the cluster. It is useful for being used in dockerized environment.
        // 2) `zkInternalConnectString` are the internal endpoints, where the services can interact with.
        //    It is used by dlog to bind a namespace.
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
            zkExternalConnectString,
            new ExponentialBackoffRetry(100, Integer.MAX_VALUE, 10000)
        )) {
            client.start();

            ZkClusterMetadataStore store =
                new ZkClusterMetadataStore(client, zkInternalConnectString, ZK_METADATA_ROOT_PATH);

            ClusterMetadata metadata;
            try {
                metadata = store.getClusterMetadata();
                log.info("Loaded cluster metadata : \n{}", metadata);
                return false;
            } catch (StorageRuntimeException sre) {
                if (sre.getCause() instanceof KeeperException.NoNodeException) {

                    String ledgersPath = metadataServiceUri.getPath();
                    Optional<String> segmentStorePath;
                    if (Strings.isNullOrEmpty(ledgersPath) || "/" == ledgersPath) {
                        segmentStorePath = Optional.empty();
                    } else {
                        segmentStorePath = Optional.of(ledgersPath);
                    }

                    log.info("Initializing the stream cluster with {} storage containers with segment store path {}.",
                            numStorageContainers, segmentStorePath.orElse(getSegmentsRootPath(ZK_METADATA_ROOT_PATH)));

                    boolean initialized = store.initializeCluster(numStorageContainers, segmentStorePath);
                    log.info("Successfully initialized the stream cluster : \n{}", store.getClusterMetadata());
                    return initialized;
                } else {
                    throw sre;
                }
            }
        }

    }
}
