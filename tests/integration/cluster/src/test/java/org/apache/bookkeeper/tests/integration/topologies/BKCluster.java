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

package org.apache.bookkeeper.tests.integration.topologies;

import com.google.common.util.concurrent.UncheckedExecutionException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.tests.containers.MetadataStoreContainer;
import org.apache.bookkeeper.tests.containers.ZKContainer;
import org.testcontainers.containers.Network;

/**
 * BookKeeper Cluster in containers.
 */
@Slf4j
public class BKCluster {

    private final String clusterName;
    private final Network network;
    private final MetadataStoreContainer metadataContainer;

    public BKCluster(String clusterName) {
        this.clusterName = clusterName;
        this.network = Network.newNetwork();
        this.metadataContainer = new ZKContainer();
    }

    public String getExternalServiceUri() {
        return metadataContainer.getExternalServiceUri();
    }

    public String getInternalServiceUri() {
        return metadataContainer.getInternalServiceUri();
    }

    public void start() throws Exception {
        // start the metadata store
        this.metadataContainer.start();

        // init a new cluster
        initNewCluster(metadataContainer.getExternalServiceUri());
    }

    public void stop() {
        this.metadataContainer.stop();
        try {
            this.network.close();
        } catch (Exception e) {
            log.info("Failed to shutdown network for bookkeeper cluster {}", clusterName, e);
        }
    }

    protected void initNewCluster(String metadataServiceUri) throws Exception {
        MetadataDrivers.runFunctionWithRegistrationManager(
            new ServerConfiguration().setMetadataServiceUri(metadataServiceUri),
            rm -> {
                try {
                    rm.initNewCluster();
                } catch (Exception e) {
                    throw new UncheckedExecutionException("Failed to init a new cluster", e);
                }
                return null;
            }
        );
    }

}
