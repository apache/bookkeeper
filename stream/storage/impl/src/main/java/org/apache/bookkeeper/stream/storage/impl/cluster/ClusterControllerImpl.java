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

import java.time.Duration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterController;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeader;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeaderSelector;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerController;

/**
 * A service that elects a cluster controller leader for performing cluster actions,
 * such as assigning containers to servers.
 */
public class ClusterControllerImpl implements ClusterController {

    private final ClusterControllerLeaderSelector controllerLeaderSelector;

    public ClusterControllerImpl(ClusterMetadataStore clusterMetadataStore,
                                 RegistrationClient registrationClient,
                                 StorageContainerController scController,
                                 ClusterControllerLeaderSelector clusterControllerLeaderSelector,
                                 StorageConfiguration conf) {
        ClusterControllerLeader controllerLeader = new ClusterControllerLeaderImpl(
            clusterMetadataStore,
            scController,
            registrationClient,
            Duration.ofMillis(conf.getClusterControllerScheduleIntervalMs()));

        this.controllerLeaderSelector = clusterControllerLeaderSelector;
        this.controllerLeaderSelector.initialize(controllerLeader);
    }


    @Override
    public void start() {
        this.controllerLeaderSelector.start();
    }

    @Override
    public void stop() {
        this.controllerLeaderSelector.close();
    }

}
