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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeader;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

/**
 * This is the controller leader which watches the servers in the cluster and handles server level failures.
 *
 * <p>The controller leader is responsible for making sure all storage containers are assigned to servers, and load
 * balancing storage containers if necessary.
 */
@Slf4j
class ZkClusterControllerLeaderSelectorListener implements LeaderSelectorListener {

    private final ClusterControllerLeader controller;


    ZkClusterControllerLeaderSelectorListener(ClusterControllerLeader controller) {
        this.controller = controller;
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        controller.processAsLeader();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        log.info("zookeeper connection state changed to {} for cluster controller", newState);
    }

}
