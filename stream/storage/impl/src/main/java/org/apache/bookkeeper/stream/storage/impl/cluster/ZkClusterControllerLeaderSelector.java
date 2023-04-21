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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.stream.storage.StorageConstants.getControllerPath;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeader;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeaderSelector;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * A controller leader selector implemented using zookeeper.
 */
@Slf4j
public class ZkClusterControllerLeaderSelector implements ClusterControllerLeaderSelector, ConnectionStateListener {

    private final CuratorFramework client;
    // the zookeeper path that controller is using for leader election
    private final String controllerZkPath;

    // leader selector to select leader
    private ClusterControllerLeader leader;
    private LeaderSelector leaderSelector;

    public ZkClusterControllerLeaderSelector(CuratorFramework client,
                                             String zkRootPath) {
        this.client = client;
        this.controllerZkPath = getControllerPath(zkRootPath);
    }

    @Override
    public void initialize(ClusterControllerLeader leader) {
        this.leader = leader;
        ZkClusterControllerLeaderSelectorListener zkLeader = new ZkClusterControllerLeaderSelectorListener(leader);
        this.leaderSelector = new LeaderSelector(client, controllerZkPath, zkLeader);
        client.getConnectionStateListenable().addListener(this);
    }

    @Override
    public void start() {
        checkNotNull(leaderSelector, "leader selector is not initialized");
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    @Override
    public void close() {
        if (null != leaderSelector) {
            leaderSelector.interruptLeadership();
            leaderSelector.close();
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
            case LOST:
                log.warn("Connection to zookeeper is lost. So interrupt my current leadership.");
                leaderSelector.interruptLeadership();
                break;
            case SUSPENDED:
                if (leaderSelector.hasLeadership()) {
                    log.info("Connection to zookeeper is disconnected, suspend the leader until it is reconnected.");
                    leader.suspend();
                }
                break;
            case RECONNECTED:
                if (leaderSelector.hasLeadership()) {
                    log.info("Connection to zookeeper is reconnected, resume the leader");
                    leader.resume();
                }
                break;
            default:
                break;
        }
    }
}
