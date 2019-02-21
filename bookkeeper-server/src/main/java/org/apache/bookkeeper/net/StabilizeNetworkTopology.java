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
package org.apache.bookkeeper.net;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is going to provide a stabilize network topology regarding to flapping zookeeper registration.
 */
public class StabilizeNetworkTopology implements NetworkTopology {

    private static final Logger logger = LoggerFactory.getLogger(StabilizeNetworkTopology.class);

    static class NodeStatus {
        long lastPresentTime;
        boolean tentativeToRemove;

        NodeStatus() {
            this.lastPresentTime = System.currentTimeMillis();
        }

        synchronized boolean isTentativeToRemove() {
            return tentativeToRemove;
        }

        synchronized NodeStatus updateStatus(boolean tentativeToRemove) {
            this.tentativeToRemove = tentativeToRemove;
            if (!this.tentativeToRemove) {
                this.lastPresentTime = System.currentTimeMillis();
            }
            return this;
        }

        synchronized long getLastPresentTime() {
            return this.lastPresentTime;
        }
    }

    protected final NetworkTopologyImpl impl;
    // timer
    protected final HashedWheelTimer timer;
    // statuses
    protected final ConcurrentMap<Node, NodeStatus> nodeStatuses;
    // stabilize period seconds
    protected final long stabilizePeriodMillis;

    private class RemoveNodeTask implements TimerTask {

        private final Node node;

        RemoveNodeTask(Node node) {
            this.node = node;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            NodeStatus status = nodeStatuses.get(node);
            if (null == status) {
                // no status of this node, remove this node from topology
                impl.remove(node);
            } else if (status.isTentativeToRemove()) {
                long millisSinceLastSeen = System.currentTimeMillis() - status.getLastPresentTime();
                if (millisSinceLastSeen >= stabilizePeriodMillis) {
                    logger.info("Node {} (seen @ {}) becomes stale for {} ms, remove it from the topology.",
                            node, status.getLastPresentTime(), millisSinceLastSeen);
                    impl.remove(node);
                    nodeStatuses.remove(node, status);
                }
            }
        }
    }

    public StabilizeNetworkTopology(HashedWheelTimer timer,
                                    int stabilizePeriodSeconds) {
        this.impl = new NetworkTopologyImpl();
        this.timer = timer;
        this.nodeStatuses = new ConcurrentHashMap<Node, NodeStatus>();
        this.stabilizePeriodMillis = TimeUnit.SECONDS.toMillis(stabilizePeriodSeconds);
    }

    void updateNode(Node node, boolean tentativeToRemove) {
        NodeStatus ns = nodeStatuses.get(node);
        if (null == ns) {
            NodeStatus newStatus = new NodeStatus();
            NodeStatus oldStatus = nodeStatuses.putIfAbsent(node, newStatus);
            if (null == oldStatus) {
                ns = newStatus;
            } else {
                ns = oldStatus;
            }
        }
        ns.updateStatus(tentativeToRemove);
    }

    @Override
    public void add(Node node) {
        updateNode(node, false);
        this.impl.add(node);
    }

    @Override
    public void remove(Node node) {
        updateNode(node, true);
        timer.newTimeout(new RemoveNodeTask(node), stabilizePeriodMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean contains(Node node) {
        return impl.contains(node);
    }

    @Override
    public Node getNode(String loc) {
        return impl.getNode(loc);
    }

    @Override
    public int getNumOfRacks() {
        return impl.getNumOfRacks();
    }

    @Override
    public Set<Node> getLeaves(String loc) {
        return impl.getLeaves(loc);
    }

    @Override
    public int countNumOfAvailableNodes(String scope, Collection<Node> excludedNodes) {
        return impl.countNumOfAvailableNodes(scope, excludedNodes);
    }
}
