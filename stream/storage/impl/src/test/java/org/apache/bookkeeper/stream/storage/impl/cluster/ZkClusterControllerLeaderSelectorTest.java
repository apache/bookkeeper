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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.storage.api.cluster.ClusterControllerLeader;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link ZkClusterControllerLeaderSelector}.
 */
@Slf4j
public class ZkClusterControllerLeaderSelectorTest extends ZooKeeperClusterTestCase {

    @Rule
    public final TestName runtime = new TestName();

    private CuratorFramework curatorClient;
    private String zkRootPath;
    private ZkClusterControllerLeaderSelector selector;

    @Before
    public void setup() throws Exception {
        curatorClient = CuratorFrameworkFactory.newClient(
            zkServers,
            new ExponentialBackoffRetry(200, 10, 5000));
        curatorClient.start();

        zkRootPath = "/" + runtime.getMethodName();
        curatorClient.create().forPath(zkRootPath);

        selector = new ZkClusterControllerLeaderSelector(curatorClient, zkRootPath);
    }

    @After
    public void teardown() {
        if (null != selector) {
            selector.close();
        }
        curatorClient.close();
    }

    @Test(expected = NullPointerException.class)
    public void testStartBeforeInitialize() {
        selector.start();
    }

    @Test
    public void testLeaderElection() throws InterruptedException {
        CountDownLatch leaderLatch = new CountDownLatch(1);
        selector.initialize(new ClusterControllerLeader() {
            @Override
            public void processAsLeader() throws Exception {
                log.info("Become leader");
                leaderLatch.countDown();
                try {
                    TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
                } catch (InterruptedException ie) {
                    log.info("Leadership is interrupted");
                    Thread.currentThread().interrupt();
                }
                log.info("Ended leadership");
            }

            @Override
            public void suspend() {

            }

            @Override
            public void resume() {

            }
        });

        selector.start();
        leaderLatch.await();
        assertTrue("Should successfully become leader", true);

        log.info("Ended test");
    }

    @Test
    public void testStateChangedToLost() throws InterruptedException {
        CountDownLatch leaderLatch = new CountDownLatch(1);
        CountDownLatch interruptedLatch = new CountDownLatch(1);
        selector.initialize(new ClusterControllerLeader() {
            @Override
            public void processAsLeader() throws Exception {
                log.info("Become leader");
                leaderLatch.countDown();
                try {
                    TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
                } catch (InterruptedException ie) {
                    log.info("Leader is interrupted", ie);
                    Thread.currentThread().interrupt();
                    interruptedLatch.countDown();
                }
            }

            @Override
            public void suspend() {
            }

            @Override
            public void resume() {
            }
        });

        selector.start();

        leaderLatch.await();

        assertTrue("Should successfully become leader", true);

        selector.stateChanged(curatorClient, ConnectionState.LOST);

        interruptedLatch.await();

        assertTrue("Leader should be interrupted", true);
    }

    @Test
    public void testStateChangedToSuspendedResumed() throws InterruptedException {
        CountDownLatch leaderLatch = new CountDownLatch(1);
        CountDownLatch suspendedLatch = new CountDownLatch(1);
        CountDownLatch resumeLatch = new CountDownLatch(1);
        selector.initialize(new ClusterControllerLeader() {
            @Override
            public void processAsLeader() throws Exception {
                log.info("Become leader");
                leaderLatch.countDown();
                try {
                    TimeUnit.SECONDS.sleep(Long.MAX_VALUE);
                } catch (InterruptedException ie) {
                    log.info("Leader is interrupted", ie);
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void suspend() {
                suspendedLatch.countDown();
            }

            @Override
            public void resume() {
                resumeLatch.countDown();
            }
        });

        selector.start();
        leaderLatch.await();
        assertTrue("Should successfully become leader", true);

        selector.stateChanged(curatorClient, ConnectionState.SUSPENDED);
        suspendedLatch.await();

        assertTrue("Leader should be suspended", true);

        selector.stateChanged(curatorClient, ConnectionState.RECONNECTED);
        resumeLatch.await();

        assertTrue("Leader should be resumed", true);
    }

}
