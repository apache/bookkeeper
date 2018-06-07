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

package org.apache.bookkeeper.tests.containers.wait;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.rnorth.ducttape.TimeoutException;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

/**
 * Wait Strategy until zookeeper container is up.
 */
@Slf4j
public class ZKWaitStrategy extends AbstractWaitStrategy {

    private final int zkPort;

    public ZKWaitStrategy(int zkPort) {
        this.zkPort = zkPort;
    }

    @Override
    protected void waitUntilReady() {
        String hostname = waitStrategyTarget.getContainerIpAddress();
        int externalPort = waitStrategyTarget.getMappedPort(zkPort);

        try {
            Unreliables.retryUntilTrue(
                (int) startupTimeout.getSeconds(),
                TimeUnit.SECONDS,
                () -> getRateLimiter().getWhenReady(
                    () -> {
                        log.info("Check if zookeeper is running at {}:{}", hostname, externalPort);
                        return BookKeeperClusterUtils.zookeeperRunning(
                            hostname, externalPort
                        );
                    }));
        } catch (TimeoutException te) {
            throw new ContainerLaunchException(
                "Timed out waiting for zookeeper to be ready");
        }
    }

}
