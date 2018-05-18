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

package org.apache.bookkeeper.tests.containers;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/**
 * Test Container for Bookies.
 */
@Slf4j
public class BKStandaloneContainer<SELF extends BKStandaloneContainer<SELF>> extends ChaosContainer<SELF> {

    private static final int ZK_PORT = 2181;
    private static final int BOOKIE_BASE_PORT = 3181;

    private static final String IMAGE_NAME = "apachebookkeeper/bookkeeper-current:latest";

    private static final String STANDALONE_HOST_NAME = "standalone";

    private final int numBookies;

    public BKStandaloneContainer(String clusterName, int numBookies) {
        super(clusterName, IMAGE_NAME);
        this.numBookies = numBookies;
    }

    @Override
    public String getContainerName() {
        return clusterName + "-standalone-" + numBookies + "-bookies";
    }

    @Override
    protected void configure() {
        addExposedPorts(
            ZK_PORT
        );
        for (int i = 0; i < numBookies; i++) {
            addExposedPort(BOOKIE_BASE_PORT + i);
        }
        setCommand(
            "/opt/bookkeeper/bin/bookkeeper",
            "localbookie",
            "" + numBookies
        );
        addEnv("JAVA_HOME", "/usr/lib/jvm/jre-1.8.0");
    }

    @Override
    public void start() {
        this.waitStrategy = new LogMessageWaitStrategy()
            .withRegEx(".*ForceWrite Thread started.*\\s")
            .withTimes(numBookies)
            .withStartupTimeout(Duration.of(60, SECONDS));
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName(STANDALONE_HOST_NAME);
            createContainerCmd.withName(getContainerName());
            createContainerCmd.withEntrypoint("/bin/bash");
        });

        super.start();
        log.info("Start a standalone bookkeeper cluster at container {}", containerName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BKStandaloneContainer)) {
            return false;
        }

        BKStandaloneContainer another = (BKStandaloneContainer) o;
        return containerName.equals(another.containerName)
            && numBookies == another.numBookies
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(
            containerName,
            numBookies);
    }
}
