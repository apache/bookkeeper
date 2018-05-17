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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/**
 * Test Container for Bookies.
 */
@Slf4j
public class BKContainer<SELF extends BKContainer<SELF>> extends GenericContainer<SELF> {

    private static final int ZK_PORT = 2181;
    private static final int BOOKIE_BASE_PORT = 3181;

    private static final String IMAGE_NAME = "apachebookkeeper/bookkeeper-current:latest";

    private static final String STANDALONE_HOST_NAME = "standalone";
    private static final String CONTAINER_NAME_BASE = "bk-standalone-test";

    private final String containerName;
    private final int numBookies;

    public BKContainer(String containerName, int numBookies) {
        super(IMAGE_NAME);
        this.containerName = containerName;
        this.numBookies = numBookies;
    }

    @Override
    public String getContainerName() {
        return CONTAINER_NAME_BASE + "-" + containerName + "-" + numBookies + "-bookies-" + System.currentTimeMillis();
    }

    public String getContainerLog() {
        StringBuilder sb = new StringBuilder();

        LogContainerCmd logContainerCmd = this.dockerClient.logContainerCmd(containerId);
        logContainerCmd.withStdOut(true).withStdErr(true);
        try {
            logContainerCmd.exec(new LogContainerResultCallback() {
                @Override
                public void onNext(Frame item) {
                    sb.append(new String(item.getPayload(), UTF_8));
                }
            }).awaitCompletion();
        } catch (InterruptedException e) {

        }
        return sb.toString();
    }

    public ExecResult execCmd(String... cmd) throws Exception {
        String cmdString = StringUtils.join(cmd, " ");

        log.info("DOCKER.exec({}:{}): Executing ...", containerId, cmdString);

        ExecResult result = execInContainer(cmd);

        log.info("Docker.exec({}:{}): Done", containerId, cmdString);
        log.info("Docker.exec({}:{}): Stdout -\n{}", containerId, cmdString, result.getStdout());
        log.info("Docker.exec({}:{}): Stderr -\n{}", containerId, cmdString, result.getStderr());

        return result;
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

}
