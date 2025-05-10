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

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.model.Frame;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.integration.utils.DockerUtils;
import org.apache.commons.lang.StringUtils;
import org.testcontainers.containers.GenericContainer;

/**
 * A base container provides chaos capability.
 */
@Slf4j
public class ChaosContainer<SelfT extends ChaosContainer<SelfT>> extends GenericContainer<SelfT> {

    static class LogContainerResultCb extends ResultCallback.Adapter<Frame> {
        @Override
        public void onNext(Frame frame) {
            log.info(new String(frame.getPayload(), UTF_8));
        }
    }

    protected final String clusterName;

    protected ChaosContainer(String clusterName, String image) {
        super(image);
        this.clusterName = clusterName;
    }

    protected void beforeStop() {
        if (null == this.getContainerId()) {
            return;
        }

        DockerUtils.dumpContainerLogToTarget(
            getDockerClient(),
            getContainerName()
        );
    }

    @Override
    public void stop() {
        beforeStop();
        super.stop();
    }

    public void tailContainerLog() {
        CompletableFuture.runAsync(() -> {
            while (null == this.getContainerId()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }

            LogContainerCmd logContainerCmd = this.dockerClient.logContainerCmd(this.getContainerId());
            logContainerCmd.withStdOut(true).withStdErr(true).withFollowStream(true);
            logContainerCmd.exec(new LogContainerResultCb());
        });
    }

    public ExecResult execCmd(String... cmd) throws Exception {
        String cmdString = StringUtils.join(cmd, " ");

        log.info("DOCKER.exec({}:{}): Executing ...", this.getContainerId(), cmdString);

        ExecResult result = execInContainer(cmd);

        log.info("Docker.exec({}:{}): Done", this.getContainerId(), cmdString);
        log.info("Docker.exec({}:{}): Stdout -\n{}", this.getContainerId(), cmdString, result.getStdout());
        log.info("Docker.exec({}:{}): Stderr -\n{}", this.getContainerId(), cmdString, result.getStderr());

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ChaosContainer)) {
            return false;
        }

        ChaosContainer another = (ChaosContainer) o;
        return clusterName.equals(another.clusterName)
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(
            clusterName);
    }

}
