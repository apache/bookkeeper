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

package org.apache.bookkeeper.metadata.etcd.testing;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.model.Frame;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.LogUtils;

/**
 * Etcd test container.
 */
@Slf4j
public class EtcdContainer extends GenericContainer<EtcdContainer> {

    static class LogContainerResultCb extends ResultCallback.Adapter<Frame> {
        @Override
        public void onNext(Frame frame) {
            log.info(new String(frame.getPayload(), UTF_8));
        }
    }

    public static final String NAME = "etcd";
    public static final int CLIENT_PORT = 2379;

    private final String clusterName;

    public EtcdContainer(String clusterName) {
        super("quay.io/coreos/etcd:v3.5.14");
        this.clusterName = clusterName;
    }

    public String getExternalServiceUri() {
        return "etcd://" + getHost() + ":" + getEtcdClientPort() + "/clusters/" + clusterName;
    }

    public String getInternalServiceUri() {
        return "etcd://" + NAME + ":" + CLIENT_PORT + "/clusters/" + clusterName;
    }

    @Override
    protected void configure() {
        super.configure();

        String[] command = new String[] {
            "/usr/local/bin/etcd",
            "--name", NAME + "0",
            "--initial-advertise-peer-urls", "http://" + NAME + ":2380",
            "--listen-peer-urls", "http://0.0.0.0:2380",
            "--advertise-client-urls", "http://" + NAME + ":2379",
            "--listen-client-urls", "http://0.0.0.0:2379",
            "--initial-cluster", NAME + "0=http://" + NAME + ":2380"
        };

        this.withNetworkAliases(NAME)
            .withExposedPorts(CLIENT_PORT)
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(clusterName + "-" + NAME);
            })
            .withCommand(command)
            .withNetworkAliases(NAME)
            .waitingFor(waitStrategy());
        tailContainerLog();
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

    public int getEtcdClientPort() {
        return getMappedPort(CLIENT_PORT);
    }

    public String getClientEndpoint() {
        return String.format("http://%s:%d", getHost(), getEtcdClientPort());
    }

    private WaitStrategy waitStrategy() {
        return new org.testcontainers.containers.wait.strategy.AbstractWaitStrategy() {
            @Override
            protected void waitUntilReady() {
                final DockerClient client = DockerClientFactory.instance().client();
                final WaitingConsumer waitingConsumer = new WaitingConsumer();

                LogUtils.followOutput(client, waitStrategyTarget.getContainerId(), waitingConsumer);

                try {
                    waitingConsumer.waitUntil(
                        f -> f.getUtf8String().contains("ready to serve client requests"),
                        startupTimeout.getSeconds(),
                        TimeUnit.SECONDS,
                        1
                    );
                } catch (TimeoutException e) {
                    throw new ContainerLaunchException("Timed out");
                }
            }
        };
    }


}
