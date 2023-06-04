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
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import java.security.Security;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.shaded.org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.testcontainers.utility.LogUtils;
import org.testcontainers.utility.MountableFile;
import javax.net.ssl.SSLException;

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
    @Getter
    private final boolean secure;

    public EtcdContainer(String clusterName, boolean secure) {
        super("quay.io/coreos/etcd:v3.3");
        this.clusterName = clusterName;
        this.secure = secure;
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

        if (secure) {
            withCommand(
                "/usr/local/bin/etcd",
                "--name", NAME + "0",
                "--initial-advertise-peer-urls", "http://" + NAME + ":2380",
                "--listen-peer-urls", "http://0.0.0.0:2380",
                "--advertise-client-urls", "https://" + NAME + ":2379",
                "--listen-client-urls", "https://0.0.0.0:2379",
                "--initial-cluster", NAME + "0=http://" + NAME + ":2380",
                "--client-cert-auth",
                "--trusted-ca-file", "/ca.pem",
                "--cert-file", "/server.pem",
                "--key-file", "/server-key.pem"
            );
        } else {
            withCommand(
                "/usr/local/bin/etcd",
                "--name", NAME + "0",
                "--initial-advertise-peer-urls", "http://" + NAME + ":2380",
                "--listen-peer-urls", "http://0.0.0.0:2380",
                "--advertise-client-urls", "http://" + NAME + ":2379",
                "--listen-client-urls", "http://0.0.0.0:2379",
                "--initial-cluster", NAME + "0=http://" + NAME + ":2380"
            );
        }

        this.withNetworkAliases(NAME)
            .withExposedPorts(CLIENT_PORT)
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(clusterName + "-" + NAME);
            })
            .withNetworkAliases(NAME)
            .waitingFor(waitStrategy());
        if (secure) {
            this.withCopyFileToContainer(MountableFile.forClasspathResource("ssl/cert/ca.pem"), "/ca.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("ssl/cert/server.pem"), "/server.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("ssl/cert/server-key.pem"),
                        "/server-key.pem");
        }
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
        if (secure) {
            return String.format("https://%s:%d", getHost(), getEtcdClientPort());
        } else {
            return String.format("http://%s:%d", getHost(), getEtcdClientPort());
        }
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

    public SslContext getSslContext() throws SSLException {
        if (!secure) {
            return null;
        }
        return GrpcSslContexts.forClient()
            .sslProvider(SslProvider.OPENSSL)
            .trustManager(EtcdContainer.class.getClassLoader().getResourceAsStream("ssl/cert/ca.pem"))
            .keyManager(
                    EtcdContainer.class.getClassLoader().getResourceAsStream("ssl/cert/client.pem"),
                    EtcdContainer.class.getClassLoader().getResourceAsStream("ssl/cert/client-key-pk8.pem")
            ).build();
    }

    public String getAuthority() {
        if (!secure) {
            return null;
        }
        return "etcd-ssl";
    }
}
