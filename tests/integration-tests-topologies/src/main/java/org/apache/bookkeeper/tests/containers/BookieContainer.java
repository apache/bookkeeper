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

import com.google.common.base.Strings;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.containers.wait.HttpWaitStrategy;
import org.apache.bookkeeper.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

/**
 * Test Container for Bookies.
 */
@Slf4j
public class BookieContainer<SelfT extends BookieContainer<SelfT>> extends ChaosContainer<SelfT> {

    private static final int BOOKIE_PORT = 3181;
    private static final int BOOKIE_GRPC_PORT = 4181; // stream storage grpc port
    private static final int BOOKIE_HTTP_PORT = 8080;

    private static final String IMAGE_NAME = "apachebookkeeper/bookkeeper-current:latest";

    private final String hostname;
    private final String metadataServiceUri;
    private final String extraServerComponents;

    public BookieContainer(String clusterName,
                           String hostname,
                           String metadataServiceUri,
                           String extraServerComponents) {
        super(clusterName, IMAGE_NAME);
        this.hostname = hostname;
        this.metadataServiceUri = metadataServiceUri;
        this.extraServerComponents = extraServerComponents;
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + hostname;
    }

    public String getExternalGrpcEndpointStr() {
        return getContainerIpAddress() + ":" + getMappedPort(BOOKIE_GRPC_PORT);
    }

    public String getInternalGrpcEndpointStr() {
        return DockerUtils.getContainerIP(dockerClient, containerId) + ":" + BOOKIE_GRPC_PORT;
    }

    @Override
    protected void configure() {
        addExposedPorts(
            BOOKIE_PORT,
            BOOKIE_GRPC_PORT,
            BOOKIE_HTTP_PORT
        );
        addEnv("BK_httpServerEnabled", "true");
        addEnv("BK_httpServerPort", "" + BOOKIE_HTTP_PORT);
        addEnv("BK_metadataServiceUri", metadataServiceUri);
        addEnv("BK_useHostNameAsBookieID", "true");
        addEnv("BK_extraServerComponents", extraServerComponents);
        if (metadataServiceUri.toLowerCase().startsWith("zk")) {
            URI uri = URI.create(metadataServiceUri);
            addEnv("BK_zkServers", uri.getAuthority());
            addEnv("BK_zkLedgersRootPath", uri.getPath());
        }
        // grpc port
        addEnv("BK_storageserver.grpc.port", "" + BOOKIE_GRPC_PORT);
    }

    @Override
    public void start() {
        if (Strings.isNullOrEmpty(extraServerComponents)) {
            this.waitStrategy = new HttpWaitStrategy()
                .forPath("/heartbeat")
                .forStatusCode(200)
                .forPort(BOOKIE_HTTP_PORT)
                .withStartupTimeout(Duration.of(60, SECONDS));
        } else {
            this.waitStrategy = new HostPortWaitStrategy()
                .withStartupTimeout(Duration.of(300, SECONDS));
        }
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName(hostname);
            createContainerCmd.withName(getContainerName());
        });

        super.start();
        log.info("Started bookie {} at cluster {}", hostname, clusterName);
    }

    @Override
    protected void beforeStop() {
        super.beforeStop();
        if (null != containerId) {
            DockerUtils.dumpContainerDirToTargetCompressed(
                getDockerClient(),
                getContainerName(),
                "/opt/bookkeeper/logs"
            );
        }
    }

    @Override
    public void stop() {
        super.stop();
        log.info("Stopped bookie {} at cluster {}", hostname, clusterName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BookieContainer)) {
            return false;
        }

        BookieContainer another = (BookieContainer) o;
        return hostname.equals(another.hostname)
            && metadataServiceUri.equals(another.metadataServiceUri)
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(
            hostname,
            metadataServiceUri);
    }

}
