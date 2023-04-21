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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.tests.containers.wait.HttpWaitStrategy;

/**
 * Test container that runs zookeeper.
 */
@Slf4j
public class ZKContainer<SelfT extends ZKContainer<SelfT>> extends MetadataStoreContainer<SelfT> {

    private static final int ZK_PORT = 2181;
    private static final int ZK_HTTP_PORT = 8080;

    private static final String IMAGE_NAME = "apachebookkeeper/bookkeeper-current:latest";
    public static final String HOST_NAME = "metadata-store";
    public static final String SERVICE_URI = "zk://" + HOST_NAME + ":" + ZK_PORT + "/ledgers";

    public ZKContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
    }

    @Override
    public String getExternalServiceUri() {
        return "zk://" + getHost() + ":" + getMappedPort(ZK_PORT) + "/ledgers";
    }

    @Override
    public String getInternalServiceUri() {
        return SERVICE_URI;
    }

    @Override
    protected void configure() {
        addExposedPorts(
            ZK_PORT,
            ZK_HTTP_PORT);
        setCommand("zookeeper");
        addEnv("BK_admin.serverPort", "" + ZK_HTTP_PORT);
    }

    @Override
    public void start() {
        this.waitStrategy = new HttpWaitStrategy()
                .forPath("/commands/ruok")
                .forStatusCode(200)
                .forPort(ZK_HTTP_PORT)
                .withStartupTimeout(Duration.of(60, SECONDS));

        this.withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd.withHostName(HOST_NAME));

        super.start();
        log.info("Start a zookeeper server at container {} : external service uri = {}, internal service uri = {}",
            this.getContainerName(), getExternalServiceUri(), getInternalServiceUri());
    }

}
