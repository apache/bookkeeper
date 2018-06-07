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
package org.apache.bookkeeper.stream.server.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.stream.storage.StorageConstants.SERVERS_PATH;
import static org.apache.bookkeeper.stream.storage.StorageConstants.ZK_METADATA_ROOT_PATH;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.DLConfiguration;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;

/**
 * A service that is responsible for registration using bookkeeper registration api.
 */
@Slf4j
public class RegistrationServiceProvider
    extends AbstractLifecycleComponent<DLConfiguration>
    implements Supplier<RegistrationClient> {

    private final String zkServers;
    private final RetryPolicy bkZkRetryPolicy;
    private final String regPath;
    private final ScheduledExecutorService regExecutor;
    private ZooKeeperClient zkClient;
    private RegistrationClient client;

    public RegistrationServiceProvider(ServerConfiguration bkServerConf,
                                       DLConfiguration conf,
                                       StatsLogger statsLogger) {
        super("registration-service-provider", conf, statsLogger);
        this.zkServers = ZKMetadataDriverBase.resolveZkServers(bkServerConf);
        this.regPath = ZK_METADATA_ROOT_PATH + "/" + SERVERS_PATH;
        this.bkZkRetryPolicy = new BoundExponentialBackoffRetryPolicy(
            bkServerConf.getZkRetryBackoffStartMs(),
            bkServerConf.getZkRetryBackoffMaxMs(),
            Integer.MAX_VALUE);
        this.regExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("registration-service-provider-scheduler").build());
    }

    @Override
    public RegistrationClient get() {
        checkNotNull(client, "retrieve registration client before starting registration service");
        return client;
    }

    public ZooKeeperClient getZkClient() {
        return zkClient;
    }

    public String getRegistrationPath() {
        return regPath;
    }

    @SneakyThrows
    @Override
    protected void doStart() {
        if (null == zkClient) {
            try {
                zkClient = ZooKeeperClient.newBuilder()
                    .operationRetryPolicy(bkZkRetryPolicy)
                    .connectString(zkServers)
                    .statsLogger(statsLogger.scope("zk"))
                    .build();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted at creating zookeeper client to {}", zkServers, e);
                throw e;
            } catch (Exception e) {
                log.error("Failed to create zookeeper client to {}", zkServers, e);
                throw e;
            }
            client = new ZKRegistrationClient(zkClient, regPath, regExecutor);
        }
    }



    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        if (null != client) {
            client.close();
        }
        if (null != zkClient) {
            try {
                zkClient.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted at closing zookeeper client to {}", zkServers, e);
            }
        }
        this.regExecutor.shutdown();
    }
}
