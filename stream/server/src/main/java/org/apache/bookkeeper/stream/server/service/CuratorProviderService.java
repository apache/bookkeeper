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

import java.io.IOException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.DLConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * A service to provide a curator client.
 */
@Slf4j
public class CuratorProviderService
    extends AbstractLifecycleComponent<DLConfiguration>
    implements Supplier<CuratorFramework> {

    private final String zkServers;
    private final RetryPolicy curatorRetryPolicy;
    private final CuratorFramework curatorClient;

    public CuratorProviderService(ServerConfiguration bkServerConf,
                                  DLConfiguration conf,
                                  StatsLogger statsLogger) {
        super("curator-provider", conf, statsLogger);
        this.zkServers = ZKMetadataDriverBase.resolveZkServers(bkServerConf);
        this.curatorRetryPolicy = new ExponentialBackoffRetry(
            bkServerConf.getZkRetryBackoffStartMs(),
            Integer.MAX_VALUE,
            bkServerConf.getZkRetryBackoffMaxMs());
        this.curatorClient = CuratorFrameworkFactory
            .newClient(zkServers, curatorRetryPolicy);
    }

    @Override
    protected void doStart() {
        curatorClient.start();
        log.info("Provided curator clients to zookeeper {}.", zkServers);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        curatorClient.close();
    }

    @Override
    public CuratorFramework get() {
        return curatorClient;
    }
}
