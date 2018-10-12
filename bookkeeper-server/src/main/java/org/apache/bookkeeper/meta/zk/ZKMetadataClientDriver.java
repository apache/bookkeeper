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
package org.apache.bookkeeper.meta.zk;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.ZKRegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * ZooKeeper based metadata client driver.
 */
@Slf4j
public class ZKMetadataClientDriver
    extends ZKMetadataDriverBase
    implements MetadataClientDriver {

    // register myself to driver manager
    static {
        MetadataDrivers.registerClientDriver(
            SCHEME, ZKMetadataClientDriver.class);
    }

    ClientConfiguration clientConf;
    ScheduledExecutorService scheduler;
    RegistrationClient regClient;

    @Override
    public synchronized MetadataClientDriver initialize(ClientConfiguration conf,
                                                        ScheduledExecutorService scheduler,
                                                        StatsLogger statsLogger,

                                                        Optional<Object> optionalCtx)
            throws MetadataException {
        super.initialize(
            conf,
            statsLogger,
            new BoundExponentialBackoffRetryPolicy(
                conf.getZkTimeout(),
                conf.getZkTimeout(),
                0),
            optionalCtx);
        this.statsLogger = statsLogger;
        this.clientConf = conf;
        this.scheduler = scheduler;
        return this;
    }

    @Override
    public synchronized RegistrationClient getRegistrationClient() {
        if (null == regClient) {
            regClient = new ZKRegistrationClient(
                zk,
                ledgersRootPath,
                scheduler);
        }
        return regClient;
    }

    @Override
    public synchronized void close() {
        if (null != regClient) {
            regClient.close();
            regClient = null;
        }
        super.close();
    }

    @Override
    public void setSessionStateListener(SessionStateListener sessionStateListener) {
        zk.register((event) -> {
            // Check for expired connection.
            if (event.getType().equals(EventType.None) && event.getState().equals(KeeperState.Expired)) {
                sessionStateListener.onSessionExpired();
            }
        });
    }
}
