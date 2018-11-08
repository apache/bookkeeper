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
package org.apache.bookkeeper.metadata.etcd;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Etcd based metadata client driver.
 */
@Slf4j
public class EtcdMetadataClientDriver extends EtcdMetadataDriverBase implements MetadataClientDriver {

    // register myself to driver manager
    static {
        MetadataDrivers.registerClientDriver(
            SCHEME, EtcdMetadataClientDriver.class);
        log.info("Registered etcd metadata client driver.");
    }

    ClientConfiguration conf;
    ScheduledExecutorService scheduler;
    RegistrationClient regClient;

    @Override
    public MetadataClientDriver initialize(ClientConfiguration conf,
                                           ScheduledExecutorService scheduler,
                                           StatsLogger statsLogger,
                                           Optional<Object> ctx)
            throws MetadataException {
        super.initialize(conf, statsLogger);
        this.conf = conf;
        this.statsLogger = statsLogger;
        return this;
    }

    @Override
    public synchronized RegistrationClient getRegistrationClient() {
        if (null == regClient) {
            regClient = new EtcdRegistrationClient(keyPrefix, client);
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
        /*
         * TODO: EtcdMetadataClientDriver has to implement this method.
         */
        throw new UnsupportedOperationException();
    }
}
