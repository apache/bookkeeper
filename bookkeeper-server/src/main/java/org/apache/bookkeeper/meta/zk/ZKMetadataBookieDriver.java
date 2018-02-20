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
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * ZooKeeper based metadata bookie driver.
 */
@Slf4j
public class ZKMetadataBookieDriver
    extends ZKMetadataDriverBase
    implements MetadataBookieDriver {

    // register myself
    static {
        MetadataDrivers.registerBookieDriver(
            SCHEME,
            ZKMetadataBookieDriver.class);
    }

    ServerConfiguration serverConf;
    RegistrationManager regManager;
    RegistrationListener listener;

    @Override
    public synchronized MetadataBookieDriver initialize(ServerConfiguration conf,
                                                        RegistrationListener listener,
                                                        StatsLogger statsLogger)
            throws MetadataException {
        super.initialize(
            conf, statsLogger, Optional.empty());
        this.statsLogger = statsLogger;
        this.serverConf = conf;
        this.listener = listener;
        return this;
    }

    @Override
    public synchronized RegistrationManager getRegistrationManager()
            throws MetadataException {
        if (null == regManager) {
            regManager = new ZKRegistrationManager();
            try {
                regManager.initialize(
                    serverConf,
                    listener,
                    statsLogger);
            } catch (BookieException e) {
                throw new MetadataException(
                    Code.METADATA_SERVICE_ERROR,
                    "Failed to initialize registration manager",
                    e);
            }
        }
        return regManager;
    }

    @Override
    public void close() {
        if (null != regManager) {
            regManager.close();
            regManager = null;
        }
        super.close();
    }
}
