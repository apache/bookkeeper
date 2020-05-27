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
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieStateManager;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.discover.ZKRegistrationManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;

/**
 * A service that manages the registration state for a given server.
 *
 * <p>It registers the server to registration service and handle the state transition.
 */
@Slf4j
public class RegistrationStateService
    extends AbstractLifecycleComponent<BookieConfiguration> {

    private final Endpoint myEndpoint;
    private final ServerConfiguration bkServerConf;
    private final RegistrationServiceProvider regServiceProvider;
    private RegistrationManager regManager;
    private BookieStateManager stateManager;

    public RegistrationStateService(Endpoint myEndpoint,
                                    ServerConfiguration bkServerConf,
                                    BookieConfiguration bookieConf,
                                    RegistrationServiceProvider serviceProvider,
                                    StatsLogger statsLogger) {
        super("registration-state-service", bookieConf, statsLogger);
        this.myEndpoint = myEndpoint;
        this.bkServerConf = bkServerConf;
        this.regServiceProvider = serviceProvider;
    }

    @Override
    protected void doStart() {
        if (null == regManager) {
            regManager = new ZKRegistrationManager(
                bkServerConf,
                regServiceProvider.getZkClient(),
                regServiceProvider.getRegistrationPath(),
                () -> {
                    if (null == stateManager) {
                        log.warn("Registration state manager is not initialized yet");
                        return;
                    }
                    stateManager.forceToUnregistered();
                    // schedule a re-register operation
                    stateManager.registerBookie(false);
                });
            try {
                stateManager = new BookieStateManager(
                    bkServerConf,
                    statsLogger.scope("state"),
                    () -> regManager,
                    Collections.emptyList(),
                    () -> NetUtils.endpointToString(myEndpoint),
                    BookieServiceInfo.NO_INFO);
                stateManager.initState();
                stateManager.registerBookie(true).get();
                log.info("Successfully register myself under registration path {}/{}",
                    regServiceProvider.getRegistrationPath(), NetUtils.endpointToString(myEndpoint));
            } catch (Exception e) {
                throw new RuntimeException("Failed to intiailize a registration state service", e);
            }
        }
    }

    @Override
    protected void doStop() {
        stateManager.forceToShuttingDown();

        // turn the server to readonly during shutting down process

        stateManager.forceToReadOnly();
    }

    @Override
    protected void doClose() throws IOException {
        stateManager.close();
        regManager.close();
    }
}
