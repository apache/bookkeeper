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
package org.apache.bookkeeper.stream.server;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.common.component.AutoCloseableLifecycleComponent;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.server.service.HttpService;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterInitializer;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.server.component.ServerLifecycleComponent.loadServerComponents;

@Slf4j
public class StateStoreMain extends Main {
    public static void main(String[] args) {
        int retCode = doMain(args);
        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) {
        ServerConfiguration conf;

        // 0. parse command line
        try {
            conf = parseCommandLine(args);
        } catch (IllegalArgumentException iae) {
            return org.apache.bookkeeper.bookie.ExitCode.INVALID_CONF;
        }

        Map<String, Object> confMap = new TreeMap<>();
        conf.getKeys().forEachRemaining(k -> confMap.put(k, conf.getProperty(k)));
        log.info("Starting State store service with config: {}", confMap);

        // 1. building the component stack:
        LifecycleComponent server;
        try {
            server = buildStateStoreServer(new BookieConfiguration(conf));
        } catch (Exception e) {
            log.error("Failed to build state store server", e);
            return org.apache.bookkeeper.bookie.ExitCode.SERVER_EXCEPTION;
        }

        // 2. start the server
        try {
            ComponentStarter.startComponent(server).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // the server is interrupted
            log.info("State store server is interrupted. Exiting ...");
        } catch (ExecutionException ee) {
            log.error("Error in state store shutdown", ee.getCause());
            return org.apache.bookkeeper.bookie.ExitCode.SERVER_EXCEPTION;
        }
        return ExitCode.OK;
    }

    /**
     * Build the State store server.
     *
     * <p>The sequence of the components is:
     *
     * <pre>
     * - stats provider
     * - state store server
     * - http service
     * </pre>
     *
     * @param conf bookie server configuration
     * @return lifecycle stack
     */
    public static LifecycleComponentStack buildStateStoreServer(BookieConfiguration conf) throws Exception {
        LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack.newBuilder().withName("state-store-server");

        // 1. build stats provider
        StatsProviderService statsProviderService =
                new StatsProviderService(conf);
        StatsLogger rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");

        serverBuilder.addComponent(statsProviderService);
        log.info("Load lifecycle component : {}", StatsProviderService.class.getName());

        // Build metadata driver
        MetadataBookieDriver metadataDriver = BookieResources.createMetadataDriver(
                conf.getServerConf(), rootStatsLogger);
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("metadataDriver", metadataDriver));
        RegistrationManager rm = metadataDriver.createRegistrationManager();
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("registrationManager", rm));

        ByteBufAllocatorWithOomHandler allocator = BookieResources.createAllocator(conf.getServerConf());

        LedgerManagerFactory lmFactory = metadataDriver.getLedgerManagerFactory();
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("lmFactory", lmFactory));
        LedgerManager ledgerManager = lmFactory.newLedgerManager();
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("ledgerManager", ledgerManager));

        StatsLogger bookieStats = rootStatsLogger.scope(BOOKIE_SCOPE);

        // 4. build http service
        if (conf.getServerConf().isHttpServerEnabled()) {
            BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                    .setBookieServer(null)
                    .setServerConfiguration(conf.getServerConf())
                    .setStatsProvider(statsProviderService.getStatsProvider())
                    .setLedgerManagerFactory(metadataDriver.getLedgerManagerFactory())
                    .build();
            HttpService httpService =
                    new HttpService(provider, conf, rootStatsLogger);

            serverBuilder.addComponent(httpService);
            log.info("Load lifecycle component : {}", HttpService.class.getName());
        }

        // 5. build extra services
        String[] extraComponents = new String[]{"org.apache.bookkeeper.stream.server.StreamStorageLifecycleComponent"};
        try {
            List<ServerLifecycleComponent> components = loadServerComponents(
                    extraComponents,
                    conf,
                    rootStatsLogger);
            for (ServerLifecycleComponent component : components) {
                serverBuilder.addComponent(component);
                log.info("Load lifecycle component : {}", component.getClass().getName());
            }
        } catch (Exception e) {
            if (conf.getServerConf().getIgnoreExtraServerComponentsStartupFailures()) {
                log.info("Failed to load extra components '{}' - {}. Continuing without those components.",
                        StringUtils.join(extraComponents), e.getMessage());
            } else {
                throw e;
            }
        }

        new ZkClusterInitializer(conf.getServerConf().getZkServers()).initializeCluster(
                ServiceURI.create(conf.getServerConf().getMetadataServiceUri()).getUri(),
                32);

        return serverBuilder.build();
    }
}
