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
package org.apache.bookkeeper.tools.cli.helpers;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * This is a mixin for commands that talks to discovery service.
 */
@Slf4j
public abstract class DiscoveryCommand<DiscoveryFlagsT extends CliFlags> extends ClientCommand<DiscoveryFlagsT> {

    protected DiscoveryCommand(CliSpec<DiscoveryFlagsT> spec) {
        super(spec);
    }

    @Override
    protected boolean apply(ClientConfiguration clientConf, DiscoveryFlagsT cmdFlags) {
        try {
            URI metadataServiceUri = URI.create(clientConf.getMetadataServiceUri());
            @Cleanup("shutdown") ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            try (MetadataClientDriver driver = MetadataDrivers.getClientDriver(metadataServiceUri)) {
                driver.initialize(
                    clientConf,
                    executor,
                    NullStatsLogger.INSTANCE,
                    Optional.empty());
                run(driver.getRegistrationClient(), cmdFlags);
                return true;
            }
        } catch (Exception e) {
            log.error("Fail to process command '{}'", name(), e);
            return false;
        }
    }

    @Override
    protected void run(BookKeeper bk, DiscoveryFlagsT cmdFlags) throws Exception {
        throw new IllegalStateException("It should never be called.");
    }

    protected abstract void run(RegistrationClient regClient, DiscoveryFlagsT cmdFlags)
        throws Exception;

}
