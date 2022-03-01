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

import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Unit test of {@link DiscoveryCommand}.
 */
public class DiscoveryCommandTest {

    private DiscoveryCommand<CliFlags> cmd;
    private ServerConfiguration serverConf;
    private ClientConfiguration clientConf;
    private RegistrationClient regClient;
    private MetadataClientDriver clientDriver;
    private ScheduledExecutorService executor;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {

        this.cmd = mock(DiscoveryCommand.class, CALLS_REAL_METHODS);

        this.serverConf = new ServerConfiguration();
        this.serverConf.setMetadataServiceUri("zk://127.0.0.1/path/to/ledgers");
        this.executor = mock(ScheduledExecutorService.class);
        this.regClient = mock(RegistrationClient.class);
        this.clientDriver = mock(MetadataClientDriver.class);
        when(clientDriver.getRegistrationClient())
            .thenReturn(regClient);
    }

    @Test
    public void testRun() throws Exception {
        try (final MockedStatic<Executors> executorsMockedStatic = Mockito.mockStatic(Executors.class);
             final MockedStatic<MetadataDrivers> mdriversMockedStatic = Mockito.mockStatic(MetadataDrivers.class);) {
            executorsMockedStatic
                    .when(() -> Executors.newSingleThreadScheduledExecutor()).thenReturn(executor);
            mdriversMockedStatic.when(() -> MetadataDrivers.getClientDriver(any(URI.class)))
                    .thenReturn(clientDriver);

            CliFlags cliFlags = new CliFlags();
            assertTrue(cmd.apply(serverConf, cliFlags));
            verify(cmd, times(1)).run(eq(regClient), same(cliFlags));
            verify(clientDriver, times(1))
                .initialize(
                        any(ClientConfiguration.class), eq(executor),
                        eq(NullStatsLogger.INSTANCE), eq(Optional.empty()));
            verify(clientDriver, times(1)).getRegistrationClient();
            verify(clientDriver, times(1)).close();
            verify(executor, times(1)).shutdown();
        }
    }


}
