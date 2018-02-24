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

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link DiscoveryCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DiscoveryCommand.class, ReflectionUtils.class })
public class DiscoveryCommandTest {

    private DiscoveryCommand cmd;
    private ServerConfiguration serverConf;
    private ClientConfiguration clientConf;
    private RegistrationClient regClient;
    private ScheduledExecutorService executor;

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(Executors.class);
        PowerMockito.mockStatic(ReflectionUtils.class);

        this.cmd = mock(DiscoveryCommand.class, CALLS_REAL_METHODS);

        this.serverConf = new ServerConfiguration();
        this.clientConf = new ClientConfiguration(serverConf);
        PowerMockito.whenNew(ClientConfiguration.class)
            .withParameterTypes(AbstractConfiguration.class)
            .withArguments(eq(serverConf))
            .thenReturn(clientConf);

        this.executor = mock(ScheduledExecutorService.class);
        PowerMockito.when(Executors.newSingleThreadScheduledExecutor())
            .thenReturn(executor);

        this.regClient = mock(RegistrationClient.class);
        PowerMockito.when(ReflectionUtils.newInstance(any()))
            .thenReturn(regClient);
    }

    @Test
    public void testRun() throws Exception {
        cmd.run(serverConf);
        verify(cmd, times(1)).run(eq(regClient));
        verify(regClient, times(1))
            .initialize(eq(clientConf), eq(executor), eq(NullStatsLogger.INSTANCE), eq(Optional.empty()));
        verify(regClient, times(1)).close();
        verify(executor, times(1)).shutdown();
    }

}
