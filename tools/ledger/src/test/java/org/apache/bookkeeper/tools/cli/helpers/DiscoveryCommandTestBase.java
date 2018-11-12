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

import static org.mockito.Mockito.mock;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * A test base for discovery related commands.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DiscoveryCommand.class })
public abstract class DiscoveryCommandTestBase extends ClientCommandTestBase {

    protected RegistrationClient regClient;
    protected ScheduledExecutorService executor;

    @Before
    public void setup() throws Exception {
        super.setup();

        PowerMockito.mockStatic(Executors.class);

        this.executor = mock(ScheduledExecutorService.class);
        PowerMockito.when(Executors.newSingleThreadScheduledExecutor())
            .thenReturn(executor);

        this.regClient = mock(RegistrationClient.class);
        PowerMockito.when(metadataClientDriver.getRegistrationClient())
            .thenReturn(regClient);
    }

}
