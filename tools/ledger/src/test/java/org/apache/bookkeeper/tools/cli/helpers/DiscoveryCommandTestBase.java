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
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.junit.Before;
/**
 * A test base for discovery related commands.
 */
public abstract class DiscoveryCommandTestBase extends ClientCommandTestBase {

    protected RegistrationClient regClient;
    protected ScheduledExecutorService executor;

    @Before
    public void setup() throws Exception {
        super.setup();

        this.executor = mock(ScheduledExecutorService.class);
        mockStatic(Executors.class).when(() -> Executors.newSingleThreadScheduledExecutor()).thenReturn(executor);
        this.regClient = mock(RegistrationClient.class);
        when(metadataClientDriver.getRegistrationClient())
            .thenReturn(regClient);
    }
}
