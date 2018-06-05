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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

import java.net.URI;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * A test base for testing client commands.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ClientCommand.class, BookKeeper.class, MetadataDrivers.class })
public abstract class ClientCommandTestBase extends CommandTestBase {

    protected ClientConfiguration clientConf;
    protected BookKeeperBuilder mockBkBuilder;
    protected BookKeeper mockBk;
    protected MetadataClientDriver metadataClientDriver;

    @Before
    public void setup() throws Exception {
        mockBk = mock(BookKeeper.class);
        this.clientConf = spy(new ClientConfiguration(conf));
        this.clientConf.setMetadataServiceUri("zk://127.0.0.1/path/to/ledgers");
        PowerMockito.whenNew(ClientConfiguration.class)
            .withNoArguments()
            .thenReturn(clientConf);
        PowerMockito.whenNew(ClientConfiguration.class)
            .withParameterTypes(AbstractConfiguration.class)
            .withArguments(eq(conf))
            .thenReturn(clientConf);
        PowerMockito.mockStatic(BookKeeper.class);
        this.mockBkBuilder = mock(BookKeeperBuilder.class, CALLS_REAL_METHODS);
        this.mockBk = mock(BookKeeper.class);
        PowerMockito.when(
            BookKeeper.class, "newBuilder", eq(clientConf))
            .thenReturn(mockBkBuilder);
        when(mockBkBuilder.build()).thenReturn(mockBk);

        PowerMockito.mockStatic(MetadataDrivers.class);
        this.metadataClientDriver = mock(MetadataClientDriver.class);
        PowerMockito.when(
            MetadataDrivers.getClientDriver(any(URI.class)))
            .thenReturn(metadataClientDriver);
    }

}
