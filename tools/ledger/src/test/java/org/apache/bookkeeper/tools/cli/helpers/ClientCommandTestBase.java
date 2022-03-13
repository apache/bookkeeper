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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.net.URI;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.junit.Before;

/**
 * A test base for testing client commands.
 */
public abstract class ClientCommandTestBase extends CommandTestBase {

    protected BookKeeperBuilder mockBkBuilder;
    protected BookKeeper mockBk;
    protected MetadataClientDriver metadataClientDriver;

    @Before
    public void setup() throws Exception {
        mockBk = mock(BookKeeper.class);
        mockConstruction(ClientConfiguration.class, withSettings().defaultAnswer(CALLS_REAL_METHODS),
                        (mock, context) ->
                        doReturn("zk://127.0.0.1/path/to/ledgers").when(mock).getMetadataServiceUri()
                );

        mockStatic(BookKeeper.class);
        this.mockBkBuilder = mock(BookKeeperBuilder.class, CALLS_REAL_METHODS);
        this.mockBk = mock(BookKeeper.class);
        getMockedStatic(BookKeeper.class).when(() -> BookKeeper.newBuilder(any(ClientConfiguration.class)))
                .thenReturn(mockBkBuilder);
        when(mockBkBuilder.build()).thenReturn(mockBk);

        this.metadataClientDriver = mock(MetadataClientDriver.class);
        mockStatic(MetadataDrivers.class);
        getMockedStatic(MetadataDrivers.class).when(() -> MetadataDrivers.getClientDriver(any(URI.class)))
            .thenReturn(metadataClientDriver);
    }
}
