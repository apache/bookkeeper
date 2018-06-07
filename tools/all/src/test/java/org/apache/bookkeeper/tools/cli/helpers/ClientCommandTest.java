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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link ClientCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ClientCommand.class, BookKeeper.class })
public class ClientCommandTest {

    private ClientCommand<CliFlags> cmd;
    private ServerConfiguration serverConf;
    private ClientConfiguration clientConf;
    private BookKeeperBuilder bkBuilder;
    private BookKeeper bk;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        this.cmd = mock(ClientCommand.class, CALLS_REAL_METHODS);

        this.serverConf = new ServerConfiguration();
        this.serverConf.setMetadataServiceUri("zk://127.0.0.1/path/to/ledgers");
        this.clientConf = new ClientConfiguration(serverConf);
        PowerMockito.whenNew(ClientConfiguration.class)
            .withParameterTypes(AbstractConfiguration.class)
            .withArguments(eq(serverConf))
            .thenReturn(clientConf);
        PowerMockito.mockStatic(BookKeeper.class);
        this.bkBuilder = mock(BookKeeperBuilder.class, CALLS_REAL_METHODS);
        this.bk = mock(BookKeeper.class);
        PowerMockito.when(
            BookKeeper.class, "newBuilder", eq(clientConf))
            .thenReturn(bkBuilder);
        when(bkBuilder.build()).thenReturn(bk);
    }

    @Test
    public void testRun() throws Exception {
        CliFlags flags = new CliFlags();
        assertTrue(cmd.apply(serverConf, flags));
        verify(cmd, times(1)).apply(eq(clientConf), same(flags));
        verify(cmd, times(1)).run(eq(bk), same(flags));
        verify(bkBuilder, times(1)).build();
    }

}
