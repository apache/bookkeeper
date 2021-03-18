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

package org.apache.bookkeeper.server;

import static org.apache.bookkeeper.server.Main.buildBookieServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;

import java.util.function.Supplier;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.BookieService;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link Main}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BookieService.class)
public class TestMain {

    static class TestComponent extends ServerLifecycleComponent {

        public TestComponent(BookieConfiguration conf, StatsLogger statsLogger) {
            super("test-component", conf, statsLogger);
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() throws IOException {
        }

    }

    @Test
    public void testBuildBookieServer() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration()
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { TestComponent.class.getName() });
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
            .withArguments(any(ServerConfiguration.class), any(StatsLogger.class), any(Supplier.class))
            .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        LifecycleComponentStack stack = buildBookieServer(conf);
        assertEquals(3, stack.getNumComponents());
        assertTrue(stack.getComponent(2) instanceof TestComponent);

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    public void testIgnoreExtraServerComponentsStartupFailures() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration()
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { "bad-server-component"})
            .setIgnoreExtraServerComponentsStartupFailures(true);
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
            .withArguments(any(ServerConfiguration.class), any(StatsLogger.class), any(Supplier.class))
            .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        LifecycleComponentStack stack = buildBookieServer(conf);
        assertEquals(2, stack.getNumComponents());

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    public void testExtraServerComponentsStartupFailures() throws Exception {
        ServerConfiguration serverConf = new ServerConfiguration()
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { "bad-server-component"})
            .setIgnoreExtraServerComponentsStartupFailures(false);
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
            .withArguments(any(ServerConfiguration.class), any(StatsLogger.class), any(Supplier.class))
            .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        try {
            buildBookieServer(conf);
            fail("Should fail to start bookie server if `ignoreExtraServerComponentsStartupFailures` is set to false");
        } catch (RuntimeException re) {
            assertTrue(re.getCause() instanceof ClassNotFoundException);
        }
    }

}
