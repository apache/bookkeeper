/*
 *
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
 *
 */

package org.apache.bookkeeper.server;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.bookie.UncleanShutdownDetectionImpl;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver.NullLedgerManagerFactory;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver.NullRegistrationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.BookieService;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link EmbeddedServer}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BookieService.class, BookieResources.class, EmbeddedServer.class})
public class TestEmbeddedServer {

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
        PowerMockito.mockStatic(BookieResources.class);
        when(BookieResources.createMetadataDriver(any(), any()))
            .thenReturn(new NullMetadataBookieDriver());
        when(BookieResources.createAllocator(any())).thenReturn(
                PowerMockito.mock(ByteBufAllocatorWithOomHandler.class));

        ServerConfiguration serverConf = new ServerConfiguration()
            .setAllowLoopback(true)
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { TestComponent.class.getName() });
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        whenNew(BookieImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(BookieImpl.class));
        whenNew(LegacyCookieValidation.class)
            .withAnyArguments().thenReturn(PowerMockito.mock(LegacyCookieValidation.class));

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
            .withAnyArguments()
            .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        EmbeddedServer server = EmbeddedServer.builder(conf).build();
        LifecycleComponentStack stack = server.getLifecycleComponentStack();
        assertEquals(7, stack.getNumComponents());
        assertTrue(stack.getComponent(6) instanceof TestComponent);

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    public void testBuildBookieServerCustomComponents() throws Exception {

        ServerConfiguration serverConf = new ServerConfiguration()
                .setAllowLoopback(true)
                .setAutoRecoveryDaemonEnabled(false)
                .setHttpServerEnabled(false)
                .setExtraServerComponents(new String[]{TestComponent.class.getName()});
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        StatsProvider statsProvider = new NullStatsProvider();
        StatsLogger rootStatsLogger = statsProvider.getStatsLogger("");
        RegistrationManager registrationManager = new NullRegistrationManager();
        LedgerManagerFactory ledgerManagerFactory = new NullLedgerManagerFactory();

        DiskChecker diskChecker = BookieResources.createDiskChecker(serverConf);

        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf.getServerConf(), diskChecker, rootStatsLogger.scope(LD_LEDGER_SCOPE));

        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf.getServerConf(), diskChecker, rootStatsLogger.scope(LD_INDEX_SCOPE), ledgerDirsManager);

        UncleanShutdownDetectionImpl uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);

        ByteBufAllocatorWithOomHandler byteBufFromResources = PowerMockito.mock(ByteBufAllocatorWithOomHandler.class);
        ByteBufAllocatorWithOomHandler byteBuf = PowerMockito.mock(ByteBufAllocatorWithOomHandler.class);

        PowerMockito.mockStatic(BookieResources.class);
        when(BookieResources.createMetadataDriver(any(), any())).thenReturn(new NullMetadataBookieDriver());
        when(BookieResources.createAllocator(any())).thenReturn(byteBufFromResources);

        whenNew(BookieImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(BookieImpl.class));
        whenNew(LegacyCookieValidation.class)
                .withAnyArguments().thenReturn(PowerMockito.mock(LegacyCookieValidation.class));

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
                .withAnyArguments()
                .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        EmbeddedServer server = EmbeddedServer.builder(conf)
                .statsProvider(statsProvider)
                .registrationManager(registrationManager)
                .ledgerManagerFactory(ledgerManagerFactory)
                .diskChecker(diskChecker)
                .ledgerDirsManager(ledgerDirsManager)
                .indexDirsManager(indexDirsManager)
                .allocator(byteBuf)
                .uncleanShutdownDetection(uncleanShutdownDetection)
                .build();

        assertSame(statsProvider, server.getStatsProvider());
        assertSame(registrationManager, server.getRegistrationManager());
        assertSame(ledgerManagerFactory, server.getLedgerManagerFactory());
        assertSame(diskChecker, server.getDiskChecker());
        assertSame(ledgerDirsManager, server.getLedgerDirsManager());
        assertSame(indexDirsManager, server.getIndexDirsManager());

        LifecycleComponentStack stack = server.getLifecycleComponentStack();
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
        PowerMockito.mockStatic(BookieResources.class);
        when(BookieResources.createMetadataDriver(any(), any()))
            .thenReturn(new NullMetadataBookieDriver());

        ServerConfiguration serverConf = new ServerConfiguration()
            .setAllowLoopback(true)
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { "bad-server-component"})
            .setIgnoreExtraServerComponentsStartupFailures(true);
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        whenNew(BookieImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(BookieImpl.class));
        whenNew(LegacyCookieValidation.class)
            .withAnyArguments().thenReturn(PowerMockito.mock(LegacyCookieValidation.class));

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
            .withAnyArguments()
            .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        LifecycleComponentStack stack = EmbeddedServer.builder(conf).build().getLifecycleComponentStack();
        assertEquals(6, stack.getNumComponents());

        stack.start();
        verify(mockServer, times(1)).start();

        stack.stop();

        stack.close();
        verify(mockServer, times(1)).shutdown();
    }

    @Test
    public void testExtraServerComponentsStartupFailures() throws Exception {
        PowerMockito.mockStatic(BookieResources.class);
        when(BookieResources.createMetadataDriver(any(), any()))
            .thenReturn(new NullMetadataBookieDriver());

        ServerConfiguration serverConf = new ServerConfiguration()
            .setAllowLoopback(true)
            .setAutoRecoveryDaemonEnabled(false)
            .setHttpServerEnabled(false)
            .setExtraServerComponents(new String[] { "bad-server-component"})
            .setIgnoreExtraServerComponentsStartupFailures(false);
        BookieConfiguration conf = new BookieConfiguration(serverConf);

        whenNew(BookieImpl.class).withAnyArguments().thenReturn(PowerMockito.mock(BookieImpl.class));
        whenNew(LegacyCookieValidation.class)
            .withAnyArguments().thenReturn(PowerMockito.mock(LegacyCookieValidation.class));

        BookieServer mockServer = PowerMockito.mock(BookieServer.class);
        whenNew(BookieServer.class)
                .withAnyArguments()
            .thenReturn(mockServer);

        BookieSocketAddress bookieAddress = new BookieSocketAddress("127.0.0.1", 1281);
        when(mockServer.getLocalAddress()).thenReturn(bookieAddress);
        when(mockServer.getBookieId()).thenReturn(bookieAddress.toBookieId());

        try {
            EmbeddedServer.builder(conf).build().getLifecycleComponentStack();
            fail("Should fail to start bookie server if `ignoreExtraServerComponentsStartupFailures` is set to false");
        } catch (RuntimeException re) {
            assertTrue(re.getCause() instanceof ClassNotFoundException);
        }
    }
}
