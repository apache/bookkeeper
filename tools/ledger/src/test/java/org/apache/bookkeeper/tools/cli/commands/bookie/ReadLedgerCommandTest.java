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
package org.apache.bookkeeper.tools.cli.commands.bookie;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;

import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link ReadLedgerCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ReadLedgerCommand.class, BookKeeperAdmin.class, BookieSocketAddress.class, ClientConfiguration.class,
    LedgerHandle.class, LedgerEntry.class, OrderedExecutor.class })
public class ReadLedgerCommandTest extends BookieCommandTestBase {

    @Mock
    private BookieSocketAddress bookieSocketAddress;

    @Mock
    private ClientConfiguration clientConfiguration;

    @Mock
    private BookKeeperAdmin bookKeeperAdmin;

    @Mock
    private LedgerHandle ledgerHandle;

    @Mock
    private LedgerEntry entry;

    @Mock
    private NioEventLoopGroup nioEventLoopGroup;

    @Mock
    private OrderedExecutor orderedExecutor;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    @Mock
    private DefaultThreadFactory defaultThreadFactory;

    @Mock
    private BookieClientImpl bookieClient;

    public ReadLedgerCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(BookieSocketAddress.class).withArguments(anyString()).thenReturn(bookieSocketAddress);
        PowerMockito.whenNew(ClientConfiguration.class).withNoArguments().thenReturn(clientConfiguration);
        PowerMockito.whenNew(BookKeeperAdmin.class).withParameterTypes(ClientConfiguration.class)
                    .withArguments(eq(clientConfiguration)).thenReturn(bookKeeperAdmin);

        when(bookKeeperAdmin.openLedger(anyLong())).thenReturn(ledgerHandle);
        when(ledgerHandle.getLastAddConfirmed()).thenReturn(1L);

        List<LedgerEntry> entries = new LinkedList<>();
        entries.add(entry);
        when(entry.getLedgerId()).thenReturn(1L);
        when(entry.getEntryId()).thenReturn(1L);
        when(entry.getLength()).thenReturn(1L);

        when(bookKeeperAdmin.readEntries(anyLong(), anyLong(), anyLong())).thenReturn(entries);

        PowerMockito.whenNew(NioEventLoopGroup.class).withNoArguments().thenReturn(nioEventLoopGroup);

        PowerMockito.mockStatic(OrderedExecutor.class);
        OrderedExecutor.Builder builder = mock(OrderedExecutor.Builder.class);
        when(OrderedExecutor.newBuilder()).thenReturn(builder);
        when(builder.numThreads(anyInt())).thenCallRealMethod();
        when(builder.name(anyString())).thenCallRealMethod();
        when(builder.build()).thenReturn(orderedExecutor);

        PowerMockito.mockStatic(Executors.class);
        PowerMockito.whenNew(DefaultThreadFactory.class).withArguments(anyString()).thenReturn(defaultThreadFactory);
        when(Executors.newSingleThreadScheduledExecutor(eq(defaultThreadFactory))).thenReturn(scheduledExecutorService);

        PowerMockito.whenNew(BookieClientImpl.class)
                    .withArguments(eq(clientConfiguration), eq(nioEventLoopGroup), eq(UnpooledByteBufAllocator.DEFAULT),
                                   eq(orderedExecutor), eq(scheduledExecutorService), eq(NullStatsLogger.INSTANCE))
                    .thenReturn(bookieClient);


    }

    @Test
    public void testWithoutBookieAddress() throws Exception {
        ReadLedgerCommand cmd = new ReadLedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-r" }));
        verifyNew(ClientConfiguration.class, times(1)).withNoArguments();
        verify(clientConfiguration, times(1)).addConfiguration(eq(conf));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(clientConfiguration));
        verify(bookKeeperAdmin, times(1)).openLedger(anyLong());
        verify(ledgerHandle, times(1)).getLastAddConfirmed();
        verify(bookKeeperAdmin, times(1)).readEntries(anyLong(), anyLong(), anyLong());
        verify(entry, times(1)).getLedgerId();
        verify(entry, times(1)).getEntryId();
        verify(entry, times(1)).getLength();
    }

    @Test
    public void testWithBookieAddress() throws Exception {
        ReadLedgerCommand cmd = new ReadLedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-b", "localhost:9000" }));
        verifyNew(NioEventLoopGroup.class, times(1)).withNoArguments();
        verifyNew(DefaultThreadFactory.class, times(1)).withArguments(anyString());
        verifyNew(BookieClientImpl.class, times(1))
            .withArguments(eq(clientConfiguration), eq(nioEventLoopGroup), eq(UnpooledByteBufAllocator.DEFAULT),
                           eq(orderedExecutor), eq(scheduledExecutorService), eq(NullStatsLogger.INSTANCE));
        verify(nioEventLoopGroup, times(1)).shutdownGracefully();
        verify(orderedExecutor, times(1)).shutdown();
        verify(bookieClient, times(1)).close();
    }

}
