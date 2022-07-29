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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ReadLedgerCommand}.
 */
public class ReadLedgerCommandTest extends BookieCommandTestBase {

    private static final BookieId bookieSocketAddress = BookieId.parse("localhost:9000");

    private LedgerHandle ledgerHandle;
    private LedgerEntry entry;
    private OrderedExecutor orderedExecutor;
    private ScheduledExecutorService scheduledExecutorService;


    public ReadLedgerCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        mockServerConfigurationConstruction();
        mockClientConfigurationConstruction();
        ledgerHandle = mock(LedgerHandle.class);
        entry = mock(LedgerEntry.class);
        orderedExecutor = mock(OrderedExecutor.class);
        scheduledExecutorService = mock(ScheduledExecutorService.class);

        when(ledgerHandle.getLastAddConfirmed()).thenReturn(1L);

        List<LedgerEntry> entries = new LinkedList<>();
        entries.add(entry);
        when(entry.getLedgerId()).thenReturn(1L);
        when(entry.getEntryId()).thenReturn(1L);
        when(entry.getLength()).thenReturn(1L);

        mockBookKeeperAdminConstruction(new Consumer<BookKeeperAdmin>() {
            @Override
            @SneakyThrows
            public void accept(BookKeeperAdmin bookKeeperAdmin) {
                when(bookKeeperAdmin.getBookieAddressResolver())
                        .thenReturn(BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
                when(bookKeeperAdmin.openLedger(anyLong())).thenReturn(ledgerHandle);
                when(bookKeeperAdmin.readEntries(anyLong(), anyLong(), anyLong())).thenReturn(entries);
            }
        });

        mockConstruction(NioEventLoopGroup.class);



        OrderedExecutor.Builder builder = mock(OrderedExecutor.Builder.class);
        mockStatic(OrderedExecutor.class).when(() -> OrderedExecutor.newBuilder()).thenReturn(builder);

        when(builder.numThreads(anyInt())).thenCallRealMethod();
        when(builder.name(anyString())).thenCallRealMethod();
        when(builder.build()).thenReturn(orderedExecutor);

        mockConstruction(DefaultThreadFactory.class);

        mockStatic(Executors.class).when(() -> Executors
                .newSingleThreadScheduledExecutor(any(DefaultThreadFactory.class)))
                .thenReturn(scheduledExecutorService);

        mockConstruction(BookieClientImpl.class);


    }

    @Test
    public void testWithoutBookieAddress() throws Exception {
        ReadLedgerCommand cmd = new ReadLedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-r" }));
        verify(ledgerHandle, times(1)).getLastAddConfirmed();
        verify(getMockedConstruction(BookKeeperAdmin.class).constructed().get(0),
                times(1)).readEntries(anyLong(), anyLong(), anyLong());
        verify(entry, times(1)).getLedgerId();
        verify(entry, times(1)).getEntryId();
        verify(entry, times(1)).getLength();
    }

    @Test
    public void testWithBookieAddress() throws Exception {
        ReadLedgerCommand cmd = new ReadLedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-b", bookieSocketAddress.getId() }));
        Assert.assertEquals(1, getMockedConstruction(NioEventLoopGroup.class).constructed().size());
        Assert.assertEquals(1, getMockedConstruction(DefaultThreadFactory.class).constructed().size());
        Assert.assertEquals(1, getMockedConstruction(BookieClientImpl.class).constructed().size());
        verify(getMockedConstruction(NioEventLoopGroup.class).constructed().get(0), times(1)).shutdownGracefully();
        verify(orderedExecutor, times(1)).shutdown();
        verify(getMockedConstruction(BookieClientImpl.class).constructed().get(0), times(1)).close();
    }

}
