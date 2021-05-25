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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link FlipBookieIdCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FlipBookieIdCommand.class, BookieImpl.class, UpdateLedgerOp.class })
public class FlipBookieIdCommandTest extends BookieCommandTestBase {

    @Mock
    private ClientConfiguration clientConfiguration;

    @Mock
    private BookKeeper bookKeeper;

    @Mock
    private BookKeeperAdmin bookKeeperAdmin;

    @Mock
    private UpdateLedgerOp updateLedgerOp;

    @Mock
    private ServerConfiguration serverConfiguration;

    private BookieId bookieSocketAddress = BookieId.parse("localhost:9000");

    public FlipBookieIdCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        PowerMockito.whenNew(ServerConfiguration.class).withNoArguments().thenReturn(conf);
        PowerMockito.whenNew(ClientConfiguration.class).withNoArguments().thenReturn(clientConfiguration);
        PowerMockito.whenNew(BookKeeper.class).withParameterTypes(ClientConfiguration.class)
                    .withArguments(eq(clientConfiguration)).thenReturn(bookKeeper);
        PowerMockito.whenNew(BookKeeperAdmin.class).withParameterTypes(BookKeeper.class).withArguments(eq(bookKeeper))
                    .thenReturn(bookKeeperAdmin);
        PowerMockito.whenNew(UpdateLedgerOp.class).withArguments(eq(bookKeeper), eq(bookKeeperAdmin))
                    .thenReturn(updateLedgerOp);
        PowerMockito.whenNew(ServerConfiguration.class).withParameterTypes(AbstractConfiguration.class)
                    .withArguments(eq(conf)).thenReturn(serverConfiguration);
        PowerMockito.mockStatic(BookieImpl.class);
        PowerMockito.when(BookieImpl.getBookieId(eq(serverConfiguration))).thenReturn(bookieSocketAddress);
    }

    @Test
    public void testCommand() throws Exception {
        FlipBookieIdCommand cmd = new FlipBookieIdCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
        verifyNew(ClientConfiguration.class, times(1)).withNoArguments();
        verify(clientConfiguration, times(1)).addConfiguration(eq(conf));
        verifyNew(BookKeeper.class, times(1)).withArguments(eq(clientConfiguration));
        verifyNew(BookKeeperAdmin.class, times(1)).withArguments(eq(bookKeeper));
        verifyNew(UpdateLedgerOp.class, times(1)).withArguments(eq(bookKeeper), eq(bookKeeperAdmin));
        verifyNew(ServerConfiguration.class, times(1)).withArguments(eq(conf));
        verify(serverConfiguration, times(1)).setUseHostNameAsBookieID(anyBoolean());
        verify(updateLedgerOp, times(1)).updateBookieIdInLedgers(eq(bookieSocketAddress), eq(bookieSocketAddress),
                anyInt(), anyInt(), anyInt(), any());
    }

}
