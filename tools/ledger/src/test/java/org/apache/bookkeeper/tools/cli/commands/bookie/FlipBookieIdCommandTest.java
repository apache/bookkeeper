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
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/**
 * Unit test for {@link FlipBookieIdCommand}.
 */
public class FlipBookieIdCommandTest extends BookieCommandTestBase {

    private MockedConstruction<UpdateLedgerOp> updateLedgerOpMockedConstruction;
    private BookieId bookieSocketAddress = BookieId.parse("localhost:9000");
    private MockedStatic<BookieImpl> bookieMockedStatic;

    public FlipBookieIdCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        createMockedClientConfiguration();
        createMockedServerConfiguration();
        addMockedConstruction(mockConstruction(BookKeeper.class));
        createMockedBookKeeperAdmin();
        updateLedgerOpMockedConstruction = mockConstruction(UpdateLedgerOp.class);
        addMockedConstruction(updateLedgerOpMockedConstruction);
        bookieMockedStatic = mockStatic(BookieImpl.class);

        bookieMockedStatic.when(() -> BookieImpl.getBookieId(any(ServerConfiguration.class)))
                .thenReturn(bookieSocketAddress);
    }

    @After
    public void after() {
        bookieMockedStatic.close();
    }

    @Test
    public void testCommand() throws Exception {
        FlipBookieIdCommand cmd = new FlipBookieIdCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
        verify(clientConfigurationMockedConstruction.constructed().get(0), times(1))
                .addConfiguration(any(ServerConfiguration.class));
        verify(serverConfigurationMockedConstruction.constructed().get(1), times(1))
                .setUseHostNameAsBookieID(anyBoolean());
        verify(updateLedgerOpMockedConstruction.constructed().get(0), times(1))
            .updateBookieIdInLedgers(eq(bookieSocketAddress), eq(bookieSocketAddress),
                anyInt(), anyInt(), anyInt(), any());
    }

}
