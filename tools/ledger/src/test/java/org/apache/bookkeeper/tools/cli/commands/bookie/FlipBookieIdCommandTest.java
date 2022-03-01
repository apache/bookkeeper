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
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link FlipBookieIdCommand}.
 */
public class FlipBookieIdCommandTest extends BookieCommandTestBase {

    private static final BookieId bookieSocketAddress = BookieId.parse("localhost:9000");

    public FlipBookieIdCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        mockClientConfigurationConstruction();
        mockServerConfigurationConstruction();
        mockConstruction(BookKeeper.class);
        mockBookKeeperAdminConstruction();
        mockConstruction(UpdateLedgerOp.class);
        mockStatic(BookieImpl.class).when(() -> BookieImpl.getBookieId(any(ServerConfiguration.class)))
                .thenReturn(bookieSocketAddress);
    }

    @Test
    public void testCommand() throws Exception {
        FlipBookieIdCommand cmd = new FlipBookieIdCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
        verify(getMockedConstruction(ClientConfiguration.class).constructed().get(0), times(1))
                .addConfiguration(any(ServerConfiguration.class));
        verify(getMockedConstruction(ServerConfiguration.class).constructed().get(1), times(1))
                .setUseHostNameAsBookieID(anyBoolean());
        verify(getMockedConstruction(UpdateLedgerOp.class).constructed().get(0), times(1))
            .updateBookieIdInLedgers(eq(bookieSocketAddress), eq(bookieSocketAddress),
                anyInt(), anyInt(), anyInt(), any());
    }

}
