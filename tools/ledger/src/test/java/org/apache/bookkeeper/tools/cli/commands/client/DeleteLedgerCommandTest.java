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
package org.apache.bookkeeper.tools.cli.commands.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test for {@link DeleteLedgerCommand}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ DeleteLedgerCommand.class, BookKeeper.class, IOUtils.class, ClientConfiguration.class })
public class DeleteLedgerCommandTest extends BookieCommandTestBase {

    private BookKeeper bookKeeper;
    private ClientConfiguration clientConf;

    public DeleteLedgerCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {

        this.clientConf = mock(ClientConfiguration.class);
        PowerMockito.whenNew(ClientConfiguration.class).withNoArguments().thenReturn(clientConf);
        PowerMockito.doNothing().when(clientConf).addConfiguration(eq(conf));

        this.bookKeeper = mock(BookKeeper.class);
        PowerMockito.whenNew(BookKeeper.class).withParameterTypes(ClientConfiguration.class)
                    .withArguments(eq(clientConf)).thenReturn(bookKeeper);
        PowerMockito.doNothing().when(bookKeeper).deleteLedger(anyLong());
        PowerMockito.doNothing().when(bookKeeper).close();

        PowerMockito.mockStatic(IOUtils.class);

    }

    @Test
    public void testCommandWithoutForce() throws Exception {
        PowerMockito.when(IOUtils.class, "confirmPrompt", anyString()).thenReturn(false);

        DeleteLedgerCommand cmd = new DeleteLedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1" }));

        PowerMockito.verifyNew(ClientConfiguration.class, never()).withNoArguments();
        verify(clientConf, never()).addConfiguration(conf);
        PowerMockito.verifyNew(BookKeeper.class, never()).withArguments(eq(clientConf));
        verify(bookKeeper, never()).deleteLedger(1);
    }

    @Test
    public void testCommandWithForce() throws Exception {
        DeleteLedgerCommand cmd = new DeleteLedgerCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-f" }));

        PowerMockito.verifyNew(ClientConfiguration.class, times(1)).withNoArguments();
        verify(clientConf, times(1)).addConfiguration(any(ServerConfiguration.class));
        PowerMockito.verifyNew(BookKeeper.class, times(1)).withArguments(eq(clientConf));
        verify(bookKeeper, times(1)).deleteLedger(1);
    }

}
