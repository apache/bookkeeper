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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;


/**
 * Unit test for {@link DeleteLedgerCommand}.
 */
public class DeleteLedgerCommandTest extends BookieCommandTestBase {

    public DeleteLedgerCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {


        mockStatic(IOUtils.class);
    }

    @Test
    public void testCommandWithoutForce() throws Exception {
        getMockedStatic(IOUtils.class).when(() -> IOUtils.confirmPrompt(anyString())).thenReturn(false);
        mockClientConfigurationConstruction(conf -> {
            doThrow(new RuntimeException("unexpected call")).when(conf).addConfiguration(any(Configuration.class));
        });

        mockConstruction(BookKeeper.class, (bk, context) -> {
            throw new RuntimeException("unexpected call");
        });

        DeleteLedgerCommand cmd = new DeleteLedgerCommand();
        assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1" }));
        assertTrue(getMockedConstruction(BookKeeper.class).constructed().isEmpty());
    }

    @Test
    public void testCommandWithForce() throws Exception {
        AtomicBoolean calledAddConf = new AtomicBoolean();
        mockClientConfigurationConstruction(conf -> {
            doAnswer(invocation -> {
                calledAddConf.set(true);
                return conf;
            }).when(conf).addConfiguration(any(Configuration.class));
        });

        mockConstruction(BookKeeper.class, (bk, context) -> {
            doNothing().when(bk).deleteLedger(anyLong());
            doNothing().when(bk).close();
        });

        DeleteLedgerCommand cmd = new DeleteLedgerCommand();
        assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-f" }));

        assertTrue(calledAddConf.get());
        verify(getMockedConstruction(BookKeeper.class).constructed().get(0), times(1)).deleteLedger(1);
    }

}
