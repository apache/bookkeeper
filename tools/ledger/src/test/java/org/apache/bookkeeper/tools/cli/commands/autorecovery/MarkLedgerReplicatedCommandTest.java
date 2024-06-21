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
package org.apache.bookkeeper.tools.cli.commands.autorecovery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;

import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.Test;

/**
 * Unit test for {@link MarkLedgerReplicatedCommand}.
 */
public class MarkLedgerReplicatedCommandTest extends BookieCommandTestBase {

    private LedgerManagerFactory factory;
    private LedgerUnderreplicationManager underreplicationManager;

    public MarkLedgerReplicatedCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        mockStatic(IOUtils.class);

        factory = mock(LedgerManagerFactory.class);
        mockMetadataDriversWithLedgerManagerFactory(factory);
        underreplicationManager = mock(LedgerUnderreplicationManager.class);
        when(factory.newLedgerUnderreplicationManager()).thenReturn(underreplicationManager);
        doNothing().when(underreplicationManager).markLedgerReplicated(anyLong());
    }

    @Test
    public void testCommandWithoutForceAndConfirmNo() throws InterruptedException, ReplicationException {
        getMockedStatic(IOUtils.class).when(() -> IOUtils.confirmPrompt(anyString())).thenReturn(false);

        MarkLedgerReplicatedCommand cmd = new MarkLedgerReplicatedCommand();
        assertFalse(cmd.apply(bkFlags, new String[] { "-l", "1" }));

        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(0)).markLedgerReplicated(1L);
    }

    @Test
    public void testCommandWithoutForceAndConfirmYes() throws InterruptedException, ReplicationException {
        getMockedStatic(IOUtils.class).when(() -> IOUtils.confirmPrompt(anyString())).thenReturn(true);

        MarkLedgerReplicatedCommand cmd = new MarkLedgerReplicatedCommand();
        assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1" }));

        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).markLedgerReplicated(1L);
    }

    @Test
    public void testCommandWithForce() throws InterruptedException, ReplicationException {
        MarkLedgerReplicatedCommand cmd = new MarkLedgerReplicatedCommand();
        assertTrue(cmd.apply(bkFlags, new String[] { "-l", "1", "-f" }));

        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).markLedgerReplicated(1L);
    }

}
