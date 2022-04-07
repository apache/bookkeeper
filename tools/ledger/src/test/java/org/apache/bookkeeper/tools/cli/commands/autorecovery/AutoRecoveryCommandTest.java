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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link ToggleCommand}.
 */
@SuppressWarnings("unchecked")
public class AutoRecoveryCommandTest extends BookieCommandTestBase {

    private LedgerManagerFactory ledgerManagerFactory;
    private LedgerUnderreplicationManager ledgerUnderreplicationManager;

    public AutoRecoveryCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        ledgerManagerFactory = mock(LedgerManagerFactory.class);
        mockMetadataDriversWithLedgerManagerFactory(ledgerManagerFactory);

        ledgerUnderreplicationManager = mock(LedgerUnderreplicationManager.class);
        when(ledgerManagerFactory.newLedgerUnderreplicationManager()).thenReturn(ledgerUnderreplicationManager);
    }

    @Test
    public void testWithEnable()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException,
               ReplicationException.UnavailableException {
        testCommand("-e");
        verify(ledgerManagerFactory, times(1)).newLedgerUnderreplicationManager();
        verify(ledgerUnderreplicationManager, times(1)).isLedgerReplicationEnabled();
    }

    @Test
    public void testWithEnableLongArgs() throws ReplicationException.UnavailableException {
        when(ledgerUnderreplicationManager.isLedgerReplicationEnabled()).thenReturn(false);
        testCommand("--enable");
        verify(ledgerUnderreplicationManager, times(1)).enableLedgerReplication();
    }

    @Test
    public void testWithLook()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException,
               ReplicationException.UnavailableException {
        testCommand("s");
        verify(ledgerManagerFactory, times(1)).newLedgerUnderreplicationManager();
        verify(ledgerUnderreplicationManager, times(1)).isLedgerReplicationEnabled();
    }

    @Test
    public void testWithNoArgs()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException,
               ReplicationException.UnavailableException {
        testCommand("");
        verify(ledgerManagerFactory, times(1)).newLedgerUnderreplicationManager();
        verify(ledgerUnderreplicationManager, times(1)).isLedgerReplicationEnabled();
    }

    @Test
    public void testWithNoArgsDisable() throws ReplicationException.UnavailableException {
        when(ledgerUnderreplicationManager.isLedgerReplicationEnabled()).thenReturn(true);
        testCommand("");
        verify(ledgerUnderreplicationManager, times(1)).isLedgerReplicationEnabled();
        verify(ledgerUnderreplicationManager, times(1)).disableLedgerReplication();
    }

    private void testCommand(String... args) {
        ToggleCommand cmd = new ToggleCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }
}
