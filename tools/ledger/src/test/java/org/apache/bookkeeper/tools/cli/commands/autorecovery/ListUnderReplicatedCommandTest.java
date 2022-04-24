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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Vector;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ListUnderReplicatedCommand}.
 */
public class ListUnderReplicatedCommandTest extends BookieCommandTestBase {

    private UnderreplicatedLedger ledger;
    private LedgerManagerFactory factory;
    private LedgerUnderreplicationManager underreplicationManager;

    public ListUnderReplicatedCommandTest() {
        super(3, 0);
    }

    @Override
    public void setup() throws Exception {
        super.setup();

        factory = mock(LedgerManagerFactory.class);
        mockMetadataDriversWithLedgerManagerFactory(factory);

        underreplicationManager = mock(LedgerUnderreplicationManager.class);
        when(factory.newLedgerUnderreplicationManager()).thenReturn(underreplicationManager);

        ledger = mock(UnderreplicatedLedger.class);
        when(ledger.getLedgerId()).thenReturn(1L);
        when(ledger.getCtime()).thenReturn(1L);

        Vector<UnderreplicatedLedger> ledgers = new Vector<>();
        ledgers.add(ledger);

        when(underreplicationManager.listLedgersToRereplicate(any())).thenReturn(ledgers.iterator());

    }

    @Test
    public void testWithoutArgs()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException {
        testCommand("");
        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).listLedgersToRereplicate(any());
        verify(ledger, times(1)).getLedgerId();
        verify(ledger, times(1)).getCtime();
    }

    @Test
    public void testMissingReplica()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException {
        testCommand("-mr", "");
        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).listLedgersToRereplicate(any());
        verify(ledger, times(1)).getLedgerId();
        verify(ledger, times(1)).getCtime();
    }

    @Test
    public void testExcludingMissingReplica()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException {
        testCommand("-emr", "");
        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).listLedgersToRereplicate(any());
        verify(ledger, times(1)).getLedgerId();
        verify(ledger, times(1)).getCtime();
    }

    @Test
    public void testPrintMissingReplica()
        throws InterruptedException, ReplicationException.CompatibilityException, KeeperException {

        ArrayList<String> list = new ArrayList<>();
        list.add("replica");

        when(ledger.getReplicaList()).thenReturn(list);
        testCommand("-pmr");
        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).listLedgersToRereplicate(any());
        verify(ledger, times(1)).getLedgerId();
        verify(ledger, times(1)).getCtime();
        verify(ledger, times(1)).getReplicaList();
    }

    @Test
    public void testPrintReplicationWorkerId() throws ReplicationException.UnavailableException, InterruptedException,
                                                      ReplicationException.CompatibilityException, KeeperException {
        when(underreplicationManager.getReplicationWorkerIdRereplicatingLedger(1L)).thenReturn("test");

        testCommand("-prw");
        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).listLedgersToRereplicate(any());
        verify(ledger, times(1)).getLedgerId();
        verify(ledger, times(1)).getCtime();
        verify(underreplicationManager, times(1)).getReplicationWorkerIdRereplicatingLedger(1L);
    }

    @Test
    public void testOnlyDisplayLedgerCount() throws InterruptedException, KeeperException,
        ReplicationException.CompatibilityException, ReplicationException.UnavailableException {
        testCommand("-c");

        verify(factory, times(1)).newLedgerUnderreplicationManager();
        verify(underreplicationManager, times(1)).listLedgersToRereplicate(any());
        verify(underreplicationManager, times(0))
            .getReplicationWorkerIdRereplicatingLedger(anyLong());
        verify(ledger, times(0)).getLedgerId();
        verify(ledger, times(0)).getCtime();
        verify(ledger, times(0)).getReplicaList();
    }

    @Test
    public void testCommand1() {
        ListUnderReplicatedCommand cmd = new ListUnderReplicatedCommand();
        cmd.apply(bkFlags, new String[] { "" });
    }

    private void testCommand(String... args) {
        ListUnderReplicatedCommand cmd = new ListUnderReplicatedCommand();
        Assert.assertTrue(cmd.apply(bkFlags, args));
    }

}

