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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommandTestBase;
import org.apache.bookkeeper.tools.cli.helpers.CommandHelpers;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit test for {@link QueryAutoRecoveryStatusCommand}.
 */
public class QueryAutoRecoveryStatusCommandTest extends BookieCommandTestBase {

    @Rule
    public final Timeout globalTimeout = Timeout.seconds(30);

    public QueryAutoRecoveryStatusCommandTest() {
        super(3, 0);
    }
    LedgerUnderreplicationManager underreplicationManager;

    @Override
    public void setup() throws Exception {
        super.setup();
        BookieId bookieId = BookieId.parse(UUID.randomUUID().toString());
        LedgerManagerFactory ledgerManagerFactory = mock(LedgerManagerFactory.class);

        mockServerConfigurationConstruction(null);
        mockMetadataDriversWithLedgerManagerFactory(ledgerManagerFactory);

        LedgerManager ledgerManager = mock(LedgerManager.class);
        underreplicationManager = mock(LedgerUnderreplicationManager.class);

        when(ledgerManagerFactory.newLedgerManager()).thenReturn(ledgerManager);
        when(ledgerManagerFactory.newLedgerUnderreplicationManager()).thenReturn(underreplicationManager);

        List<BookieId> ensemble = Lists.newArrayList(new BookieSocketAddress("192.0.2.1", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.2", 1234).toBookieId(),
                new BookieSocketAddress("192.0.2.3", 1234).toBookieId());
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(11112233)
                .withClosedState()
                .withLength(100000999)
                .withLastEntryId(2000011)
                .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(2)
                .withPassword("passwd".getBytes())
                .withDigestType(BookKeeper.DigestType.CRC32.toApiDigestType())
                .newEnsembleEntry(0L, ensemble).build();
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
        Versioned<LedgerMetadata> vmeta = new Versioned<LedgerMetadata>(metadata, new LongVersion(1000));
        promise.complete(vmeta);

        when(ledgerManager.readLedgerMetadata(1)).thenReturn(promise);
        when(ledgerManager.readLedgerMetadata(33232)).thenReturn(promise);

        Constructor<? extends UnderreplicatedLedger> constructor = UnderreplicatedLedger.class.
                getDeclaredConstructor(long.class);
        constructor.setAccessible(true);
        final Queue<String> queue = new LinkedList<String>();
        queue.add("1111");
        Iterator<UnderreplicatedLedger> iter =  new Iterator<UnderreplicatedLedger>() {
            @Override
            public boolean hasNext() {
                if (queue.size() > 0) {
                    queue.remove();
                    try {
                        curBatch.add(constructor.newInstance(1));
                        curBatch.add(constructor.newInstance(33232));
                    } catch (Exception e) {
                    }
                }

                if (curBatch.size() > 0) {
                    return true;
                }
                return false;
            }

            @Override
            public UnderreplicatedLedger next() {
                return curBatch.remove();
            }

            final Queue<UnderreplicatedLedger> curBatch = new LinkedList<UnderreplicatedLedger>();
        };

        when(underreplicationManager.listLedgersToRereplicate(any())).thenReturn(iter);

        mockStatic(CommandHelpers.class, CALLS_REAL_METHODS).when(() -> CommandHelpers
                .getBookieSocketAddrStringRepresentation(
                        eq(bookieId), any(BookieAddressResolver.class))).thenReturn("");
    }

    @Test()
    public void testQueryRecoverStatusCommand() {
        try {
            when(underreplicationManager.getReplicationWorkerIdRereplicatingLedger(1)).thenReturn("192.168.0.103");
            when(underreplicationManager.getReplicationWorkerIdRereplicatingLedger(33232)).thenReturn("192.168.0.103");
        } catch (Exception e) {
        }
        QueryAutoRecoveryStatusCommand cmd = new QueryAutoRecoveryStatusCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "" }));
    }

    @Test()
    public void testQueryRecoverStatusCommandWithDetail() {
        try {
            when(underreplicationManager.getReplicationWorkerIdRereplicatingLedger(1)).thenReturn("192.168.0.103");
            when(underreplicationManager.getReplicationWorkerIdRereplicatingLedger(33232)).thenReturn("192.168.0.103");
        } catch (Exception e) {
        }
        QueryAutoRecoveryStatusCommand cmd = new QueryAutoRecoveryStatusCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-v" }));
    }

    @Test()
    public void testNoLedgerIsBeingRecovered() {
        QueryAutoRecoveryStatusCommand cmd = new QueryAutoRecoveryStatusCommand();
        Assert.assertTrue(cmd.apply(bkFlags, new String[] { "-v" }));
    }
}
