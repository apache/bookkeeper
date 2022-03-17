/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static junit.framework.TestCase.assertEquals;
import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.mockito.ArgumentMatchers.eq;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tools.cli.commands.bookies.CorrectEnsemblePlacementCommand;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of correct-ensemble-placement command.
 */
public class CorrectEnsemblePlacementCmdTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(CorrectEnsemblePlacementCmdTest.class);
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";

    public CorrectEnsemblePlacementCmdTest() throws Exception {
        super(0);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setGcWaitTime(60000);
        baseConf.setFlushInterval(1);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        StaticDNSResolver.reset();
    }

    /**
     * list of entry logger files that contains given ledgerId.
     */
    @Test
    public void testArgument() throws Exception {
        startNewBookie();

        @Cleanup
        final BookKeeper bk = new BookKeeper(baseClientConf, zkc);
        @Cleanup
        final LedgerHandle lh = createLedgerWithEntries(bk, 10, 1, 1);

        final String[] argv1 = { "correct-ensemble-placement", "--ledgerids", String.valueOf(lh.getId()),
                "--skipOpenLedgers", "--force"};
        final String[] argv2 = { "correct-ensemble-placement", "--ledgerids", String.valueOf(lh.getId()),
                "--skipOpenLedgers", "--force", "--dryrun"};
        final BookieShell bkShell =
                new BookieShell(LedgerIdFormatter.LONG_LEDGERID_FORMATTER, EntryFormatter.STRING_FORMATTER);
        bkShell.setConf(baseClientConf);

        assertEquals("Failed to return exit code!", 0, bkShell.run(argv1));
        assertEquals("Failed to return exit code!", 0, bkShell.run(argv2));
    }

    @Test
    public void testCorrectEnsemblePlacementByRackaware() throws Exception {
        final int ensembleSize = 7;
        final int quorumSize = 2;

        final BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181);
        final BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181);
        final BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181);
        final BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181);
        final BookieSocketAddress addr5 = new BookieSocketAddress("127.0.0.5", 3181);
        final BookieSocketAddress addr6 = new BookieSocketAddress("127.0.0.6", 3181);
        final BookieSocketAddress addr7 = new BookieSocketAddress("127.0.0.7", 3181);
        final BookieSocketAddress addr8 = new BookieSocketAddress("127.0.0.8", 3181);
        final BookieSocketAddress addr9 = new BookieSocketAddress("127.0.0.9", 3181);

        final Set<BookieId> writableBookies = new HashSet<>();
        writableBookies.add(addr1.toBookieId());
        writableBookies.add(addr2.toBookieId());
        writableBookies.add(addr3.toBookieId());
        writableBookies.add(addr4.toBookieId());
        writableBookies.add(addr5.toBookieId());
        writableBookies.add(addr6.toBookieId());
        writableBookies.add(addr7.toBookieId());
        writableBookies.add(addr8.toBookieId());
        writableBookies.add(addr9.toBookieId());

        // add bookie node to resolver
        StaticDNSResolver.reset();

        final String rackName1 = NetworkTopology.DEFAULT_REGION + "/r1";
        final String rackName2 = NetworkTopology.DEFAULT_REGION + "/r2";
        final String rackName3 = NetworkTopology.DEFAULT_REGION + "/r3";

        // update dns mapping
        // add port for testing
        StaticDNSResolver.addNodeToRack(addr1.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr2.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr3.getSocketAddress().getAddress().getHostAddress(), rackName1);
        StaticDNSResolver.addNodeToRack(addr4.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr5.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr6.getSocketAddress().getAddress().getHostAddress(), rackName2);
        StaticDNSResolver.addNodeToRack(addr7.getSocketAddress().getAddress().getHostAddress(), rackName3);
        StaticDNSResolver.addNodeToRack(addr8.getSocketAddress().getAddress().getHostAddress(), rackName3);
        StaticDNSResolver.addNodeToRack(addr9.getSocketAddress().getAddress().getHostAddress(), rackName3);
        LOG.info("Set up static DNS Resolver.");
        baseClientConf.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
        baseClientConf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());

        startNewBookie();

        final NavigableMap<Long, List<BookieId>> ensemble = new TreeMap<>();
        // create failed ensemble
        // expect that the ensemble will be replaced to
        // [addr1, addr4, addr7, addr2, addr5, addr8, *addr6 (in /default-region/r2 bookies)*]
        ensemble.put(0L, Arrays.asList(
                addr1.toBookieId(), addr4.toBookieId(),
                addr7.toBookieId(), addr2.toBookieId(),
                addr5.toBookieId(), addr8.toBookieId(),
                addr3.toBookieId()));
        final LedgerMetadata lm1 = Mockito.mock(LedgerMetadata.class);
        Mockito.doReturn(ensemble).when(lm1).getAllEnsembles();
        Mockito.doReturn(ensembleSize).when(lm1).getEnsembleSize();
        Mockito.doReturn(quorumSize).when(lm1).getWriteQuorumSize();
        Mockito.doReturn(quorumSize).when(lm1).getAckQuorumSize();
        final LedgerHandle lh1 = Mockito.mock(LedgerHandle.class);
        Mockito.when(lh1.getLedgerMetadata()).thenReturn(lm1);

        @Cleanup
        final BookKeeper bookKeeper = Mockito.spy(new BookKeeper(baseClientConf));
        Mockito.doReturn(true).when(bookKeeper).isClosed(Mockito.anyLong());

        @Cleanup
        final BookKeeperAdmin admin = Mockito.spy(new BookKeeperAdmin(baseClientConf));
        Mockito.doAnswer(invocationOnMock -> {
            final AsyncCallback.OpenCallback op = invocationOnMock.getArgument(1);
            final CountDownLatch ctx = invocationOnMock.getArgument(2);
            op.openComplete(BKException.Code.OK, lh1, ctx);
            return null;
        }).when(admin).asyncOpenLedger(Mockito.anyLong(), Mockito.any(), Mockito.any());
        // expected return
        final Map<Integer, BookieId> expectedMap = new HashMap<>();
        expectedMap.put(6, addr6.toBookieId());
        Mockito.doNothing().when(admin)
                .replicateLedgerFragment(Mockito.any(), Mockito.any(), eq(expectedMap), Mockito.any());

        final EnsemblePlacementPolicy policy = bookKeeper.getPlacementPolicy();
        final Field topologyField = TopologyAwareEnsemblePlacementPolicy.class
                .getDeclaredField("topology");
        topologyField.setAccessible(true);
        final NetworkTopology topology = Mockito.spy((NetworkTopology) topologyField.get(policy));
        Mockito.doReturn(2).when(topology).getNumOfRacks();
        topologyField.set(policy, topology);
        final Field bookieAddressResolverField = TopologyAwareEnsemblePlacementPolicy.class
                .getDeclaredField("bookieAddressResolver");
        bookieAddressResolverField.setAccessible(true);
        final BookieAddressResolver bookieAddressResolver =
                Mockito.spy((BookieAddressResolver) bookieAddressResolverField.get(policy));
        Mockito.doAnswer(invocationOnMock -> {
            final BookieId bookieId = invocationOnMock.getArgument(0);
            return new BookieSocketAddress(bookieId.getId());
        }).when(bookieAddressResolver).resolve(Mockito.any());
        bookieAddressResolverField.set(policy, bookieAddressResolver);

        // add mock bookies to knownBookies
        policy.onClusterChanged(writableBookies, Collections.emptySet());

        // make sure that the ensemble is FAIL state
        assertEquals(EnsemblePlacementPolicy.PlacementPolicyAdherence.FAIL,
                policy.isEnsembleAdheringToPlacementPolicy(ensemble.get(0L), quorumSize, quorumSize));

        final CorrectEnsemblePlacementCommand.CorrectEnsemblePlacementFlags flag =
                new CorrectEnsemblePlacementCommand.CorrectEnsemblePlacementFlags();
        flag.ledgerIds(Collections.singletonList(1L));
        final CorrectEnsemblePlacementCommand cmd = new CorrectEnsemblePlacementCommand();
        cmd.relocate(baseConf, flag, bookKeeper, admin);

        Mockito.verify(admin, Mockito.times(1))
                .replicateLedgerFragment(Mockito.any(), Mockito.any(), eq(expectedMap), Mockito.any());
    }

    private LedgerHandle createLedgerWithEntries(BookKeeper bk, int numOfEntries,
                                                 int ensembleSize, int quorumSize) throws Exception {
        LedgerHandle lh = bk.createLedger(ensembleSize, quorumSize, digestType, PASSWORD.getBytes());
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch latch = new CountDownLatch(numOfEntries);

        final AsyncCallback.AddCallback cb = (rccb, lh1, entryId, ctx) -> {
            rc.compareAndSet(BKException.Code.OK, rccb);
            latch.countDown();
        };
        for (int i = 0; i < numOfEntries; i++) {
            lh.asyncAddEntry(("foobar" + i).getBytes(), cb, null);
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new Exception("Entries took too long to add");
        }
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }
        return lh;
    }
}
