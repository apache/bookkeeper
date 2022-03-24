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

package org.apache.bookkeeper.bookie.datainteg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;

import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.exceptions.CompositeException;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerStorage.StorageState;
import org.apache.bookkeeper.bookie.MockLedgerStorage;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MockTicker;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MockLedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.MockBookieClient;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test of DataIntegrityCheckImpl.
 */
@SuppressWarnings("deprecation")
public class DataIntegrityCheckTest {
    private static final byte[] PASSWD = new byte[0];

    private final BookieId bookie1 = BookieId.parse("bookie1:3181");
    private final BookieId bookie2 = BookieId.parse("bookie2:3181");
    private final BookieId bookie3 = BookieId.parse("bookie3:3181");
    private final BookieId bookie4 = BookieId.parse("bookie4:3181");
    private final BookieId bookie5 = BookieId.parse("bookie5:3181");

    private OrderedExecutor executor = null;

    @Before
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("test").build();
    }

    @After
    public void teardown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static ServerConfiguration serverConf() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAdvertisedAddress("foobar");
        return conf;
    }

    private LedgerMetadataBuilder newMetadataWithEnsemble(
            long ledgerId,
            BookieId... bookies) {
        return LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(bookies.length)
            .withWriteQuorumSize(bookies.length)
            .withAckQuorumSize(bookies.length)
            .newEnsembleEntry(0, Lists.newArrayList(bookies));
    }

    private LedgerMetadataBuilder newClosedMetadataWithEnsemble(long ledgerId,
                                                                long numEntries,
                                                                BookieId... bookies) {
        return LedgerMetadataBuilder.create()
            .withId(ledgerId)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(bookies.length)
            .withWriteQuorumSize(bookies.length)
            .withAckQuorumSize(bookies.length)
            .newEnsembleEntry(0, Lists.newArrayList(bookies))
            .withLastEntryId(numEntries - 1)
            .withLength(128 * numEntries)
            .withClosedState();
    }

    @Test
    public void testPrebootBookieIdInOpenSegmentMarkedInLimbo() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();

        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        lm.createLedgerMetadata(0xbeefL, newMetadataWithEnsemble(0xbeefL, bookieId).build()).get();

        MockLedgerStorage storage = new MockLedgerStorage();
        assertThat(storage.ledgerExists(0xbeefL), is(false));
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(BookieImpl.getBookieId(conf), lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        impl.runPreBootCheck("test").get();

        assertThat(storage.hasLimboState(0xbeefL), is(true));
        assertThat(storage.isFenced(0xbeefL), is(true));
    }

    @Test
    public void testPrebootFencedMarkedInLimbo() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();

        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        lm.createLedgerMetadata(0xbeefL,
                newMetadataWithEnsemble(0xbeefL,
                        BookieImpl.getBookieId(conf)).withInRecoveryState().build()).get();

        MockLedgerStorage storage = new MockLedgerStorage();
        assertThat(storage.ledgerExists(0xbeefL), is(false));

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        impl.runPreBootCheck("test").get();

        assertThat(storage.hasLimboState(0xbeefL), is(true));
        assertThat(storage.isFenced(0xbeefL), is(true));
    }

    @Test
    public void testPrebootClosedNotMarkedInLimbo() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();

        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        lm.createLedgerMetadata(0xbeefL,
                newMetadataWithEnsemble(0xbeefL, BookieImpl.getBookieId(conf)).withClosedState()
                .withLength(100).withLastEntryId(1).build()).get();

        MockLedgerStorage storage = new MockLedgerStorage();
        assertThat(storage.ledgerExists(0xbeefL), is(false));

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        impl.runPreBootCheck("test").get();

        assertThat(storage.hasLimboState(0xbeefL), is(false));
        assertThat(storage.isFenced(0xbeefL), is(false));
    }

    @Test
    public void testPrebootFlushCalled() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();

        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        lm.createLedgerMetadata(0xbeefL, newMetadataWithEnsemble(
                0xbeefL, BookieImpl.getBookieId(conf)).build()).get();

        MockLedgerStorage storage = spy(new MockLedgerStorage());
        assertThat(storage.ledgerExists(0xbeefL), is(false));

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        verify(storage, times(0)).flush();
        impl.runPreBootCheck("test").get();
        verify(storage, times(1)).flush();

        assertThat(storage.hasLimboState(0xbeefL), is(true));
        assertThat(storage.isFenced(0xbeefL), is(true));
    }

    @Test(expected = ExecutionException.class)
    public void testFailureInPrebootMarkFailsAll() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();

        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        lm.createLedgerMetadata(0xbeedL, newMetadataWithEnsemble(0xbeedL, bookieId).build()).get();
        lm.createLedgerMetadata(0xbeefL, newMetadataWithEnsemble(0xbeefL, bookieId).build()).get();
        lm.createLedgerMetadata(0xbee0L, newMetadataWithEnsemble(0xbee0L, bookieId).build()).get();

        MockLedgerStorage storage = new MockLedgerStorage() {
                @Override
                public void setLimboState(long ledgerId) throws IOException {
                    if (ledgerId == 0xbeefL) {
                        throw new IOException("boom!");
                    } else {
                        super.setLimboState(ledgerId);
                    }
                }
            };

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        impl.runPreBootCheck("test").get();
    }

    @Test
    public void testRecoverLimboOpensAndClears() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.just(newClosedMetadataWithEnsemble(ledgerId, -1, bookieId, bookie1).build());
                }
            };

        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        ledgers.put(0xf00L, newMetadataWithEnsemble(0xf00L, bookieId, bookie1).build());
        storage.setMasterKey(0xf00L, PASSWD);
        storage.setLimboState(0xf00L);
        ledgers.put(0xdeadL, newMetadataWithEnsemble(0xdeadL, bookieId, bookie1).build());
        storage.setMasterKey(0xdeadL, PASSWD);
        storage.setLimboState(0xdeadL);

        Set<DataIntegrityCheckImpl.LedgerResult> results = impl.checkAndRecoverLedgers(
                ledgers, "test").get();

        assertThat(results.stream().filter(r -> r.isOK()).count(), equalTo(2L));
        verify(storage, times(1)).clearLimboState(0xf00L);
        verify(storage, times(1)).clearLimboState(0xdeadL);
    }

    @Test
    public void testRecoverLimboErrorOnOpenOnlyAffectsThatOne() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    if (ledgerId == 0xf00L) {
                        return Single.error(new BKException.BKReadException());
                    } else {
                        return Single.just(newClosedMetadataWithEnsemble(ledgerId, 0, bookieId, bookie1).build());
                    }
                }
            };

        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        ledgers.put(0xf00L, newMetadataWithEnsemble(0xf00L, bookieId, bookie1).build());
        storage.setMasterKey(0xf00L, PASSWD);
        storage.setLimboState(0xf00L);
        ledgers.put(0xdeadL, newMetadataWithEnsemble(0xdeadL, bookieId, bookie1).build());
        storage.setMasterKey(0xdeadL, PASSWD);
        storage.setLimboState(0xdeadL);

        Set<DataIntegrityCheckImpl.LedgerResult> results = impl.checkAndRecoverLedgers(ledgers, "test").get();

        assertThat(results.stream().filter(r -> r.isOK()).count(), equalTo(1L));
        assertThat(results.stream().filter(r -> r.isOK()).map(r -> r.getLedgerId()).findFirst().get(),
                   equalTo(0xdeadL));
        assertThat(results.stream().filter(r -> r.isError()).count(), equalTo(1L));
        assertThat(results.stream().filter(r -> r.isError()).map(r -> r.getLedgerId()).findFirst().get(),
                   equalTo(0xf00L));

        verify(storage, times(0)).clearLimboState(0xf00L);
        verify(storage, times(1)).clearLimboState(0xdeadL);
    }

    @Test
    public void testRecoverLimboNoSuchLedger() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    if (ledgerId == 0xdeadL) {
                        return Single.error(
                                new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    } else {
                        return Single.just(newClosedMetadataWithEnsemble(ledgerId, -1, bookieId, bookie1).build());
                    }
                }
            };

        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        ledgers.put(0xf00L, newMetadataWithEnsemble(0xf00L, bookieId, bookie1).build());
        storage.setMasterKey(0xf00L, PASSWD);
        storage.setLimboState(0xf00L);
        ledgers.put(0xdeadL, newMetadataWithEnsemble(0xdeadL, bookieId, bookie1).build());
        storage.setMasterKey(0xdeadL, PASSWD);
        storage.setLimboState(0xdeadL);

        Set<DataIntegrityCheckImpl.LedgerResult> results = impl.checkAndRecoverLedgers(ledgers, "test").get();

        assertThat(results.stream().filter(r -> r.isOK()).count(), equalTo(1L));
        assertThat(results.stream().filter(r -> r.isOK()).map(r -> r.getLedgerId()).findFirst().get(),
                   equalTo(0xf00L));
        assertThat(results.stream().filter(r -> r.isMissing()).count(), equalTo(1L));
        assertThat(results.stream().filter(r -> r.isMissing()).map(r -> r.getLedgerId()).findFirst().get(),
                   equalTo(0xdeadL));

        verify(storage, times(1)).clearLimboState(0xf00L);
        verify(storage, times(0)).clearLimboState(0xdeadL);
    }

    @Test
    public void testRecoverLimboClearStateFailure() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public void clearLimboState(long ledgerId) throws IOException {
                    if (ledgerId == 0xf00L) {
                        throw new IOException("foobar");
                    }
                }
            });
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.just(newClosedMetadataWithEnsemble(ledgerId, -1, bookieId, bookie1).build());
                }
            };
        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        ledgers.put(0xf00L, newMetadataWithEnsemble(0xf00L, bookieId, bookie1).build());
        storage.setMasterKey(0xf00L, PASSWD);
        storage.setLimboState(0xf00L);
        ledgers.put(0xdeadL, newMetadataWithEnsemble(0xdeadL, bookieId, bookie1).build());
        storage.setMasterKey(0xdeadL, PASSWD);
        storage.setLimboState(0xdeadL);

        Set<DataIntegrityCheckImpl.LedgerResult> results = impl.checkAndRecoverLedgers(ledgers, "test").get();

        verify(storage, times(0)).flush();
    }

    // TODO: what is this test?
//    @Test
//    public void testRecoverLimboFlushFailure() throws Exception {
//        MockLedgerManager lm = new MockLedgerManager();
//        ServerConfiguration conf = serverConf();
//        BookieId bookieId = BookieImpl.getBookieId(conf);
//        MockLedgerStorage storage = spy(new MockLedgerStorage() {
//                @Override
//                public void flush() throws IOException {
//                    throw new IOException("foobar");
//                }
//            });
//        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
//                                                                 mock(EntryCopier.class),
//                                                                 mock(BookKeeperAdmin.class),
//                                                                 Schedulers.io()) {
//                @Override
//                CompletableFuture<Long> recoverLedgerIgnoreMissing(long ledgerId) {
//                    return CompletableFuture.completedFuture(ledgerId);
//                }
//            };
//        Set<Long> ledgers = Sets.newHashSet(0xf00L, 0xdeadL);
//
//        try {
//            impl.recoverLedgersInLimbo(ledgers).get();
//            Assert.fail("Shouldn't continue on an IOException");
//        } catch (ExecutionException ee) {
//            assertThat(ee.getCause(), instanceOf(IOException.class));
//        }
//        assertThat(results.stream().filter(r -> r.isOK()).count(), equalTo(1L));
//        assertThat(results.stream().filter(r -> r.isOK()).map(r -> r.getLedgerId()).findFirst().get(),
//                   equalTo(0xdeadL));
//        assertThat(results.stream().filter(r -> r.isError()).count(), equalTo(1L));
//        assertThat(results.stream().filter(r -> r.isError()).map(r -> r.getLedgerId()).findFirst().get(),
//                   equalTo(0xf00L));
//
//        verify(storage, times(1)).clearLimboState(0xf00L);
//        verify(storage, times(1)).clearLimboState(0xdeadL);
//    }

    @Test
    public void testRecoverLimboManyLedgers() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        List<Long> cleared = new ArrayList<>();
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public void clearLimboState(long ledgerId) {
                    // not using spy for this because it takes 10ms per ledger to verify
                    cleared.add(ledgerId);
                }
            });
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.just(newClosedMetadataWithEnsemble(ledgerId, -1, bookieId, bookie1).build());
                }
            };
        final long numLedgers = 10000;
        long first = 1;
        long last = first + numLedgers;

        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        for (long i = first; i < last; i++) {
            LedgerMetadata metadata = newMetadataWithEnsemble(i, bookieId, bookie1).build();
            ledgers.put(i, metadata);
            storage.setMasterKey(i, metadata.getPassword());
            storage.setLimboState(i);
        }
        assertThat(ledgers.size(), equalTo((int) numLedgers));

        Set<DataIntegrityCheckImpl.LedgerResult> results = impl.checkAndRecoverLedgers(ledgers, "test").get();
        assertThat(results.size(), equalTo((int) numLedgers));
        assertThat(results.stream().filter(r -> r.isOK()).count(), equalTo(numLedgers));
        for (DataIntegrityCheckImpl.LedgerResult r : results) {
            assertThat(r.isOK(), equalTo(true));
            ledgers.remove(r.getLedgerId());
        }
        assertThat(ledgers.isEmpty(), equalTo(true));

        Set<Long> clearedSet = Sets.newHashSet(cleared);
        assertThat(clearedSet.size(), equalTo(cleared.size()));
        for (long l : LongStream.range(first, last).toArray()) {
            assertThat(l, isIn(clearedSet));
        }
        verify(storage, times(10000)).clearLimboState(anyLong());
    }

    @Test
    public void testRecoverLimboManyLedgersErrorOnFirst() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        List<Long> cleared = new ArrayList<>();
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public void clearLimboState(long ledgerId) {
                    // not using spy for this because it takes 10ms per ledger to verify
                    cleared.add(ledgerId);
                }
            });

        final long numLedgers = 100;
        long first = 1;
        long last = first + numLedgers;

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    if (ledgerId == first) {
                        return Single.error(
                                new BKException.BKBookieHandleNotAvailableException());
                    } else {
                        return Single.just(newClosedMetadataWithEnsemble(ledgerId, -1, bookieId, bookie1).build());
                    }
                }
            };
        Map<Long, LedgerMetadata> ledgers = new HashMap<>();
        for (long i = first; i < last; i++) {
            LedgerMetadata metadata = newMetadataWithEnsemble(i, bookieId, bookie1).build();
            ledgers.put(i, metadata);
            storage.setMasterKey(i, metadata.getPassword());
            storage.setLimboState(i);
        }
        assertThat(ledgers.size(), equalTo((int) numLedgers));

        Set<DataIntegrityCheckImpl.LedgerResult> results = impl.checkAndRecoverLedgers(ledgers, "test").get();
        assertThat(results.size(), equalTo((int) numLedgers));
        assertThat(results.stream().filter(r -> r.isOK()).count(), equalTo(numLedgers - 1));
        assertThat(results.stream().filter(r -> r.isError()).count(), equalTo(1L));
        assertThat(results.stream().filter(r -> r.isError()).map(r -> r.getLedgerId()).findFirst().get(),
                   equalTo(first));
        Set<Long> clearedSet = Sets.newHashSet(cleared);
        assertThat(clearedSet.size(), equalTo(cleared.size()));
        for (long l : LongStream.range(first, last).toArray()) {
            if (l == first) {
                assertThat(l, not(isIn(clearedSet)));
            } else {
                assertThat(l, isIn(clearedSet));
            }
        }
        verify(storage, times((int) numLedgers - 1)).clearLimboState(anyLong());
    }

    @Test
    public void testRecoverLimboNoLedgers() throws Exception {
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        BookieId bookieId = BookieImpl.getBookieId(conf);
        List<Long> cleared = new ArrayList<>();
        MockLedgerStorage storage = spy(new MockLedgerStorage());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookieId, lm, storage,
                                                                 mock(EntryCopier.class),
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.just(newClosedMetadataWithEnsemble(ledgerId, -1, bookieId, bookie1).build());
                }
            };
        ImmutableMap<Long, LedgerMetadata> ledgers = ImmutableMap.of();
        Set<DataIntegrityCheckImpl.LedgerResult> resolved =
            impl.checkAndRecoverLedgers(ledgers, "test").get(10, TimeUnit.SECONDS);
        assertThat(resolved.isEmpty(), equalTo(true));
        verify(storage, times(0)).clearLimboState(anyLong());
    }


    @Test
    public void testRecoverSingleLedgerEntriesOnLedgerIDontHave() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie3, bookie2).build();
        bookieClient.getMockBookies().seedLedger(id1, metadata1);

        assertThat(storage.ledgerExists(id1), equalTo(false));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        assertThat(storage.ledgerExists(id1), equalTo(true)); // because we passed it in
        for (long i = 0; i <= metadata1.getLastEntryId(); i++) {
            assertThat(storage.entryExists(id1, i), equalTo(false));
        }
    }

    @Test
    public void testRecoverSingleLedgerNotClosedOneEnsemble() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newMetadataWithEnsemble(id1, bookie1, bookie2).build();
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);

        assertThat(storage.ledgerExists(id1), equalTo(false));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        LedgerMetadata md1 = newMetadataWithEnsemble(id1, bookie1).build();
        assertThat(storage.ledgerExists(id1), equalTo(false));
    }

    @Test
    public void testRecoverSingleLedgerNoClosedMultiEnsembleBookieInClosed() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newMetadataWithEnsemble(id1, bookie1, bookie2)
            .newEnsembleEntry(10L, Lists.newArrayList(bookie3, bookie2)).build();
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id1, metadata1);

        assertThat(storage.ledgerExists(id1), equalTo(false));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        for (long e = 0; e < 10; e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
        }
        assertThat(storage.entryExists(id1, 10), equalTo(false));
    }

    @Test
    public void testRecoverSingleLedgerNotClosedMultiEnsembleBookieInFinal() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newMetadataWithEnsemble(id1, bookie3, bookie2)
            .newEnsembleEntry(10L, Lists.newArrayList(bookie1, bookie2)).build();
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id1, metadata1);

        assertThat(storage.ledgerExists(id1), equalTo(false));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        assertThat(storage.ledgerExists(id1), equalTo(true));
    }

    @Test
    public void testRecoverSingleLedgerLargeEnsembleStriped() throws Exception {

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie4, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie4, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = LedgerMetadataBuilder.create()
            .withId(id1)
            .withPassword(new byte[0])
            .withDigestType(DigestType.CRC32C)
            .withEnsembleSize(5)
            .withWriteQuorumSize(2)
            .withAckQuorumSize(2)
            .newEnsembleEntry(0, Lists.newArrayList(bookie1, bookie2, bookie3, bookie4, bookie5))
            .withClosedState().withLastEntryId(10).withLength(1000)
            .build();
        bookieClient.getMockBookies().seedLedgerForBookie(bookie1, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie5, id1, metadata1);

        assertThat(storage.ledgerExists(id1), equalTo(false));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.entryExists(id1, 0), equalTo(false));
        assertThat(storage.entryExists(id1, 1), equalTo(false));
        assertThat(storage.entryExists(id1, 2), equalTo(true));
        assertThat(storage.entryExists(id1, 3), equalTo(true));
        assertThat(storage.entryExists(id1, 4), equalTo(false));
        assertThat(storage.entryExists(id1, 5), equalTo(false));
        assertThat(storage.entryExists(id1, 6), equalTo(false));
        assertThat(storage.entryExists(id1, 7), equalTo(true));
        assertThat(storage.entryExists(id1, 8), equalTo(true));
        assertThat(storage.entryExists(id1, 9), equalTo(false));
        assertThat(storage.entryExists(id1, 10), equalTo(false));
    }

    @Test
    public void testRecoverSingleLedgerEntriesOnlyEntriesNeeded() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie3, bookie2)
            .newEnsembleEntry(10, Lists.newArrayList(bookie1, bookie2))
            .newEnsembleEntry(100, Lists.newArrayList(bookie3, bookie2)).build();
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id1, metadata1);

        assertThat(storage.ledgerExists(id1), equalTo(false));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.entryExists(id1, 9), equalTo(false));
        for (long e = 10; e < 100; e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
        }
        assertThat(storage.entryExists(id1, 100), equalTo(false));
    }

    @Test
    public void testRecoverSingleLedgerEntriesOnlyEntriesNeededEverySecond() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        long added = 0;
        storage.setMasterKey(id1, PASSWD);
        for (long e = 0; e <= metadata1.getLastEntryId(); e++) {
            if (e % 2 == 0) {
                storage.addEntry(bookieClient.getMockBookies().generateEntry(id1, e, e - 1));
                added++;
            }
        }
        assertThat(storage.ledgerExists(id1), equalTo(true));

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertNoErrors();

        for (long e = 0; e <= metadata1.getLastEntryId(); e++) {
            if (e % 2 == 0) {
                verify(bookieClient, times(0)).readEntry(anyObject(), eq(id1), eq(e),
                                                         anyObject(), anyObject(), anyInt());
            }
            if (e % 2 == 1) {
                verify(bookieClient, times(1)).readEntry(anyObject(), eq(id1), eq(e),
                                                         anyObject(), anyObject(), anyInt());
            }

            assertThat(storage.entryExists(id1, e), equalTo(true));
        }
    }

    @Test
    public void testRecoverSingleLedgerErrorAtStart() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();

        // only seed for ledger1 & ledger3
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.setPreReadHook((bookie, ledger, entry) -> {
                if (entry == 0L) {
                    return FutureUtils.exception(new BKException.BKReadException());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertError((t) -> {
                return t instanceof BKException.BKReadException;
            });
        assertThat(storage.entryExists(id1, 0), equalTo(false));
        for (long e = 1; e <= metadata1.getLastEntryId(); e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
        }
    }

    @Test
    public void testRecoverSingleLedgerErrorEverySecond() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();

        // only seed for ledger1 & ledger3
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.setPreReadHook((bookie, ledger, entry) -> {
                if (entry % 2 == 0) {
                    return FutureUtils.exception(new BKException.BKReadException());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertError((t) -> {
                if (t instanceof CompositeException) {
                    CompositeException e = (CompositeException) t;
                    for (Throwable t2 : e.getExceptions()) {
                        if (!(t2 instanceof BKException.BKReadException)) {
                            return false;
                        }
                    }
                    return e.getExceptions().size() == 500;
                } else {
                    return false;
                }
            });
        for (long e = 0; e <= metadata1.getLastEntryId(); e++) {
            if (e % 2 == 0) {
                assertThat(storage.entryExists(id1, e), equalTo(false));
            } else {
                assertThat(storage.entryExists(id1, e), equalTo(true));
            }
        }
    }

    @Test
    public void testRecoverSingleLedgerErrorOneOnStore() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public long addEntry(ByteBuf entry) throws IOException, BookieException {
                    long entryId = extractEntryId(entry);
                    if (entryId > 10 && entryId <= 100) {
                        throw new IOException("Don't feel like storing these");
                    }
                    return super.addEntry(entry);
                }
            });

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        // only seed for ledger1 & ledger3
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);

        TestObserver<Long> observer = TestObserver.create();
        impl.checkAndRecoverLedgerEntries(id1, metadata1, "test").subscribe(observer);
        observer.await().assertError((t) -> {
                if (t instanceof CompositeException) {
                    CompositeException e = (CompositeException) t;
                    for (Throwable t2 : e.getExceptions()) {
                        boolean failStore = t2 instanceof IOException;
                        if (!failStore) {
                            return false;
                        }
                    }
                    return e.getExceptions().size() == 90;
                } else {
                    return false;
                }
            });
        for (long e = 0; e <= 10; e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
        }
        for (long e = 11; e <= 100; e++) {
            assertThat(storage.entryExists(id1, e), equalTo(false));
        }
        for (long e = 101; e <= metadata1.getLastEntryId(); e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
        }
    }

    @Test
    public void testRecoverMultiLedgers() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3);

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));
        Map<Long, LedgerMetadata> ledgers = ImmutableMap.of(
                id1, metadata1, id2, metadata2, id3, metadata3);
        Set<DataIntegrityCheckImpl.LedgerResult> resolved =
            impl.checkAndRecoverLedgers(ledgers, "test").get(10, TimeUnit.SECONDS);
        assertThat(resolved.stream().filter(r -> r.isOK()).count(), equalTo(3L));
        assertThat(resolved.stream().filter(r -> r.isOK()).map(r -> r.getLedgerId())
                   .collect(Collectors.toSet()), containsInAnyOrder(id1, id2, id3));
        for (long e = 0; e <= metadata1.getLastEntryId(); e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
            assertThat(storage.entryExists(id2, e), equalTo(true));
            assertThat(storage.entryExists(id3, e), equalTo(true));
        }
    }

    @Test
    public void testRecoverMultiLedgersOneUnavailable() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        // id2 will be unavailable because there's no entries
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3);

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));

        Map<Long, LedgerMetadata> ledgers = ImmutableMap.of(
                id1, metadata1, id2, metadata2, id3, metadata3);
        Set<DataIntegrityCheckImpl.LedgerResult> resolved =
            impl.checkAndRecoverLedgers(ledgers, "test").get(10, TimeUnit.SECONDS);
        assertThat(resolved.stream().filter(r -> r.isOK()).count(), equalTo(2L));
        assertThat(resolved.stream().filter(r -> r.isError()).count(), equalTo(1L));
        assertThat(resolved.stream().filter(r -> r.isOK()).map(r -> r.getLedgerId())
                   .collect(Collectors.toSet()), containsInAnyOrder(id1, id3));
        for (long e = 0; e <= metadata1.getLastEntryId(); e++) {
            assertThat(storage.entryExists(id1, e), equalTo(true));
            assertThat(storage.entryExists(id3, e), equalTo(true));
        }
    }

    @Test
    public void testRecoverMultiLedgersOneFailsToWriteLocally() throws Exception {
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;

        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public long addEntry(ByteBuf entry) throws IOException, BookieException {
                    if (extractLedgerId(entry) == id1
                        && extractEntryId(entry) == 3) {
                        throw new IOException("Don't feel like storing this");
                    }
                    return super.addEntry(entry);
                }
            });

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3);

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));

        Map<Long, LedgerMetadata> ledgers = ImmutableMap.of(
                id1, metadata1, id2, metadata2, id3, metadata3);

        Set<DataIntegrityCheckImpl.LedgerResult> resolved =
            impl.checkAndRecoverLedgers(ledgers, "test").get(10, TimeUnit.SECONDS);
        assertThat(resolved.stream().filter(r -> r.isOK()).count(), equalTo(2L));
        assertThat(resolved.stream().filter(r -> r.isOK())
                   .map(r -> r.getLedgerId()).collect(Collectors.toSet()),
                   containsInAnyOrder(id2, id3));
        assertThat(resolved.stream().filter(r -> r.isError())
                   .map(r -> r.getLedgerId()).collect(Collectors.toSet()),
                   containsInAnyOrder(id1));

        for (long e = 0; e <= metadata1.getLastEntryId(); e++) {
            assertThat(storage.entryExists(id1, e), equalTo(e != 3));
            assertThat(storage.entryExists(id2, e), equalTo(true));
            assertThat(storage.entryExists(id3, e), equalTo(true));
        }
    }

    @Test
    public void testRecoverMultiLedgersAllUnavailable() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));

        Map<Long, LedgerMetadata> ledgers = ImmutableMap.of(
                id1, metadata1, id2, metadata2, id3, metadata3);

        Set<DataIntegrityCheckImpl.LedgerResult> resolved =
            impl.checkAndRecoverLedgers(ledgers, "test").get(10, TimeUnit.SECONDS);
        assertThat(resolved.stream().filter(r -> r.isOK()).count(), equalTo(0L));
        assertThat(resolved.stream().filter(r -> r.isError()).count(), equalTo(3L));
        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.entryExists(id1, 0), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(true));
        assertThat(storage.entryExists(id2, 0), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(true));
        assertThat(storage.entryExists(id3, 0), equalTo(false));
    }

    @Test
    public void testEnsemblesContainBookie() throws Exception {
        LedgerMetadata md1 = newMetadataWithEnsemble(1, bookie1).build();
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md1, bookie1), equalTo(true));
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md1, bookie2), equalTo(false));
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md1, bookie3), equalTo(false));

        LedgerMetadata md2 = newMetadataWithEnsemble(2, bookie1, bookie2)
            .newEnsembleEntry(1, Lists.newArrayList(bookie2, bookie3)).build();
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md2, bookie1), equalTo(true));
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md2, bookie2), equalTo(true));
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md2, bookie3), equalTo(true));

        LedgerMetadata md3 = newMetadataWithEnsemble(3, bookie1, bookie2)
            .newEnsembleEntry(1, Lists.newArrayList(bookie2, bookie1)).build();
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md3, bookie1), equalTo(true));
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md3, bookie2), equalTo(true));
        assertThat(DataIntegrityCheckImpl.ensemblesContainBookie(md3, bookie3), equalTo(false));
    }

    @Test
    public void testMetadataCacheLoad() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        Map<Long, LedgerMetadata> ledgers = impl.getCachedOrReadMetadata("test").get();
        assertThat(ledgers.keySet(), containsInAnyOrder(id1, id2, id3));
    }

    @Test
    public void testFullCheckCacheLoadAndProcessIfEmpty() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3);

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));

        impl.runFullCheck().get();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.ledgerExists(id2), equalTo(true));
        assertThat(storage.ledgerExists(id3), equalTo(true));
    }

    @Test
    public void testFullCheckCacheLoadAndProcessSomeInLimbo() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newMetadataWithEnsemble(id3, bookie1, bookie3).build();
        LedgerMetadata metadata3closed = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.just(metadata3closed);
                }
            };

        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3closed);

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));
        storage.setMasterKey(id3, PASSWD);
        storage.setLimboState(id3);
        assertThat(storage.hasLimboState(id3), equalTo(true));

        storage.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));

        impl.runFullCheck().get();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   not(isIn(storage.getStorageStateFlags())));

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.ledgerExists(id2), equalTo(true));
        assertThat(storage.ledgerExists(id3), equalTo(true));
        assertThat(storage.hasLimboState(id3), equalTo(false));
    }

    @Test
    public void testFullCheckInLimboRecoveryFailsFirstTime() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 1000, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 1000, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newMetadataWithEnsemble(id3, bookie1, bookie3).build();
        LedgerMetadata metadata3closed = newClosedMetadataWithEnsemble(id3, 1000, bookie1, bookie3).build();

        AtomicInteger callCount = new AtomicInteger(0);
        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    if (callCount.getAndIncrement() == 0) {
                        return Single.error(new BKException.BKReadException());
                    } else {
                        return Single.just(metadata3closed);
                    }
                }
            };

        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3closed);

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));
        storage.setMasterKey(id3, PASSWD);
        storage.setLimboState(id3);
        assertThat(storage.hasLimboState(id3), equalTo(true));

        storage.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));

        impl.runFullCheck().get();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));
        verify(storage, times(1)).flush();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.ledgerExists(id2), equalTo(true));
        assertThat(storage.ledgerExists(id3), equalTo(true));
        assertThat(storage.entryExists(id3, 0), equalTo(false));
        assertThat(storage.hasLimboState(id3), equalTo(true));

        // run again, second time shouldn't error
        impl.runFullCheck().get();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   not(isIn(storage.getStorageStateFlags())));
        verify(storage, times(2)).flush();

        assertThat(storage.ledgerExists(id3), equalTo(true));
        assertThat(storage.entryExists(id3, 0), equalTo(true));
        assertThat(storage.hasLimboState(id3), equalTo(false));
    }

    @Test
    public void testFullCheckInEntryCopyFailsFirstTime() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 100, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 100, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newMetadataWithEnsemble(id3, bookie1, bookie3).build();
        LedgerMetadata metadata3closed = newClosedMetadataWithEnsemble(id3, 100, bookie1, bookie3).build();

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.just(metadata3closed);
                }
            };

        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));
        storage.setMasterKey(id3, PASSWD);
        storage.setLimboState(id3);
        assertThat(storage.hasLimboState(id3), equalTo(true));

        storage.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));

        impl.runFullCheck().get();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));
        verify(storage, times(1)).flush();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.ledgerExists(id2), equalTo(true));
        assertThat(storage.ledgerExists(id3), equalTo(true));
        assertThat(storage.entryExists(id3, 0), equalTo(false));
        assertThat(storage.hasLimboState(id3), equalTo(false));

        // make it possible to recover the ledger by seeding bookie3
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3closed);

        // run again, second time shouldn't error
        impl.runFullCheck().get();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   not(isIn(storage.getStorageStateFlags())));
        verify(storage, times(2)).flush();

        assertThat(storage.ledgerExists(id3), equalTo(true));
        assertThat(storage.entryExists(id3, 0), equalTo(true));
        assertThat(storage.hasLimboState(id3), equalTo(false));
    }


    @Test
    public void testFullCheckAllInLimboAndMissing() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        MockLedgerStorage storage = spy(new MockLedgerStorage());

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newMetadataWithEnsemble(id1, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newMetadataWithEnsemble(id2, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newMetadataWithEnsemble(id3, bookie1, bookie3).build();

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io()) {
                @Override
                Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
                    return Single.error(
                            new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                }
            };

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));
        storage.setMasterKey(id1, PASSWD);
        storage.setLimboState(id1);
        storage.setMasterKey(id2, PASSWD);
        storage.setLimboState(id2);
        storage.setMasterKey(id3, PASSWD);
        storage.setLimboState(id3);
        assertThat(storage.hasLimboState(id1), equalTo(true));
        assertThat(storage.hasLimboState(id2), equalTo(true));
        assertThat(storage.hasLimboState(id3), equalTo(true));

        storage.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));

        impl.runFullCheck().get();

        verify(storage, times(1)).flush();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   not(isIn(storage.getStorageStateFlags())));
    }

    @Test
    public void testFullCheckFailFlushRetainsFlag() throws Exception {
        MockBookieClient bookieClient = spy(new MockBookieClient(executor));
        MockLedgerManager lm = new MockLedgerManager();
        ServerConfiguration conf = serverConf();
        AtomicInteger count = new AtomicInteger(0);
        MockLedgerStorage storage = spy(new MockLedgerStorage() {
                @Override
                public void flush() throws IOException {
                    if (count.getAndIncrement() == 0) {
                        throw new IOException("broken flush");
                    }
                }
            });

        EntryCopier copier = new EntryCopierImpl(bookie1, bookieClient, storage, new MockTicker());
        long id1 = 0xdeadL;
        long id2 = 0xbedeL;
        long id3 = 0xbebeL;
        LedgerMetadata metadata1 = newClosedMetadataWithEnsemble(id1, 100, bookie1, bookie2).build();
        LedgerMetadata metadata2 = newClosedMetadataWithEnsemble(id2, 100, bookie1, bookie3).build();
        LedgerMetadata metadata3 = newClosedMetadataWithEnsemble(id3, 100, bookie1, bookie3).build();

        DataIntegrityCheckImpl impl = new DataIntegrityCheckImpl(bookie1, lm, storage,
                                                                 copier,
                                                                 mock(BookKeeperAdmin.class),
                                                                 Schedulers.io());
        bookieClient.getMockBookies().seedLedgerForBookie(bookie2, id1, metadata1);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id2, metadata2);
        bookieClient.getMockBookies().seedLedgerForBookie(bookie3, id3, metadata3);

        lm.createLedgerMetadata(id1, metadata1).get();
        lm.createLedgerMetadata(id2, metadata2).get();
        lm.createLedgerMetadata(id3, metadata3).get();

        assertThat(storage.ledgerExists(id1), equalTo(false));
        assertThat(storage.ledgerExists(id2), equalTo(false));
        assertThat(storage.ledgerExists(id3), equalTo(false));

        storage.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        try {
            impl.runFullCheck().get();
            Assert.fail("Should have failed on flush");
        } catch (ExecutionException ee) {
            assertThat(ee.getCause(), instanceOf(IOException.class));
        }
        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   isIn(storage.getStorageStateFlags()));
        verify(storage, times(1)).flush();

        assertThat(storage.ledgerExists(id1), equalTo(true));
        assertThat(storage.ledgerExists(id2), equalTo(true));
        assertThat(storage.ledgerExists(id3), equalTo(true));

        // run again, second time shouldn't error
        impl.runFullCheck().get();

        assertThat(StorageState.NEEDS_INTEGRITY_CHECK,
                   not(isIn(storage.getStorageStateFlags())));
        verify(storage, times(2)).flush();
    }
}

