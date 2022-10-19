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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.client.api.BKException.Code.NoBookieAvailableException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.MockBookieClient;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Mock-based Client testcases.
 */
public abstract class MockBookKeeperTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MockBookKeeperTestCase.class);

    protected OrderedScheduler scheduler;
    protected OrderedExecutor executor;
    protected BookKeeper bk;
    protected BookieClient bookieClient;
    protected LedgerManager ledgerManager;
    protected LedgerIdGenerator ledgerIdGenerator;

    private BookieWatcher bookieWatcher;

    protected ConcurrentMap<Long, LedgerMetadata> mockLedgerMetadataRegistry;
    protected AtomicLong mockNextLedgerId;
    protected ConcurrentSkipListSet<Long> fencedLedgers;
    protected ConcurrentMap<Long, Map<BookieId, Map<Long, MockEntry>>> mockLedgerData;

    private Map<BookieId, List<Runnable>> deferredBookieForceLedgerResponses;
    private Set<BookieId> suspendedBookiesForForceLedgerAcks;

    List<BookieId> failedBookies;
    Set<BookieId> availableBookies;
    private int lastIndexForBK;
    protected int maxNumberOfAvailableBookies = Integer.MAX_VALUE;

    private Map<BookieId, Map<Long, MockEntry>> getMockLedgerContents(long ledgerId) {
        return mockLedgerData.computeIfAbsent(ledgerId, (id) -> new ConcurrentHashMap<>());
    }

    private Map<Long, MockEntry> getMockLedgerContentsInBookie(long ledgerId, BookieId bookieSocketAddress) {
        return getMockLedgerContents(ledgerId).computeIfAbsent(bookieSocketAddress, addr -> new ConcurrentHashMap<>());
    }

    private MockEntry getMockLedgerEntry(long ledgerId,
                                         BookieId bookieSocketAddress, long entryId) throws BKException{
        if (failedBookies.contains(bookieSocketAddress)) {
            throw BKException.create(NoBookieAvailableException);
        }
        return getMockLedgerContentsInBookie(ledgerId, bookieSocketAddress).get(entryId);
    }

    private static final class MockEntry {

        byte[] payload;
        long lastAddConfirmed;

        public MockEntry(byte[] payload, long lastAddConfirmed) {
            this.payload = payload;
            this.lastAddConfirmed = lastAddConfirmed;
        }

    }

    @Before
    public void setup() throws Exception {
        maxNumberOfAvailableBookies = Integer.MAX_VALUE;
        deferredBookieForceLedgerResponses = new ConcurrentHashMap<>();
        suspendedBookiesForForceLedgerAcks = Collections.synchronizedSet(new HashSet<>());
        mockLedgerMetadataRegistry = new ConcurrentHashMap<>();
        mockLedgerData = new ConcurrentHashMap<>();
        mockNextLedgerId = new AtomicLong(1);
        fencedLedgers = new ConcurrentSkipListSet<>();
        scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(4).name("bk-test").build();
        executor = OrderedExecutor.newBuilder().build();
        bookieWatcher = mock(BookieWatcher.class);

        bookieClient = mock(BookieClient.class);
        ledgerManager = mock(LedgerManager.class);
        ledgerIdGenerator = mock(LedgerIdGenerator.class);
        BookieAddressResolver bookieAddressResolver = BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER;
        when(bookieWatcher.getBookieAddressResolver()).thenReturn(bookieAddressResolver);

        bk = mock(BookKeeper.class);
        doReturn(new ClientConfiguration()).when(bk).getConf();

        failedBookies = new ArrayList<>();
        availableBookies = new HashSet<>();

        when(bk.getCloseLock()).thenReturn(new ReentrantReadWriteLock());
        when(bk.isClosed()).thenReturn(false);
        when(bk.getBookieWatcher()).thenReturn(bookieWatcher);
        when(bk.getBookieAddressResolver()).thenReturn(bookieAddressResolver);
        when(bk.getMainWorkerPool()).thenReturn(executor);
        when(bk.getBookieClient()).thenReturn(bookieClient);
        when(bk.getScheduler()).thenReturn(scheduler);

        setBookKeeperConfig(new ClientConfiguration());
        when(bk.getStatsLogger()).thenReturn(NullStatsLogger.INSTANCE);
        BookKeeperClientStats clientStats = BookKeeperClientStats.newInstance(NullStatsLogger.INSTANCE);
        ClientContext clientCtx = new ClientContext() {
                @Override
                public ClientInternalConf getConf() {
                    return ClientInternalConf.fromConfig(bk.getConf());
                }

                @Override
                public LedgerManager getLedgerManager() {
                    return ledgerManager;
                }

                @Override
                public BookieWatcher getBookieWatcher() {
                    return bookieWatcher;
                }

                @Override
                public EnsemblePlacementPolicy getPlacementPolicy() {
                    return null;
                }

                @Override
                public BookieClient getBookieClient() {
                    return bookieClient;
                }

                @Override
                public OrderedExecutor getMainWorkerPool() {
                    return scheduler;
                }

                @Override
                public OrderedScheduler getScheduler() {
                    return scheduler;
                }

                @Override
                public BookKeeperClientStats getClientStats() {
                    return clientStats;
                }

                @Override
                public boolean isClientClosed() {
                    return bk.isClosed();
                }

                @Override
                public ByteBufAllocator getByteBufAllocator() {
                    return UnpooledByteBufAllocator.DEFAULT;
                }
            };
        when(bk.getClientCtx()).thenReturn(clientCtx);
        when(bk.getLedgerManager()).thenReturn(ledgerManager);
        when(bk.getLedgerIdGenerator()).thenReturn(ledgerIdGenerator);
        when(bk.getReturnRc(anyInt())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
        when(bookieClient.isWritable(any(), anyLong())).thenReturn(true);

        setupLedgerIdGenerator();
        setupCreateLedgerMetadata();
        setupReadLedgerMetadata();
        setupWriteLedgerMetadata();
        setupRemoveLedgerMetadata();
        setupRegisterLedgerMetadataListener();
        setupBookieWatcherForNewEnsemble();
        setupBookieWatcherForEnsembleChange();
        setupBookieClientReadEntry();
        setupBookieClientReadLac();
        setupBookieClientAddEntry();
        setupBookieClientForceLedger();
    }

    protected void setBookKeeperConfig(ClientConfiguration conf) {
        when(bk.getConf()).thenReturn(conf);
    }

    private DigestManager getDigestType(long ledgerId) throws GeneralSecurityException {
        LedgerMetadata metadata = mockLedgerMetadataRegistry.get(ledgerId);
        return DigestManager.instantiate(
                ledgerId,
                metadata.getPassword(),
                org.apache.bookkeeper.client.BookKeeper.DigestType.toProtoDigestType(
                        org.apache.bookkeeper.client.BookKeeper.DigestType.fromApiDigestType(
                                metadata.getDigestType())),
                UnpooledByteBufAllocator.DEFAULT, false);
    }

    @After
    public void tearDown() {
        scheduler.shutdown();
        executor.shutdown();
    }

    protected CreateBuilder newCreateLedgerOp() {
        return new LedgerCreateOp.CreateBuilderImpl(bk);
    }

    protected OpenBuilder newOpenLedgerOp() {
        return new LedgerOpenOp.OpenBuilderImpl(bk);
    }

    protected DeleteBuilder newDeleteLedgerOp() {
        return new LedgerDeleteOp.DeleteBuilderImpl(bk);
    }

    protected void closeBookkeeper() {
        when(bk.isClosed()).thenReturn(true);
    }

    protected void killBookie(BookieId killedBookieSocketAddress) {
        failedBookies.add(killedBookieSocketAddress);
        availableBookies.remove(killedBookieSocketAddress);
    }

    protected void startKilledBookie(BookieId killedBookieSocketAddress) {
        checkState(failedBookies.contains(killedBookieSocketAddress));
        checkState(!availableBookies.contains(killedBookieSocketAddress));
        failedBookies.remove(killedBookieSocketAddress);
        availableBookies.add(killedBookieSocketAddress);
    }

    protected void suspendBookieForceLedgerAcks(BookieId address) {
        suspendedBookiesForForceLedgerAcks.add(address);
    }

    protected void resumeBookieWriteAcks(BookieId address) {
        suspendedBookiesForForceLedgerAcks.remove(address);
        List<Runnable> pendingResponses = deferredBookieForceLedgerResponses.remove(address);
        if (pendingResponses != null) {
            pendingResponses.forEach(Runnable::run);
        }
    }

    protected BookieId startNewBookie() {
        BookieId address = generateBookieSocketAddress(lastIndexForBK++);
        availableBookies.add(address);
        return address;
    }

    protected BookieId generateBookieSocketAddress(int index) {
        return new BookieSocketAddress("localhost", 1111 + index).toBookieId();
    }

    protected ArrayList<BookieId> generateNewEnsemble(int ensembleSize) throws BKException.BKNotEnoughBookiesException {
        LOG.info("generateNewEnsemble {}", ensembleSize);
        if (ensembleSize > maxNumberOfAvailableBookies) {
            throw new BKException.BKNotEnoughBookiesException();
        }
        ArrayList<BookieId> ensemble = new ArrayList<>(ensembleSize);
        for (int i = 0; i < ensembleSize; i++) {
            ensemble.add(generateBookieSocketAddress(i));
        }
        availableBookies.addAll(ensemble);
        lastIndexForBK = ensembleSize;
        return ensemble;
    }

    private void setupBookieWatcherForNewEnsemble() throws BKException.BKNotEnoughBookiesException {
        when(bookieWatcher.newEnsemble(anyInt(), anyInt(), anyInt(), any()))
            .thenAnswer((Answer<ArrayList<BookieId>>) new Answer<ArrayList<BookieId>>() {
                @Override
                @SuppressWarnings("unchecked")
                public ArrayList<BookieId> answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    int ensembleSize = (Integer) args[0];
                    return generateNewEnsemble(ensembleSize);
                }
            });
    }

    private void setupBookieWatcherForEnsembleChange() throws BKException.BKNotEnoughBookiesException {
        when(bookieWatcher.replaceBookie(anyInt(), anyInt(), anyInt(), anyMap(), anyList(), anyInt(), anySet()))
                .thenAnswer((Answer<BookieId>) new Answer<BookieId>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public BookieId answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        List<BookieId> existingBookies = (List<BookieId>) args[4];
                        Set<BookieId> excludeBookies = (Set<BookieId>) args[6];
                        excludeBookies.addAll(existingBookies);
                        Set<BookieId> remainBookies = new HashSet<BookieId>(availableBookies);
                        remainBookies.removeAll(excludeBookies);
                        if (remainBookies.iterator().hasNext()) {
                            return remainBookies.iterator().next();
                        }
                        throw BKException.create(BKException.Code.NotEnoughBookiesException);
                    }
                });
    }

    protected void registerMockEntryForRead(long ledgerId, long entryId, BookieId bookieSocketAddress,
        byte[] entryData, long lastAddConfirmed) {
        getMockLedgerContentsInBookie(ledgerId, bookieSocketAddress).put(entryId, new MockEntry(entryData,
                    lastAddConfirmed));
    }

    protected void registerMockLedgerMetadata(long ledgerId, LedgerMetadata ledgerMetadata) {
        mockLedgerMetadataRegistry.put(ledgerId, ledgerMetadata);
    }

    protected void setNewGeneratedLedgerId(long ledgerId) {
        mockNextLedgerId.set(ledgerId);
        setupLedgerIdGenerator();
    }

    protected LedgerMetadata getLedgerMetadata(long ledgerId) {
        return mockLedgerMetadataRegistry.get(ledgerId);
    }

    @SuppressWarnings("unchecked")
    private void setupReadLedgerMetadata() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Long ledgerId = (Long) args[0];
            CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
            executor.executeOrdered(ledgerId, () -> {
                LedgerMetadata ledgerMetadata = mockLedgerMetadataRegistry.get(ledgerId);
                if (ledgerMetadata == null) {
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                } else {
                    promise.complete(new Versioned<>(ledgerMetadata, new LongVersion(1)));
                }
            });
            return promise;
        }).when(ledgerManager).readLedgerMetadata(anyLong());
    }

    @SuppressWarnings("unchecked")
    private void setupRemoveLedgerMetadata() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Long ledgerId = (Long) args[0];
            CompletableFuture<Void> promise = new CompletableFuture<>();
            executor.executeOrdered(ledgerId, () -> {
                    if (mockLedgerMetadataRegistry.remove(ledgerId) != null) {
                        promise.complete(null);
                    } else {
                        promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsOnMetadataServerException());
                    }
                });
            return promise;
        }).when(ledgerManager).removeLedgerMetadata(anyLong(), any());
    }

    private void setupRegisterLedgerMetadataListener() {
        doAnswer((Answer<Void>) new Answer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer(InvocationOnMock invocation) throws Throwable {
                return null;
            }
        }).when(ledgerManager).registerLedgerMetadataListener(anyLong(), any());
    }

    @SuppressWarnings("unchecked")
    private void setupLedgerIdGenerator() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[0];
            cb.operationComplete(Code.OK, mockNextLedgerId.getAndIncrement());
            return null;
        }).when(ledgerIdGenerator).generateLedgerId(any());
    }

    @SuppressWarnings("unchecked")
    private void setupCreateLedgerMetadata() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Long ledgerId = (Long) args[0];

            CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
            executor.executeOrdered(ledgerId, () -> {

                    LedgerMetadata ledgerMetadata = (LedgerMetadata) args[1];
                    mockLedgerMetadataRegistry.put(ledgerId, ledgerMetadata);
                    promise.complete(new Versioned<>(ledgerMetadata, new LongVersion(1)));
            });
            return promise;
        }).when(ledgerManager).createLedgerMetadata(anyLong(), any());
    }

    @SuppressWarnings("unchecked")
    private void setupWriteLedgerMetadata() {
        doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                Long ledgerId = (Long) args[0];
                LedgerMetadata metadata = (LedgerMetadata) args[1];
                Version currentVersion = (Version) args[2];
                CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
                executor.executeOrdered(ledgerId, () -> {
                        LedgerMetadata newMetadata = LedgerMetadataBuilder.from(metadata).build();
                        mockLedgerMetadataRegistry.put(ledgerId, newMetadata);
                        promise.complete(new Versioned<>(newMetadata, new LongVersion(1234)));
                    });
                return promise;
            }).when(ledgerManager).writeLedgerMetadata(anyLong(), any(), any());
    }

    @SuppressWarnings("unchecked")
    protected void setupBookieClientReadEntry() {
        final Stubber stub = doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookieId bookieSocketAddress = (BookieId) args[0];
            long ledgerId = (Long) args[1];
            long entryId = (Long) args[2];
            BookkeeperInternalCallbacks.ReadEntryCallback callback =
                (BookkeeperInternalCallbacks.ReadEntryCallback) args[3];
            boolean fenced = (((Integer) args[5]) & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING;

            executor.executeOrdered(ledgerId, () -> {
                DigestManager macManager = null;
                try {
                    macManager = getDigestType(ledgerId);
                } catch (GeneralSecurityException gse){
                    LOG.error("Initialize macManager fail", gse);
                }
                MockEntry mockEntry = null;
                try {
                    mockEntry = getMockLedgerEntry(ledgerId, bookieSocketAddress, entryId);
                } catch (BKException bke) {
                    LOG.info("readEntryAndFenceLedger - occur BKException {}@{} at {}", entryId, ledgerId,
                            bookieSocketAddress);
                    callback.readEntryComplete(bke.getCode(), ledgerId, entryId, null, args[5]);
                }

                if (fenced) {
                    fencedLedgers.add(ledgerId);
                }

                if (mockEntry != null) {
                    LOG.info("readEntry - found mock entry {}@{} at {}", entryId, ledgerId, bookieSocketAddress);
                    ReferenceCounted entry = macManager.computeDigestAndPackageForSending(entryId,
                        mockEntry.lastAddConfirmed, mockEntry.payload.length,
                        Unpooled.wrappedBuffer(mockEntry.payload), new byte[20], 0);
                    callback.readEntryComplete(BKException.Code.OK, ledgerId, entryId, MockBookieClient.copyData(entry),
                            args[4]);
                    entry.release();
                } else {
                    LOG.info("readEntry - no such mock entry {}@{} at {}", entryId, ledgerId, bookieSocketAddress);
                    callback.readEntryComplete(BKException.Code.NoSuchEntryException, ledgerId, entryId, null, args[4]);
                }
            });
            return null;
        });

        stub.when(bookieClient).readEntry(any(), anyLong(), anyLong(),
                any(BookkeeperInternalCallbacks.ReadEntryCallback.class),
                any(), anyInt());

        stub.when(bookieClient).readEntry(any(), anyLong(), anyLong(),
                any(BookkeeperInternalCallbacks.ReadEntryCallback.class),
                any(), anyInt(), any());

        stub.when(bookieClient).readEntry(any(), anyLong(), anyLong(),
                any(BookkeeperInternalCallbacks.ReadEntryCallback.class),
                any(), anyInt(), any(), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    protected void setupBookieClientReadLac() {
        final Stubber stub = doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookieId bookieSocketAddress = (BookieId) args[0];
            long ledgerId = (Long) args[1];
            final BookkeeperInternalCallbacks.ReadLacCallback callback =
                (BookkeeperInternalCallbacks.ReadLacCallback) args[2];
            Object ctx = args[3];
            long entryId = BookieProtocol.LAST_ADD_CONFIRMED;
            // simply use "readEntry" with LAST_ADD_CONFIRMED to get current LAC
            // there is nothing that writes ExplicitLAC within MockBookKeeperTestCase
            bookieClient.readEntry(bookieSocketAddress, ledgerId, entryId,
                    new BookkeeperInternalCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
                    callback.readLacComplete(rc, ledgerId, null, buffer, ctx);
                }
            }, ctx, BookieProtocol.FLAG_NONE);
            return null;
        });

        stub.when(bookieClient).readLac(any(BookieId.class), anyLong(),
                any(BookkeeperInternalCallbacks.ReadLacCallback.class),
                any());
    }

    private byte[] extractEntryPayload(long ledgerId, long entryId, ByteBufList toSend)
            throws BKException.BKDigestMatchException {
        ByteBuf toSendCopy = Unpooled.copiedBuffer(toSend.toArray());
        toSendCopy.resetReaderIndex();
        DigestManager macManager = null;
        try {
            macManager = getDigestType(ledgerId);
        } catch (GeneralSecurityException gse){
            LOG.error("Initialize macManager fail", gse);
        }
        ByteBuf content = macManager.verifyDigestAndReturnData(entryId, toSendCopy);
        byte[] entry = new byte[content.readableBytes()];
        content.readBytes(entry);
        content.resetReaderIndex();
        content.release();
        return entry;
    }

    @SuppressWarnings("unchecked")
    protected void setupBookieClientAddEntry() {
        final Stubber stub = doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookkeeperInternalCallbacks.WriteCallback callback = (BookkeeperInternalCallbacks.WriteCallback) args[5];
            BookieId bookieSocketAddress = (BookieId) args[0];
            long ledgerId = (Long) args[1];
            long entryId = (Long) args[3];
            ByteBufList toSend = (ByteBufList) args[4];
            Object ctx = args[6];
            int options = (int) args[7];
            boolean isRecoveryAdd =
                ((short) options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD;

            toSend.retain();
            executor.executeOrdered(ledgerId, () -> {
                byte[] entry;
                try {
                    entry = extractEntryPayload(ledgerId, entryId, toSend);
                } catch (BKDigestMatchException e) {
                    callback.writeComplete(Code.DigestMatchException,
                            ledgerId, entryId, bookieSocketAddress, ctx);
                    toSend.release();
                    return;
                }
                boolean fenced = fencedLedgers.contains(ledgerId);
                if (fenced && !isRecoveryAdd) {
                    callback.writeComplete(BKException.Code.LedgerFencedException,
                        ledgerId, entryId, bookieSocketAddress, ctx);
                } else {
                    if (failedBookies.contains(bookieSocketAddress)) {
                        callback.writeComplete(NoBookieAvailableException,
                                ledgerId, entryId, bookieSocketAddress, ctx);
                        toSend.release();
                        return;
                    }
                    if (getMockLedgerContentsInBookie(ledgerId, bookieSocketAddress).isEmpty()) {
                            registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED,
                                    bookieSocketAddress, new byte[0], BookieProtocol.INVALID_ENTRY_ID);
                    }
                    registerMockEntryForRead(ledgerId, entryId, bookieSocketAddress, entry, ledgerId);
                    callback.writeComplete(BKException.Code.OK,
                            ledgerId, entryId, bookieSocketAddress, ctx);
                }
                toSend.release();
            });

            return null;
        });

        stub.when(bookieClient).addEntry(any(BookieId.class),
                anyLong(), any(byte[].class),
                anyLong(), any(ByteBufList.class),
                any(BookkeeperInternalCallbacks.WriteCallback.class),
                any(), anyInt(), anyBoolean(), any(EnumSet.class));
    }

    @SuppressWarnings("unchecked")
    protected void setupBookieClientForceLedger() {
        final Stubber stub = doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookieId bookieSocketAddress = (BookieId) args[0];
            long ledgerId = (Long) args[1];
            BookkeeperInternalCallbacks.ForceLedgerCallback callback =
                    (BookkeeperInternalCallbacks.ForceLedgerCallback) args[2];
            Object ctx = args[3];

            Runnable activity = () -> {
                executor.executeOrdered(ledgerId, () -> {
                    if (failedBookies.contains(bookieSocketAddress)) {
                        callback.forceLedgerComplete(NoBookieAvailableException, ledgerId, bookieSocketAddress, ctx);
                        return;
                    }
                    callback.forceLedgerComplete(BKException.Code.OK, ledgerId, bookieSocketAddress, ctx);
                });
            };
            if (suspendedBookiesForForceLedgerAcks.contains(bookieSocketAddress)) {
                List<Runnable> queue = deferredBookieForceLedgerResponses.computeIfAbsent(bookieSocketAddress,
                        (k) -> new CopyOnWriteArrayList<>());
                queue.add(activity);
            } else {
                activity.run();
            }
            return null;
        });

        stub.when(bookieClient).forceLedger(any(BookieId.class),
                anyLong(),
                any(BookkeeperInternalCallbacks.ForceLedgerCallback.class),
                any());
    }

}
