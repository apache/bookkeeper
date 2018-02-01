/**
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

import static org.apache.bookkeeper.client.api.BKException.Code.NoBookieAvailableException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Mock-based Client testcases.
 */
public abstract class MockBookKeeperTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MockBookKeeperTestCase.class);

    protected ScheduledExecutorService scheduler;
    protected OrderedSafeExecutor executor;
    protected BookKeeper bk;
    protected BookieClient bookieClient;
    protected LedgerManager ledgerManager;
    protected LedgerIdGenerator ledgerIdGenerator;

    private BookieWatcher bookieWatcher;

    protected ConcurrentMap<Long, LedgerMetadata> mockLedgerMetadataRegistry;
    protected AtomicLong mockNextLedgerId;
    protected ConcurrentSkipListSet<Long> fencedLedgers;
    protected ConcurrentMap<Long, Map<BookieSocketAddress, Map<Long, MockEntry>>> mockLedgerData;

    List<BookieSocketAddress> failedBookies;
    Set<BookieSocketAddress> availableBookies;
    private int lastIndexForBK;

    private Map<BookieSocketAddress, Map<Long, MockEntry>> getMockLedgerContents(long ledgerId) {
        return mockLedgerData.computeIfAbsent(ledgerId, (id) -> new ConcurrentHashMap<>());
    }

    private Map<Long, MockEntry> getMockLedgerContentsInBookie(long ledgerId, BookieSocketAddress bookieSocketAddress) {
        return getMockLedgerContents(ledgerId).computeIfAbsent(bookieSocketAddress, addr -> new ConcurrentHashMap<>());
    }

    private MockEntry getMockLedgerEntry(long ledgerId,
                                         BookieSocketAddress bookieSocketAddress, long entryId) throws BKException{
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
        mockLedgerMetadataRegistry = new ConcurrentHashMap<>();
        mockLedgerData = new ConcurrentHashMap<>();
        mockNextLedgerId = new AtomicLong(1);
        fencedLedgers = new ConcurrentSkipListSet<>();
        scheduler = new ScheduledThreadPoolExecutor(4);
        executor = OrderedSafeExecutor.newBuilder().build();
        bookieWatcher = mock(BookieWatcher.class);

        bookieClient = mock(BookieClient.class);
        ledgerManager = mock(LedgerManager.class);
        ledgerIdGenerator = mock(LedgerIdGenerator.class);

        bk = mock(BookKeeper.class);

        NullStatsLogger nullStatsLogger = setupLoggers();

        failedBookies = new ArrayList<>();
        availableBookies = new HashSet<>();

        when(bk.getCloseLock()).thenReturn(new ReentrantReadWriteLock());
        when(bk.isClosed()).thenReturn(false);
        when(bk.getBookieWatcher()).thenReturn(bookieWatcher);
        when(bk.getDisableEnsembleChangeFeature()).thenReturn(mock(Feature.class));
        when(bk.getExplicitLacInterval()).thenReturn(0);
        when(bk.getMainWorkerPool()).thenReturn(executor);
        when(bk.getBookieClient()).thenReturn(bookieClient);
        when(bk.getScheduler()).thenReturn(scheduler);
        when(bk.getReadSpeculativeRequestPolicy()).thenReturn(Optional.absent());
        when(bk.getConf()).thenReturn(new ClientConfiguration());
        when(bk.getStatsLogger()).thenReturn(nullStatsLogger);
        when(bk.getLedgerManager()).thenReturn(ledgerManager);
        when(bk.getLedgerIdGenerator()).thenReturn(ledgerIdGenerator);
        when(bk.getReturnRc(anyInt())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

        setupLedgerIdGenerator();
        setupCreateLedgerMetadata();
        setupReadLedgerMetadata();
        setupWriteLedgerMetadata();
        setupRemoveLedgerMetadata();
        setupRegisterLedgerMetadataListener();
        setupBookieWatcherForNewEnsemble();
        setupBookieWatcherForEnsembleChange();
        setupBookieClientReadEntry();
        setupBookieClientAddEntry();
    }

    protected NullStatsLogger setupLoggers() {
        NullStatsLogger nullStatsLogger = NullStatsLogger.INSTANCE;
        when(bk.getOpenOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getRecoverOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getAddOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getReadOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getDeleteOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getCreateOpLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getRecoverAddCountLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        when(bk.getRecoverReadCountLogger()).thenReturn(nullStatsLogger.getOpStatsLogger("mock"));
        return nullStatsLogger;
    }

    @After
    public void tearDown() {
        scheduler.shutdown();
        executor.shutdown();
    }

    protected void setBookkeeperConfig(ClientConfiguration config) {
        when(bk.getConf()).thenReturn(config);
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

    protected void killBookie(BookieSocketAddress killedBookieSocketAddress) {
        failedBookies.add(killedBookieSocketAddress);
        availableBookies.remove(killedBookieSocketAddress);
    }

    protected BookieSocketAddress startNewBookie() {
        BookieSocketAddress address = generateBookieSocketAddress(lastIndexForBK++);
        availableBookies.add(address);
        return address;
    }

    protected BookieSocketAddress generateBookieSocketAddress(int index) {
        return new BookieSocketAddress("localhost", 1111 + index);
    }

    protected ArrayList<BookieSocketAddress> generateNewEnsemble(int ensembleSize) {
        ArrayList<BookieSocketAddress> ensemble = new ArrayList<>(ensembleSize);
        for (int i = 0; i < ensembleSize; i++) {
            ensemble.add(generateBookieSocketAddress(i));
        }
        availableBookies.addAll(ensemble);
        lastIndexForBK = ensembleSize;
        return ensemble;
    }

    private void setupBookieWatcherForNewEnsemble() throws BKException.BKNotEnoughBookiesException {
        when(bookieWatcher.newEnsemble(anyInt(), anyInt(), anyInt(), any()))
            .thenAnswer((Answer<ArrayList<BookieSocketAddress>>) new Answer<ArrayList<BookieSocketAddress>>() {
                @Override
                @SuppressWarnings("unchecked")
                public ArrayList<BookieSocketAddress> answer(InvocationOnMock invocation) throws Throwable {
                    Object[] args = invocation.getArguments();
                    int ensembleSize = (Integer) args[0];
                    return generateNewEnsemble(ensembleSize);
                }
            });
    }

    private void setupBookieWatcherForEnsembleChange() throws BKException.BKNotEnoughBookiesException {
        when(bookieWatcher.replaceBookie(anyInt(), anyInt(), anyInt(), anyMap(), anyList(), anyInt(), anySet()))
                .thenAnswer((Answer<BookieSocketAddress>) new Answer<BookieSocketAddress>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public BookieSocketAddress answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        List<BookieSocketAddress> existingBookies = (List<BookieSocketAddress>) args[4];
                        Set<BookieSocketAddress> excludeBookies = (Set<BookieSocketAddress>) args[6];
                        excludeBookies.addAll(existingBookies);
                        Set<BookieSocketAddress> remainBookies = new HashSet<BookieSocketAddress>(availableBookies);
                        remainBookies.removeAll(excludeBookies);
                        if (remainBookies.iterator().hasNext()) {
                            return remainBookies.iterator().next();
                        }
                        throw BKException.create(BKException.Code.NotEnoughBookiesException);
                    }
                });
    }
    private void submit(Runnable operation) {
        try {
            scheduler.submit(operation);
        } catch (RejectedExecutionException rejected) {
            operation.run();
        }
    }

    protected void registerMockEntryForRead(long ledgerId, long entryId, BookieSocketAddress bookieSocketAddress,
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
            executor.submitOrdered(ledgerId, () -> {
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[1];
                LedgerMetadata ledgerMetadata = mockLedgerMetadataRegistry.get(ledgerId);
                if (ledgerMetadata == null) {
                    cb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                } else {
                    cb.operationComplete(BKException.Code.OK, new LedgerMetadata(ledgerMetadata));
                }
            });
            return null;
        }).when(ledgerManager).readLedgerMetadata(anyLong(), any());
    }

    @SuppressWarnings("unchecked")
    private void setupRemoveLedgerMetadata() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Long ledgerId = (Long) args[0];
            executor.submitOrdered(ledgerId, () -> {
                BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
                if (mockLedgerMetadataRegistry.remove(ledgerId) != null) {
                    cb.operationComplete(BKException.Code.OK, null);
                } else {
                    cb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                }
            });
            return null;
        }).when(ledgerManager).removeLedgerMetadata(anyLong(), any(), any());
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
            BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
            Long ledgerId = (Long) args[0];
            executor.submitOrdered(ledgerId, () -> {
                LedgerMetadata ledgerMetadata = (LedgerMetadata) args[1];
                mockLedgerMetadataRegistry.put(ledgerId, new LedgerMetadata(ledgerMetadata));
                cb.operationComplete(BKException.Code.OK, null);
            });
            return null;
        }).when(ledgerManager).createLedgerMetadata(anyLong(), any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setupWriteLedgerMetadata() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Long ledgerId = (Long) args[0];
            LedgerMetadata metadata = (LedgerMetadata) args[1];
            BookkeeperInternalCallbacks.GenericCallback cb = (BookkeeperInternalCallbacks.GenericCallback) args[2];
            executor.submitOrdered(ledgerId, () -> {
                mockLedgerMetadataRegistry.put(ledgerId, new LedgerMetadata(metadata));
                cb.operationComplete(BKException.Code.OK, null);
            });
            return null;
        }).when(ledgerManager).writeLedgerMetadata(anyLong(), any(), any());
    }

    @SuppressWarnings("unchecked")
    protected void setupBookieClientReadEntry() {
        doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookkeeperInternalCallbacks.ReadEntryCallback callback =
                (BookkeeperInternalCallbacks.ReadEntryCallback) args[4];
            BookieSocketAddress bookieSocketAddress = (BookieSocketAddress) args[0];
            long ledgerId = (Long) args[1];
            long entryId = (Long) args[3];

            executor.submitOrdered(ledgerId, () -> {
                DigestManager macManager = null;
                try {
                    macManager = DigestManager.instantiate(ledgerId, new byte[2], DigestType.CRC32);
                } catch (GeneralSecurityException gse){
                    LOG.error("Initialize macManager fail", gse);
                }
                fencedLedgers.add(ledgerId);
                MockEntry mockEntry = null;
                try {
                    mockEntry = getMockLedgerEntry(ledgerId, bookieSocketAddress, entryId);
                } catch (BKException bke) {
                    LOG.info("readEntryAndFenceLedger - occur BKException {}@{} at {}", entryId, ledgerId,
                            bookieSocketAddress);
                    callback.readEntryComplete(bke.getCode(), ledgerId, entryId, null, args[5]);
                }
                if (mockEntry != null) {
                    LOG.info("readEntryAndFenceLedger - found mock entry {}@{} at {}", entryId, ledgerId,
                            bookieSocketAddress);
                    ByteBuf entry = macManager.computeDigestAndPackageForSending(entryId, mockEntry.lastAddConfirmed,
                        mockEntry.payload.length, Unpooled.wrappedBuffer(mockEntry.payload));
                    callback.readEntryComplete(BKException.Code.OK, ledgerId, entryId, Unpooled.copiedBuffer(entry),
                            args[5]);
                    entry.release();
                } else {
                    LOG.info("readEntryAndFenceLedger - no such mock entry {}@{} at {}", entryId, ledgerId,
                            bookieSocketAddress);
                    callback.readEntryComplete(BKException.Code.NoSuchEntryException, ledgerId, entryId, null, args[5]);
                }
            });
            return null;
        }).when(bookieClient).readEntryAndFenceLedger(any(), anyLong(), any(), anyLong(),
            any(BookkeeperInternalCallbacks.ReadEntryCallback.class), any());

        doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookieSocketAddress bookieSocketAddress = (BookieSocketAddress) args[0];
            long ledgerId = (Long) args[1];
            long entryId = (Long) args[2];
            BookkeeperInternalCallbacks.ReadEntryCallback callback =
                (BookkeeperInternalCallbacks.ReadEntryCallback) args[3];

            executor.submitOrdered(ledgerId, () -> {
                DigestManager macManager = null;
                try {
                    macManager = DigestManager.instantiate(ledgerId, new byte[2], DigestType.CRC32);
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
                if (mockEntry != null) {
                    LOG.info("readEntry - found mock entry {}@{} at {}", entryId, ledgerId, bookieSocketAddress);
                    ByteBuf entry = macManager.computeDigestAndPackageForSending(entryId,
                        mockEntry.lastAddConfirmed, mockEntry.payload.length,
                        Unpooled.wrappedBuffer(mockEntry.payload));
                    callback.readEntryComplete(BKException.Code.OK, ledgerId, entryId, Unpooled.copiedBuffer(entry),
                            args[4]);
                    entry.release();
                } else {
                    LOG.info("readEntry - no such mock entry {}@{} at {}", entryId, ledgerId, bookieSocketAddress);
                    callback.readEntryComplete(BKException.Code.NoSuchEntryException, ledgerId, entryId, null, args[4]);
                }
            });
            return null;
        }).when(bookieClient).readEntry(any(), anyLong(), anyLong(),
            any(BookkeeperInternalCallbacks.ReadEntryCallback.class), any());
    }

    private static byte[] extractEntryPayload(long ledgerId, long entryId, ByteBuf toSend)
            throws BKException.BKDigestMatchException {
        ByteBuf toSendCopy = Unpooled.copiedBuffer(toSend);
        toSendCopy.resetReaderIndex();
        DigestManager macManager = null;
        try {
            macManager = DigestManager.instantiate(ledgerId, new byte[2], DigestType.CRC32);
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
        doAnswer(invokation -> {
            Object[] args = invokation.getArguments();
            BookkeeperInternalCallbacks.WriteCallback callback = (BookkeeperInternalCallbacks.WriteCallback) args[5];
            BookieSocketAddress bookieSocketAddress = (BookieSocketAddress) args[0];
            long ledgerId = (Long) args[1];
            long entryId = (Long) args[3];
            ByteBuf toSend = (ByteBuf) args[4];
            Object ctx = args[6];
            int options = (int) args[7];
            boolean isRecoveryAdd =
                ((short) options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD;

            executor.submitOrdered(ledgerId, () -> {
                byte[] entry;
                try {
                    entry = extractEntryPayload(ledgerId, entryId, toSend);
                } catch (BKDigestMatchException e) {
                    callback.writeComplete(Code.DigestMatchException, ledgerId, entryId, bookieSocketAddress, ctx);
                    return;
                }
                boolean fenced = fencedLedgers.contains(ledgerId);
                if (fenced && !isRecoveryAdd) {
                    callback.writeComplete(BKException.Code.LedgerFencedException,
                        ledgerId, entryId, bookieSocketAddress, ctx);
                } else {
                    if (failedBookies.contains(bookieSocketAddress)) {
                        callback.writeComplete(NoBookieAvailableException, ledgerId, entryId, bookieSocketAddress, ctx);
                        return;
                    }
                    if (getMockLedgerContentsInBookie(ledgerId, bookieSocketAddress).isEmpty()) {
                            registerMockEntryForRead(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED, bookieSocketAddress,
                                    new byte[0], BookieProtocol.INVALID_ENTRY_ID);
                    }
                    registerMockEntryForRead(ledgerId, entryId, bookieSocketAddress, entry, ledgerId);
                    callback.writeComplete(BKException.Code.OK, ledgerId, entryId, bookieSocketAddress, ctx);
                }
            });
            return null;
        }).when(bookieClient).addEntry(any(BookieSocketAddress.class),
            anyLong(), any(byte[].class),
            anyLong(), any(ByteBuf.class),
            any(BookkeeperInternalCallbacks.WriteCallback.class),
            any(), anyInt());
    }

}
