/*
 *
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
 *
 */
package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test parallel ledger recovery.
 */
public class ParallelLedgerRecoveryTest extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(ParallelLedgerRecoveryTest.class);

    static class TestLedgerManager implements LedgerManager {

        final LedgerManager lm;
        volatile CountDownLatch waitLatch = null;
        final ExecutorService executorService;

        TestLedgerManager(LedgerManager lm) {
            this.lm = lm;
            this.executorService = Executors.newSingleThreadExecutor();
        }

        void setLatch(CountDownLatch waitLatch) {
            this.waitLatch = waitLatch;
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(
                long ledgerId, LedgerMetadata metadata) {
            return lm.createLedgerMetadata(ledgerId, metadata);
        }

        @Override
        public CompletableFuture<Void> removeLedgerMetadata(long ledgerId, Version version) {
            return lm.removeLedgerMetadata(ledgerId, version);
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> readLedgerMetadata(long ledgerId) {
            return lm.readLedgerMetadata(ledgerId);
        }

        @Override
        public LedgerRangeIterator getLedgerRanges(long zkOpTimeoutMs) {
            return lm.getLedgerRanges(zkOpTimeoutMs);
        }

        @Override
        public CompletableFuture<Versioned<LedgerMetadata>> writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                                                Version currentVersion) {
            final CountDownLatch cdl = waitLatch;
            if (null != cdl) {
                CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cdl.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            LOG.error("Interrupted on waiting latch : ", e);
                        }
                        lm.writeLedgerMetadata(ledgerId, metadata, currentVersion)
                            .whenComplete((metadata, exception) -> {
                                    if (exception != null) {
                                        promise.completeExceptionally(exception);
                                    } else {
                                        promise.complete(metadata);
                                    }
                                });
                    }
                });
                return promise;
            } else {
                return lm.writeLedgerMetadata(ledgerId, metadata, currentVersion);
            }
        }

        @Override
        public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
            lm.registerLedgerMetadataListener(ledgerId, listener);
        }

        @Override
        public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
            lm.unregisterLedgerMetadataListener(ledgerId, listener);
        }

        @Override
        public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context,
                                        int successRc, int failureRc) {
            lm.asyncProcessLedgers(processor, finalCb, context, successRc, failureRc);
        }

        @Override
        public void close() throws IOException {
            lm.close();
            executorService.shutdown();
        }
    }

    static class TestLedgerManagerFactory extends HierarchicalLedgerManagerFactory {
        @Override
        public LedgerManager newLedgerManager() {
            return new TestLedgerManager(super.newLedgerManager());
        }
    }

    static class TestMetadataClientDriver extends ZKMetadataClientDriver {

        @Override
        public synchronized LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
            if (null == lmFactory) {
                try {
                    lmFactory = new TestLedgerManagerFactory()
                        .initialize(conf, layoutManager, TestLedgerManagerFactory.CUR_VERSION);
                } catch (IOException e) {
                    throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
                }
            }
            return lmFactory;
        }
    }

    static class TestMetadataBookieDriver extends ZKMetadataBookieDriver {

        @Override
        public synchronized LedgerManagerFactory getLedgerManagerFactory() throws MetadataException {
            if (null == lmFactory) {
                try {
                    lmFactory = new TestLedgerManagerFactory()
                        .initialize(conf, layoutManager, TestLedgerManagerFactory.CUR_VERSION);
                } catch (IOException e) {
                    throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
                }
            }
            return lmFactory;
        }
    }

    final DigestType digestType;

    public ParallelLedgerRecoveryTest() throws Exception {
        super(3);

        this.digestType = DigestType.CRC32;
    }

    @Override
    protected void startBKCluster(String metadataServiceUri) throws Exception {
        MetadataDrivers.registerClientDriver("zk", TestMetadataClientDriver.class, true);
        MetadataDrivers.registerBookieDriver("zk", TestMetadataBookieDriver.class, true);
        baseConf.setLedgerManagerFactoryClass(TestLedgerManagerFactory.class);
        baseClientConf.setLedgerManagerFactoryClass(TestLedgerManagerFactory.class);
        baseClientConf.setReadEntryTimeout(60000);
        baseClientConf.setAddEntryTimeout(60000);

        super.startBKCluster(metadataServiceUri);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            MetadataDrivers.registerClientDriver("zk", ZKMetadataClientDriver.class, true);
            MetadataDrivers.registerBookieDriver("zk", ZKMetadataBookieDriver.class, true);
        }
    }

    @Test
    public void testRecoverBeforeWriteMetadata1() throws Exception {
        rereadDuringRecovery(true, 1, false, false);
    }

    @Test
    public void testRecoverBeforeWriteMetadata2() throws Exception {
        rereadDuringRecovery(true, 3, false, false);
    }

    @Test
    public void testRecoverBeforeWriteMetadata3() throws Exception {
        rereadDuringRecovery(false, 1, false, false);
    }

    @Test
    public void testRecoverBeforeWriteMetadata4() throws Exception {
        rereadDuringRecovery(false, 3, false, false);
    }

    @Test
    public void testRereadDuringRecovery1() throws Exception {
        rereadDuringRecovery(true, 1, true, false);
    }

    @Test
    public void testRereadDuringRecovery2() throws Exception {
        rereadDuringRecovery(true, 3, true, false);
    }

    @Test
    public void testRereadDuringRecovery3() throws Exception {
        rereadDuringRecovery(false, 1, true, false);
    }

    @Test
    public void testRereadDuringRecovery4() throws Exception {
        rereadDuringRecovery(false, 3, true, false);
    }

    @Test
    public void testConcurrentRecovery1() throws Exception {
        rereadDuringRecovery(true, 1, true, false);
    }

    @Test
    public void testConcurrentRecovery2() throws Exception {
        rereadDuringRecovery(true, 3, true, false);
    }

    @Test
    public void testConcurrentRecovery3() throws Exception {
        rereadDuringRecovery(false, 1, true, false);
    }

    @Test
    public void testConcurrentRecovery4() throws Exception {
        rereadDuringRecovery(false, 3, true, false);
    }

    private void rereadDuringRecovery(boolean parallelRead, int batchSize,
                                      boolean updateMetadata, boolean close) throws Exception {
        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setEnableParallelRecoveryRead(parallelRead);
        newConf.setRecoveryReadBatchSize(batchSize);
        BookKeeper newBk = new BookKeeper(newConf);

        TestLedgerManager tlm = (TestLedgerManager) newBk.getUnderlyingLedgerManager();

        final LedgerHandle lh = newBk.createLedger(numBookies, 2, 2, digestType, "".getBytes());
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        sleepBookie(lh.getCurrentEnsemble().get(0), latch1);
        sleepBookie(lh.getCurrentEnsemble().get(1), latch2);

        int numEntries = (numBookies * 3) + 1;
        final AtomicInteger numPendingAdds = new AtomicInteger(numEntries);
        final CountDownLatch addDone = new CountDownLatch(1);
        for (int i = 0; i < numEntries; i++) {
            lh.asyncAddEntry(("" + i).getBytes(), new org.apache.bookkeeper.client.AsyncCallback.AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (BKException.Code.OK != rc) {
                        addDone.countDown();
                        return;
                    }
                    if (numPendingAdds.decrementAndGet() == 0) {
                        addDone.countDown();
                    }
                }
            }, null);
        }
        latch1.countDown();
        latch2.countDown();
        addDone.await(10, TimeUnit.SECONDS);
        assertEquals(0, numPendingAdds.get());

        LOG.info("Added {} entries to ledger {}.", numEntries, lh.getId());

        long ledgerLenth = lh.getLength();

        LedgerHandle recoverLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(BookieProtocol.INVALID_ENTRY_ID, recoverLh.getLastAddPushed());
        assertEquals(BookieProtocol.INVALID_ENTRY_ID, recoverLh.getLastAddConfirmed());
        assertEquals(0, recoverLh.getLength());

        LOG.info("OpenLedgerNoRecovery {}.", lh.getId());

        final CountDownLatch metadataLatch = new CountDownLatch(1);

        tlm.setLatch(metadataLatch);

        final CountDownLatch recoverLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        ((ReadOnlyLedgerHandle) recoverLh).recover(new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                LOG.info("Recovering ledger {} completed : {}.", lh.getId(), rc);
                success.set(BKException.Code.OK == rc);
                recoverLatch.countDown();
            }
        });

        // clear the metadata latch
        tlm.setLatch(null);

        if (updateMetadata) {
            if (close) {
                LOG.info("OpenLedger {} to close.", lh.getId());
                LedgerHandle newRecoverLh = newBk.openLedger(lh.getId(), digestType, "".getBytes());
                newRecoverLh.close();
            } else {
                LOG.info("OpenLedgerNoRecovery {} again.", lh.getId());
                LedgerHandle newRecoverLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
                assertEquals(BookieProtocol.INVALID_ENTRY_ID, newRecoverLh.getLastAddPushed());
                assertEquals(BookieProtocol.INVALID_ENTRY_ID, newRecoverLh.getLastAddConfirmed());

                // mark the ledger as in recovery to update version.
                ClientUtil.transformMetadata(newBk.getClientCtx(), newRecoverLh.getId(),
                        (metadata) -> LedgerMetadataBuilder.from(metadata).withInRecoveryState().build());

                newRecoverLh.close();
                LOG.info("Updated ledger manager {}.", newRecoverLh.getLedgerMetadata());
            }
        }

        // resume metadata operation on recoverLh
        metadataLatch.countDown();

        LOG.info("Resume metadata update.");

        // wait until recover completed
        recoverLatch.await(20, TimeUnit.SECONDS);
        assertTrue(success.get());
        assertEquals(numEntries - 1, recoverLh.getLastAddPushed());
        assertEquals(numEntries - 1, recoverLh.getLastAddConfirmed());
        assertEquals(ledgerLenth, recoverLh.getLength());
        assertTrue(recoverLh.getLedgerMetadata().isClosed());

        Enumeration<LedgerEntry> enumeration = recoverLh.readEntries(0, numEntries - 1);
        int numReads = 0;
        while (enumeration.hasMoreElements()) {
            LedgerEntry entry = enumeration.nextElement();
            assertEquals((long) numReads, entry.getEntryId());
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        recoverLh.close();
        newBk.close();
    }

    @Test
    public void testRecoveryOnEntryGap() throws Exception {
        byte[] passwd = "recovery-on-entry-gap".getBytes(UTF_8);
        LedgerHandle lh = bkc.createLedger(1, 1, 1, DigestType.CRC32, passwd);
        for (int i = 0; i < 10; i++) {
            lh.addEntry(("recovery-on-entry-gap-" + i).getBytes(UTF_8));
        }

        // simulate ledger writer failure on concurrent writes causing gaps

        long entryId = 14;
        long lac = 8;
        byte[] data = "recovery-on-entry-gap-gap".getBytes(UTF_8);
        ReferenceCounted toSend =
                lh.macManager.computeDigestAndPackageForSending(
                        entryId, lac, lh.getLength() + 100, Unpooled.wrappedBuffer(data, 0, data.length),
                        new byte[20], 0);
        final CountDownLatch addLatch = new CountDownLatch(1);
        final AtomicBoolean addSuccess = new AtomicBoolean(false);
        LOG.info("Add entry {} with lac = {}", entryId, lac);

        bkc.getBookieClient().addEntry(lh.getCurrentEnsemble().get(0),
                                       lh.getId(), lh.ledgerKey, entryId, toSend,
                                       new WriteCallback() {
                                           @Override
                                           public void writeComplete(int rc, long ledgerId, long entryId,
                                                                     BookieId addr, Object ctx) {
                                               addSuccess.set(BKException.Code.OK == rc);
                                               addLatch.countDown();
                                           }
                                       }, 0, BookieProtocol.FLAG_NONE, false, WriteFlag.NONE);
        addLatch.await();
        assertTrue("add entry 14 should succeed", addSuccess.get());

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setEnableParallelRecoveryRead(true);
        newConf.setRecoveryReadBatchSize(10);

        BookKeeper newBk = new BookKeeper(newConf);

        final LedgerHandle recoverLh =
                newBk.openLedgerNoRecovery(lh.getId(), DigestType.CRC32, passwd);

        assertEquals("wrong lac found", 8L, recoverLh.getLastAddConfirmed());

        final CountDownLatch recoverLatch = new CountDownLatch(1);
        final AtomicLong newLac = new AtomicLong(-1);
        final AtomicBoolean isMetadataClosed = new AtomicBoolean(false);
        final AtomicInteger numSuccessCalls = new AtomicInteger(0);
        final AtomicInteger numFailureCalls = new AtomicInteger(0);
        ((ReadOnlyLedgerHandle) recoverLh).recover(new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (BKException.Code.OK == rc) {
                    newLac.set(recoverLh.getLastAddConfirmed());
                    isMetadataClosed.set(recoverLh.getLedgerMetadata().isClosed());
                    numSuccessCalls.incrementAndGet();
                } else {
                    numFailureCalls.incrementAndGet();
                }
                recoverLatch.countDown();
            }
        });
        recoverLatch.await();
        assertEquals("wrong lac found", 9L, newLac.get());
        assertTrue("metadata isn't closed after recovery", isMetadataClosed.get());
        Thread.sleep(5000);
        assertEquals("recovery callback should be triggered only once", 1, numSuccessCalls.get());
        assertEquals("recovery callback should be triggered only once", 0, numFailureCalls.get());
    }

    static class DelayResponseBookie extends TestBookieImpl {

        static final class WriteCallbackEntry {

            private final WriteCallback cb;
            private final int rc;
            private final long ledgerId;
            private final long entryId;
            private final BookieId addr;
            private final Object ctx;

            WriteCallbackEntry(WriteCallback cb,
                               int rc, long ledgerId, long entryId,
                               BookieId addr, Object ctx) {
                this.cb = cb;
                this.rc = rc;
                this.ledgerId = ledgerId;
                this.entryId = entryId;
                this.addr = addr;
                this.ctx = ctx;
            }

            public void callback() {
                cb.writeComplete(rc, ledgerId, entryId, addr, ctx);
            }
        }

        private final AtomicBoolean delayAddResponse = new AtomicBoolean(false);
        private final AtomicBoolean delayReadResponse = new AtomicBoolean(false);
        private final AtomicLong delayReadOnEntry = new AtomicLong(-1234L);
        private volatile CountDownLatch delayReadLatch = null;
        private final LinkedBlockingQueue<WriteCallbackEntry> delayQueue =
                new LinkedBlockingQueue<WriteCallbackEntry>();

        public DelayResponseBookie(ServerConfiguration conf)
                throws Exception {
            super(conf);
        }

        @Override
        public void addEntry(ByteBuf entry, boolean ackBeforeSync, final WriteCallback cb, Object ctx, byte[] masterKey)
                throws IOException, BookieException, InterruptedException {
            super.addEntry(entry, ackBeforeSync, new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId,
                                          BookieId addr, Object ctx) {
                    if (delayAddResponse.get()) {
                        delayQueue.add(new WriteCallbackEntry(cb, rc, ledgerId, entryId, addr, ctx));
                    } else {
                        cb.writeComplete(rc, ledgerId, entryId, addr, ctx);
                    }
                }
            }, ctx, masterKey);
        }

        @Override
        public ByteBuf readEntry(long ledgerId, long entryId) throws IOException, NoLedgerException, BookieException {
            LOG.info("ReadEntry {} - {}", ledgerId, entryId);
            if (delayReadResponse.get() && delayReadOnEntry.get() == entryId) {
                CountDownLatch latch = delayReadLatch;
                if (null != latch) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // no-op
                    }
                }
            }
            return super.readEntry(ledgerId, entryId);
        }

        void delayAdd(boolean delayed) {
            this.delayAddResponse.set(delayed);
        }

        void delayRead(boolean delayed, long entryId, CountDownLatch delayReadLatch) {
            this.delayReadResponse.set(delayed);
            this.delayReadOnEntry.set(entryId);
            this.delayReadLatch = delayReadLatch;
        }

    }

    @Test
    public void testRecoveryWhenClosingLedgerHandle() throws Exception {
        byte[] passwd = "recovery-when-closing-ledger-handle".getBytes(UTF_8);

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setEnableParallelRecoveryRead(true);
        newConf.setRecoveryReadBatchSize(1);
        newConf.setAddEntryTimeout(9999999);
        newConf.setReadEntryTimeout(9999999);

        final BookKeeper newBk0 = new BookKeeper(newConf);
        final LedgerHandle lh0 = newBk0.createLedger(1, 1, 1, digestType, passwd);

        final BookKeeper newBk1 = new BookKeeper(newConf);
        final LedgerHandle lh1 = newBk1.openLedgerNoRecovery(lh0.getId(), digestType, passwd);
        final TestLedgerManager tlm1 = (TestLedgerManager) newBk1.getUnderlyingLedgerManager();

        final BookKeeper readBk = new BookKeeper(newConf);
        final LedgerHandle readLh = readBk.openLedgerNoRecovery(lh0.getId(), digestType, passwd);

        LOG.info("Create ledger {}", lh0.getId());

        // 0) place the bookie with a fake bookie
        BookieId address = lh0.getCurrentEnsemble().get(0);
        ServerConfiguration conf = killBookie(address);
        conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
        DelayResponseBookie fakeBookie = new DelayResponseBookie(conf);
        startAndAddBookie(conf, fakeBookie);

        // 1) bk0 write two entries
        lh0.addEntry("entry-0".getBytes(UTF_8));
        lh0.addEntry("entry-1".getBytes(UTF_8));

        // 2) readBk read last add confirmed
        long lac = readLh.readLastConfirmed();
        assertEquals(0L, lac);
        lac = lh1.readLastConfirmed();
        assertEquals(0L, lac);

        final CountDownLatch addLatch = new CountDownLatch(3);
        final AtomicInteger numAddFailures = new AtomicInteger(0);
        // 3) bk0 write more entries in parallel
        fakeBookie.delayAdd(true);
        for (int i = 2; i < 5; i++) {
            lh0.asyncAddEntry(("entry-" + i).getBytes(UTF_8), new AsyncCallback.AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (BKException.Code.OK != rc) {
                        numAddFailures.incrementAndGet();
                    }
                    addLatch.countDown();
                }
            }, null);
        }
        while (fakeBookie.delayQueue.size() < 3) {
            // wait until all add requests are queued
            Thread.sleep(100);
        }

        // 4) lac moved to 1L
        lac = readLh.readLastConfirmed();
        assertEquals(1L, lac);
        lac = lh1.readLastConfirmed();
        assertEquals(1L, lac);

        // 5) bk1 is doing recovery, but the metadata update is delayed
        final CountDownLatch readLatch = new CountDownLatch(1);
        fakeBookie.delayAdd(false);
        fakeBookie.delayRead(true, 3L, readLatch);
        final CountDownLatch metadataLatch = new CountDownLatch(1);
        tlm1.setLatch(metadataLatch);
        final CountDownLatch recoverLatch = new CountDownLatch(1);
        final AtomicBoolean recoverSuccess = new AtomicBoolean(false);
        ((ReadOnlyLedgerHandle) lh1).recover(new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                LOG.info("Recovering ledger {} completed : {}", lh1.getId(), rc);
                recoverSuccess.set(BKException.Code.OK == rc);
                recoverLatch.countDown();
            }
        });
        Thread.sleep(2000);
        readLatch.countDown();

        // we don't expected lac being updated before we successfully marked the ledger in recovery
        lac = readLh.readLastConfirmed();
        assertEquals(1L, lac);

        // 6) bk0 closes ledger before bk1 marks in recovery
        lh0.close();
        assertEquals(1L, lh0.getLastAddConfirmed());

        // 7) bk1 proceed recovery and succeed
        metadataLatch.countDown();
        recoverLatch.await();
        assertTrue(recoverSuccess.get());
        assertEquals(1L, lh1.getLastAddConfirmed());

        // 8) make sure we won't see lac advanced during ledger is closed by bk0 and recovered by bk1
        final AtomicLong lacHolder = new AtomicLong(-1234L);
        final AtomicInteger rcHolder = new AtomicInteger(-1234);
        final CountDownLatch doneLatch = new CountDownLatch(1);

        new ReadLastConfirmedOp(bkc.getBookieClient(),
                                readLh.distributionSchedule,
                                readLh.macManager,
                                readLh.ledgerId,
                                readLh.getLedgerMetadata().getAllEnsembles().lastEntry().getValue(),
                                readLh.ledgerKey,
                new ReadLastConfirmedOp.LastConfirmedDataCallback() {
                    @Override
                    public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                        rcHolder.set(rc);
                        lacHolder.set(data.getLastAddConfirmed());
                        doneLatch.countDown();
                    }
                }).initiate();
        doneLatch.await();
        assertEquals(BKException.Code.OK, rcHolder.get());
        assertEquals(1L, lacHolder.get());

        newBk0.close();
        newBk1.close();
        readBk.close();
    }

    /**
     * Validate ledger can recover with response: (Qw - Qa)+1.
     * @throws Exception
     */
    @Test
    public void testRecoveryWithUnavailableBookie() throws Exception {

        byte[] passwd = "".getBytes(UTF_8);
        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        final BookKeeper readBk = new BookKeeper(newConf);
        final BookKeeper newBk0 = new BookKeeper(newConf);

        /**
         * Test Group-1 : Expected Response for recovery: Qr = (Qw - Qa)+1 = (3
         * -2) + 1 = 2
         */
        int ensembleSize = 3;
        int writeQuorumSize = 3;
        int ackQuormSize = 2;
        LedgerHandle lh0 = newBk0.createLedger(ensembleSize, writeQuorumSize, ackQuormSize, DigestType.DUMMY, passwd);
        LedgerHandle readLh = readBk.openLedgerNoRecovery(lh0.getId(), DigestType.DUMMY, passwd);
        // Test 1: bookie response: OK, NO_SUCH_LEDGER_EXISTS, NOT_AVAILABLE
        // Expected: Recovery successful Q(response) = 2
        int responseCode = readLACFromQuorum(readLh, BKException.Code.BookieHandleNotAvailableException,
                BKException.Code.OK, BKException.Code.NoSuchLedgerExistsException);
        assertEquals(responseCode, BKException.Code.OK);
        // Test 2: bookie response: OK, NOT_AVAILABLE, NOT_AVAILABLE
        // Expected: Recovery fail Q(response) = 1
        responseCode = readLACFromQuorum(readLh, BKException.Code.BookieHandleNotAvailableException,
                BKException.Code.OK, BKException.Code.BookieHandleNotAvailableException);
        assertEquals(responseCode, BKException.Code.BookieHandleNotAvailableException);

        /**
         * Test Group-2 : Expected Response for recovery: Qr = (Qw - Qa)+1 = (2
         * -2) + 1 = 1
         */
        ensembleSize = 2;
        writeQuorumSize = 2;
        ackQuormSize = 2;
        lh0 = newBk0.createLedger(ensembleSize, writeQuorumSize, ackQuormSize, DigestType.DUMMY, passwd);
        readLh = readBk.openLedgerNoRecovery(lh0.getId(), DigestType.DUMMY, passwd);
        // Test 1: bookie response: OK, NOT_AVAILABLE
        // Expected: Recovery successful Q(response) = 1
        responseCode = readLACFromQuorum(readLh, BKException.Code.BookieHandleNotAvailableException,
                BKException.Code.OK);
        assertEquals(responseCode, BKException.Code.OK);

        // Test 1: bookie response: OK, NO_SUCH_LEDGER_EXISTS
        // Expected: Recovery successful Q(response) = 2
        responseCode = readLACFromQuorum(readLh, BKException.Code.NoSuchLedgerExistsException, BKException.Code.OK);
        assertEquals(responseCode, BKException.Code.OK);

        // Test 3: bookie response: NOT_AVAILABLE, NOT_AVAILABLE
        // Expected: Recovery fail Q(response) = 0
        responseCode = readLACFromQuorum(readLh, BKException.Code.BookieHandleNotAvailableException,
                BKException.Code.BookieHandleNotAvailableException);
        assertEquals(responseCode, BKException.Code.BookieHandleNotAvailableException);

        newBk0.close();
        readBk.close();
    }

    private int readLACFromQuorum(LedgerHandle ledger, int... bookieLACResponse) throws Exception {
        MutableInt responseCode = new MutableInt(100);
        CountDownLatch responseLatch = new CountDownLatch(1);
        ReadLastConfirmedOp readLCOp = new ReadLastConfirmedOp(
                bkc.getBookieClient(),
                ledger.getDistributionSchedule(),
                ledger.getDigestManager(),
                ledger.getId(),
                ledger.getLedgerMetadata().getAllEnsembles().lastEntry().getValue(),
                ledger.getLedgerKey(),
                new ReadLastConfirmedOp.LastConfirmedDataCallback() {
                    @Override
                    public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                        System.out.println("response = " + rc);
                        responseCode.setValue(rc);
                        responseLatch.countDown();
                    }
                });
        byte[] lac = new byte[Long.SIZE * 3];
        ByteBuf data = Unpooled.wrappedBuffer(lac, 0, lac.length);
        int writerIndex = data.writerIndex();
        data.resetWriterIndex();
        data.writeLong(ledger.getId());
        data.writeLong(0L);
        data.writerIndex(writerIndex);
        for (int i = 0; i < bookieLACResponse.length; i++) {
            readLCOp.readEntryComplete(bookieLACResponse[i], 0, 0, data, i);
        }
        responseLatch.await();
        return responseCode.intValue();
    }
}
