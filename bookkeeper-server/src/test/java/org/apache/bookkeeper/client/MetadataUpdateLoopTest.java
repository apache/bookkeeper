/**
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MockLedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang3.tuple.Triple;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test metadata update loop.
 */
public class MetadataUpdateLoopTest {
    static final Logger LOG = LoggerFactory.getLogger(MetadataUpdateLoopTest.class);

    /**
     * Test that we can update the metadata using the update loop.
     */
    @Test
    public void testBasicUpdate() throws Exception {
        try (LedgerManager lm = new MockLedgerManager()) {
            LedgerMetadata initMeta = LedgerMetadataBuilder.create().withEnsembleSize(5)
                .newEnsembleEntry(0L, Lists.newArrayList(
                                          new BookieSocketAddress("0.0.0.0:3181"),
                                          new BookieSocketAddress("0.0.0.1:3181"),
                                          new BookieSocketAddress("0.0.0.2:3181"),
                                          new BookieSocketAddress("0.0.0.3:3181"),
                                          new BookieSocketAddress("0.0.0.4:3181"))).build();
            GenericCallbackFuture<Versioned<LedgerMetadata>> promise = new GenericCallbackFuture<>();
            long ledgerId = 1234L;
            lm.createLedgerMetadata(ledgerId, initMeta, promise);
            Versioned<LedgerMetadata> writtenMetadata = promise.get();

            AtomicReference<Versioned<LedgerMetadata>> reference = new AtomicReference<>(writtenMetadata);

            BookieSocketAddress newAddress = new BookieSocketAddress("0.0.0.5:3181");
            MetadataUpdateLoop loop = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> true,
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(0, newAddress);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet);
            loop.run().get();

            Assert.assertNotEquals(reference.get(), writtenMetadata);
            Assert.assertEquals(reference.get().getValue().getEnsemble(0L).get(0), newAddress);
        }
    }

    /**
     * Test that when 2 update loops conflict when making diffent updates to the metadata,
     * both will eventually succeed, and both updates will be reflected in the final metadata.
     */
    @Test
    public void testConflictOnWrite() throws Exception {
        try (BlockableMockLedgerManager lm = spy(new BlockableMockLedgerManager())) {
            lm.blockWrites();

            long ledgerId = 1234L;
            BookieSocketAddress b0 = new BookieSocketAddress("0.0.0.0:3181");
            BookieSocketAddress b1 = new BookieSocketAddress("0.0.0.1:3181");
            BookieSocketAddress b2 = new BookieSocketAddress("0.0.0.2:3181");
            BookieSocketAddress b3 = new BookieSocketAddress("0.0.0.3:3181");

            LedgerMetadata initMeta = LedgerMetadataBuilder.create().withEnsembleSize(2)
                .newEnsembleEntry(0L, Lists.newArrayList(b0, b1)).build();
            GenericCallbackFuture<Versioned<LedgerMetadata>> promise = new GenericCallbackFuture<>();
            lm.createLedgerMetadata(ledgerId, initMeta, promise);
            Versioned<LedgerMetadata> writtenMetadata = promise.get();

            AtomicReference<Versioned<LedgerMetadata>> reference1 = new AtomicReference<>(writtenMetadata);
            CompletableFuture<Versioned<LedgerMetadata>> loop1 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference1::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(b0),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(0, b2);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference1::compareAndSet).run();

            AtomicReference<Versioned<LedgerMetadata>> reference2 = new AtomicReference<>(writtenMetadata);
            CompletableFuture<Versioned<LedgerMetadata>> loop2 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference2::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(b1),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(1, b3);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference2::compareAndSet).run();

            lm.releaseWrites();

            Versioned<LedgerMetadata> l1meta = loop1.get();
            Versioned<LedgerMetadata> l2meta = loop2.get();

            Assert.assertEquals(l1meta, reference1.get());
            Assert.assertEquals(l2meta, reference2.get());

            Assert.assertEquals(l1meta.getVersion().compare(l2meta.getVersion()), Version.Occurred.BEFORE);

            Assert.assertEquals(l1meta.getValue().getEnsemble(0L).get(0), b2);
            Assert.assertEquals(l1meta.getValue().getEnsemble(0L).get(1), b1);

            Assert.assertEquals(l2meta.getValue().getEnsemble(0L).get(0), b2);
            Assert.assertEquals(l2meta.getValue().getEnsemble(0L).get(1), b3);

            verify(lm, times(3)).writeLedgerMetadata(anyLong(), any(), any(), any());
        }
    }

    /**
     * Test that when 2 updates loops try to make the same modification, and they
     * conflict on the write to the store, the one that receives the conflict won't
     * try to write again, as the value is now correct.
     */
    @Test
    public void testConflictOnWriteBothWritingSame() throws Exception {
        try (BlockableMockLedgerManager lm = spy(new BlockableMockLedgerManager())) {
            lm.blockWrites();

            long ledgerId = 1234L;
            BookieSocketAddress b0 = new BookieSocketAddress("0.0.0.0:3181");
            BookieSocketAddress b1 = new BookieSocketAddress("0.0.0.1:3181");
            BookieSocketAddress b2 = new BookieSocketAddress("0.0.0.2:3181");

            LedgerMetadata initMeta = LedgerMetadataBuilder.create().withEnsembleSize(2)
                .newEnsembleEntry(0L, Lists.newArrayList(b0, b1)).build();
            GenericCallbackFuture<Versioned<LedgerMetadata>> promise = new GenericCallbackFuture<>();
            lm.createLedgerMetadata(ledgerId, initMeta, promise);
            Versioned<LedgerMetadata> writtenMetadata = promise.get();

            AtomicReference<Versioned<LedgerMetadata>> reference = new AtomicReference<>(writtenMetadata);

            CompletableFuture<Versioned<LedgerMetadata>> loop1 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(b0),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(0, b2);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet).run();
            CompletableFuture<Versioned<LedgerMetadata>> loop2 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(b0),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(0, b2);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet).run();

            lm.releaseWrites();

            Assert.assertEquals(loop1.get(), loop2.get());
            Assert.assertEquals(loop1.get(), reference.get());

            Assert.assertEquals(reference.get().getValue().getEnsemble(0L).get(0), b2);
            Assert.assertEquals(reference.get().getValue().getEnsemble(0L).get(1), b1);

            verify(lm, times(2)).writeLedgerMetadata(anyLong(), any(), any(), any());
        }
    }

    /**
     * Test that when 2 update loops both manage to write, but conflict on
     * updating the local value.
     */
    @Test
    public void testConflictOnLocalUpdate() throws Exception {
        try (DeferCallbacksMockLedgerManager lm = spy(new DeferCallbacksMockLedgerManager(1))) {
            long ledgerId = 1234L;
            BookieSocketAddress b0 = new BookieSocketAddress("0.0.0.0:3181");
            BookieSocketAddress b1 = new BookieSocketAddress("0.0.0.1:3181");
            BookieSocketAddress b2 = new BookieSocketAddress("0.0.0.2:3181");
            BookieSocketAddress b3 = new BookieSocketAddress("0.0.0.3:3181");

            LedgerMetadata initMeta = LedgerMetadataBuilder.create().withEnsembleSize(2)
                .newEnsembleEntry(0L, Lists.newArrayList(b0, b1)).build();
            GenericCallbackFuture<Versioned<LedgerMetadata>> promise = new GenericCallbackFuture<>();
            lm.createLedgerMetadata(ledgerId, initMeta, promise);
            Versioned<LedgerMetadata> writtenMetadata = promise.get();

            AtomicReference<Versioned<LedgerMetadata>> reference = new AtomicReference<>(writtenMetadata);

            CompletableFuture<Versioned<LedgerMetadata>> loop1 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(b0),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(0, b2);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet).run();

            lm.waitForWriteCount(1);
            CompletableFuture<Versioned<LedgerMetadata>> loop2 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(b1),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(1, b3);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet).run();
            Assert.assertEquals(loop2.get(), reference.get());

            lm.runDeferred();

            Assert.assertEquals(loop1.get(), reference.get());

            Assert.assertEquals(reference.get().getValue().getEnsemble(0L).get(0), b2);
            Assert.assertEquals(reference.get().getValue().getEnsemble(0L).get(1), b3);

            verify(lm, times(3)).writeLedgerMetadata(anyLong(), any(), any(), any());
        }
    }

    private static BookieSocketAddress address(String s) {
        try {
            return new BookieSocketAddress(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Hammer test. Kick off a lot of metadata updates concurrently with a ledger manager
     * that runs callbacks on random threads, and validate all updates complete eventually,
     * and that the final metadata reflects all the updates.
     */
    @Test
    public void testHammer() throws Exception {
        try (NonDeterministicMockLedgerManager lm = new NonDeterministicMockLedgerManager()) {
            long ledgerId = 1234L;

            int ensembleSize = 100;
            List<BookieSocketAddress> initialEnsemble = IntStream.range(0, ensembleSize)
                .mapToObj((i) -> address(String.format("0.0.0.%d:3181", i)))
                .collect(Collectors.toList());

            LedgerMetadata initMeta = LedgerMetadataBuilder.create().withEnsembleSize(ensembleSize)
                .newEnsembleEntry(0L, initialEnsemble).build();
            GenericCallbackFuture<Versioned<LedgerMetadata>> promise = new GenericCallbackFuture<>();
            lm.createLedgerMetadata(ledgerId, initMeta, promise);
            Versioned<LedgerMetadata> writtenMetadata = promise.get();

            AtomicReference<Versioned<LedgerMetadata>> reference = new AtomicReference<>(writtenMetadata);

            List<BookieSocketAddress> replacementBookies = IntStream.range(0, ensembleSize)
                .mapToObj((i) -> address(String.format("0.0.%d.1:3181", i)))
                .collect(Collectors.toList());

            List<CompletableFuture<Versioned<LedgerMetadata>>> loops = IntStream.range(0, ensembleSize)
                .mapToObj((i) -> new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> currentMetadata.getEnsemble(0L).contains(initialEnsemble.get(i)),
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(i, replacementBookies.get(i));
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet).run())
                .collect(Collectors.toList());

            loops.forEach((l) -> l.join());

            Assert.assertEquals(reference.get().getValue().getEnsemble(0L), replacementBookies);
        }
    }

    /**
     * Test that if we have two conflicting updates, only one of the loops will complete.
     * The other will throw an exception.
     */
    @Test
    public void testNewestValueCannotBeUsedAfterReadBack() throws Exception {
        try (BlockableMockLedgerManager lm = spy(new BlockableMockLedgerManager())) {
            lm.blockWrites();

            long ledgerId = 1234L;
            BookieSocketAddress b0 = new BookieSocketAddress("0.0.0.0:3181");
            BookieSocketAddress b1 = new BookieSocketAddress("0.0.0.1:3181");

            LedgerMetadata initMeta = LedgerMetadataBuilder.create().withEnsembleSize(1)
                .newEnsembleEntry(0L, Lists.newArrayList(b0)).build();
            GenericCallbackFuture<Versioned<LedgerMetadata>> promise = new GenericCallbackFuture<>();
            lm.createLedgerMetadata(ledgerId, initMeta, promise);
            Versioned<LedgerMetadata> writtenMetadata = promise.get();

            AtomicReference<Versioned<LedgerMetadata>> reference = new AtomicReference<>(writtenMetadata);
            CompletableFuture<Versioned<LedgerMetadata>> loop1 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> !currentMetadata.isClosed(),
                    (currentMetadata) -> LedgerMetadataBuilder.from(currentMetadata).closingAt(10L, 100L).build(),
                    reference::compareAndSet).run();
            CompletableFuture<Versioned<LedgerMetadata>> loop2 = new MetadataUpdateLoop(
                    lm,
                    ledgerId,
                    reference::get,
                    (currentMetadata) -> {
                        if (currentMetadata.isClosed()) {
                            throw new BKException.BKLedgerClosedException();
                        } else {
                            return currentMetadata.getEnsemble(0L).contains(b0);
                        }
                    },
                    (currentMetadata) -> {
                        List<BookieSocketAddress> ensemble = Lists.newArrayList(currentMetadata.getEnsemble(0L));
                        ensemble.set(0, b1);
                        return LedgerMetadataBuilder.from(currentMetadata).replaceEnsembleEntry(0L, ensemble).build();
                    },
                    reference::compareAndSet).run();
            lm.releaseWrites();

            Versioned<LedgerMetadata> l1meta = loop1.get();
            try {
                loop2.get();
                Assert.fail("Update loop should have failed");
            } catch (ExecutionException ee) {
                Assert.assertEquals(ee.getCause().getClass(), BKException.BKLedgerClosedException.class);
            }
            Assert.assertEquals(l1meta, reference.get());
            Assert.assertEquals(l1meta.getValue().getEnsemble(0L).get(0), b0);
            Assert.assertTrue(l1meta.getValue().isClosed());

            verify(lm, times(2)).writeLedgerMetadata(anyLong(), any(), any(), any());
        }
    }

    static class NonDeterministicMockLedgerManager extends MockLedgerManager {
        final ExecutorService cbExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("non-deter-%d").build());

        @Override
        public void executeCallback(Runnable r) {
            cbExecutor.execute(r);
        }

        @Override
        public void close() {
            cbExecutor.shutdownNow();
            super.close();
        }
    }

    static class DeferCallbacksMockLedgerManager extends MockLedgerManager {
        int writeCount = 0;
        final int numToDefer;
        List<Triple<GenericCallback<Versioned<LedgerMetadata>>, Integer, Versioned<LedgerMetadata>>> deferred =
            Lists.newArrayList();

        DeferCallbacksMockLedgerManager(int numToDefer) {
            this.numToDefer = numToDefer;
        }

        synchronized void runDeferred() {
            deferred.forEach((d) -> d.getLeft().operationComplete(d.getMiddle(), d.getRight()));
        }

        synchronized void waitForWriteCount(int count) throws Exception {
            while (writeCount < count) {
                wait();
            }
        }

        @Override
        public synchronized void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                     Version currentVersion,
                                                     GenericCallback<Versioned<LedgerMetadata>> cb) {
            super.writeLedgerMetadata(ledgerId, metadata, currentVersion,
                                      (rc, written) -> {
                                          synchronized (DeferCallbacksMockLedgerManager.this) {
                                              if (writeCount++ < numToDefer) {
                                                  LOG.info("Added aaaaato deferals");
                                                  deferred.add(Triple.of(cb, rc, written));
                                              } else {
                                                  LOG.info("Completing {}", numToDefer);
                                                  cb.operationComplete(rc, written);
                                              }
                                              DeferCallbacksMockLedgerManager.this.notifyAll();
                                          }
                                      });
        };
    }

    @Data
    @AllArgsConstructor
    static class DeferredUpdate {
        final long ledgerId;
        final LedgerMetadata metadata;
        final Version currentVersion;
        final GenericCallback<Versioned<LedgerMetadata>> cb;
    }

    static class BlockableMockLedgerManager extends MockLedgerManager {
        boolean blocking = false;
        List<DeferredUpdate> reqs = Lists.newArrayList();

        synchronized void blockWrites() {
            blocking = true;
        }

        synchronized void releaseWrites() {
            blocking = false;
            reqs.forEach((r) -> super.writeLedgerMetadata(r.getLedgerId(), r.getMetadata(),
                                                          r.getCurrentVersion(), r.getCb()));
        }

        @Override
        public synchronized void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata,
                                                     Version currentVersion,
                                                     GenericCallback<Versioned<LedgerMetadata>> cb) {
            if (blocking) {
                reqs.add(new DeferredUpdate(ledgerId, metadata, currentVersion, cb));
            } else {
                super.writeLedgerMetadata(ledgerId, metadata, currentVersion, cb);
            }
        };
    }
}
