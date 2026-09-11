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
package org.apache.bookkeeper.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link CreateBuilder#withOrderingKey(Object)} and {@link OpenBuilder#withOrderingKey(Object)} pin
 * every callback of the resulting handle to the worker thread selected by the key, and that without a key the
 * thread is still selected by ledger id. Both wire protocols are covered since their response dispatch differs.
 */
public class OrderingKeyTest extends BookKeeperClusterTestCase {

    private static final byte[] PASSWORD = "ordering-key".getBytes(UTF_8);
    private static final byte[] DATA = "entry".getBytes(UTF_8);
    private static final int NUM_WORKER_THREADS = 4;
    private static final long TIMEOUT_SECONDS = 30;

    public OrderingKeyTest() {
        super(3);
    }

    @Test
    public void testOrderingKeyV3() throws Exception {
        testOrderingKey(false);
    }

    @Test
    public void testOrderingKeyV2() throws Exception {
        testOrderingKey(true);
    }

    @Test
    public void testDefaultKeyedByLedgerIdV3() throws Exception {
        testDefaultKeyedByLedgerId(false);
    }

    @Test
    public void testDefaultKeyedByLedgerIdV2() throws Exception {
        testDefaultKeyedByLedgerId(true);
    }

    private void testOrderingKey(boolean useV2WireProtocol) throws Exception {
        try (BookKeeper bk = new BookKeeper(clientConf(useV2WireProtocol))) {
            OrderedExecutor pool = bk.getMainWorkerPool();

            // Create with an explicit ledger id, so that the key can be chosen to map to a different
            // thread than the ledger id: this proves the key, not the id, selects the thread.
            long ledgerId = 0xABCDEFL;
            String createKey = keyOnOtherThread(pool, "create", ledgerId);
            Thread createKeyThread = threadOf(pool.chooseThread(createKey));
            assertNotSame(threadOf(pool.chooseThread(ledgerId)), createKeyThread);

            LedgerHandleAdv writer = (LedgerHandleAdv) bk.newCreateLedgerOp()
                    .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                    .withPassword(PASSWORD)
                    .makeAdv()
                    .withLedgerId(ledgerId)
                    .withOrderingKey(createKey)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals(ledgerId, writer.getId());
            for (long entryId = 0; entryId < 3; entryId++) {
                long id = entryId;
                assertSame(createKeyThread, callbackThread(
                        f -> writer.asyncAddEntry(id, DATA, (rc, lh, eid, ctx) -> complete(f, rc), null)));
            }
            assertSame(createKeyThread, callbackThread(
                    f -> writer.asyncReadEntries(0, 2, (rc, lh, entries, ctx) -> complete(f, rc), null)));
            writer.close();

            // Open the closed ledger, without recovery, under a different key.
            String openKey = keyOnOtherThread(pool, "open", ledgerId);
            Thread openKeyThread = threadOf(pool.chooseThread(openKey));
            LedgerHandle reader = (LedgerHandle) bk.newOpenLedgerOp()
                    .withLedgerId(ledgerId).withPassword(PASSWORD).withRecovery(false)
                    .withOrderingKey(openKey)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertSame(openKeyThread, callbackThread(
                    f -> reader.asyncReadEntries(0, 2, (rc, lh, entries, ctx) -> complete(f, rc), null)));
            reader.close();

            // Open an unclosed ledger without recovery: reading the last add confirmed goes to the bookies
            // (on a closed ledger it completes inline from the metadata) and lands on the key's thread.
            LedgerHandle unclosed = (LedgerHandle) bk.newCreateLedgerOp()
                    .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                    .withPassword(PASSWORD)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (int i = 0; i < 3; i++) {
                unclosed.addEntry(DATA);
            }
            String lacKey = keyOnOtherThread(pool, "lac", unclosed.getId());
            Thread lacKeyThread = threadOf(pool.chooseThread(lacKey));
            LedgerHandle tailer = (LedgerHandle) bk.newOpenLedgerOp()
                    .withLedgerId(unclosed.getId()).withPassword(PASSWORD).withRecovery(false)
                    .withOrderingKey(lacKey)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertSame(lacKeyThread, callbackThread(
                    f -> tailer.asyncReadLastConfirmed((rc, lac, ctx) -> complete(f, rc), null)));
            tailer.close();

            // Open the same unclosed ledger with recovery: the recovery reads and adds, the recovery
            // completion and the reads that follow all run under the key. The fenced writer is
            // deliberately left open.
            String recoveryKey = keyOnOtherThread(pool, "recovery", unclosed.getId());
            Thread recoveryKeyThread = threadOf(pool.chooseThread(recoveryKey));
            LedgerHandle recovered = (LedgerHandle) bk.newOpenLedgerOp()
                    .withLedgerId(unclosed.getId()).withPassword(PASSWORD).withRecovery(true)
                    .withOrderingKey(recoveryKey)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals(2, recovered.getLastAddConfirmed());
            assertSame(recoveryKeyThread, callbackThread(
                    f -> recovered.asyncReadEntries(0, 2, (rc, lh, entries, ctx) -> complete(f, rc), null)));
            recovered.close();
        }
    }

    private void testDefaultKeyedByLedgerId(boolean useV2WireProtocol) throws Exception {
        try (BookKeeper bk = new BookKeeper(clientConf(useV2WireProtocol))) {
            OrderedExecutor pool = bk.getMainWorkerPool();

            LedgerHandle writer = (LedgerHandle) bk.newCreateLedgerOp()
                    .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
                    .withPassword(PASSWORD)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Thread ledgerIdThread = threadOf(pool.chooseThread(writer.getId()));
            assertSame(ledgerIdThread, callbackThread(
                    f -> writer.asyncAddEntry(DATA, (rc, lh, eid, ctx) -> complete(f, rc), null)));
            writer.close();

            LedgerHandle reader = (LedgerHandle) bk.newOpenLedgerOp()
                    .withLedgerId(writer.getId()).withPassword(PASSWORD)
                    .execute().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertSame(ledgerIdThread, callbackThread(
                    f -> reader.asyncReadEntries(0, 0, (rc, lh, entries, ctx) -> complete(f, rc), null)));
            reader.close();
        }
    }

    private ClientConfiguration clientConf(boolean useV2WireProtocol) {
        return new ClientConfiguration(baseClientConf)
                .setUseV2WireProtocol(useV2WireProtocol)
                .setNumWorkerThreads(NUM_WORKER_THREADS);
    }

    /** Picks a key whose worker thread differs from the one the pool selects for {@code ledgerId}. */
    private static String keyOnOtherThread(OrderedExecutor pool, String prefix, long ledgerId) {
        for (int i = 0; i < 100; i++) {
            String key = prefix + "-" + i;
            if (pool.chooseThread(key) != pool.chooseThread(ledgerId)) {
                return key;
            }
        }
        throw new AssertionError("no key mapped to a thread other than the ledger id's");
    }

    /** The thread behind one of the pool's single-threaded executors. */
    private static Thread threadOf(Executor executor) throws Exception {
        return callbackThread(f -> executor.execute(() -> f.complete(Thread.currentThread())));
    }

    /** Runs {@code operation} and returns the thread on which it completed the future. */
    private static Thread callbackThread(Consumer<CompletableFuture<Thread>> operation) throws Exception {
        CompletableFuture<Thread> thread = new CompletableFuture<>();
        operation.accept(thread);
        return thread.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void complete(CompletableFuture<Thread> future, int rc) {
        if (rc == BKException.Code.OK) {
            future.complete(Thread.currentThread());
        } else {
            future.completeExceptionally(BKException.create(rc));
        }
    }
}
