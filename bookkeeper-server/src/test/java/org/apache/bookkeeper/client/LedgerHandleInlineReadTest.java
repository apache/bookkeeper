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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.MockBookieClient;
import org.apache.bookkeeper.proto.MockBookies;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;

/**
 * Reads issued from the ledger's own worker thread are initiated inline instead of being queued on it.
 */
public class LedgerHandleInlineReadTest {

    private static final BookieId b1 = new BookieSocketAddress("b1", 3181).toBookieId();
    private static final BookieId b2 = new BookieSocketAddress("b2", 3181).toBookieId();
    private static final BookieId b3 = new BookieSocketAddress("b3", 3181).toBookieId();

    private MockClientContext clientCtx;
    private LedgerHandle lh;
    private final AtomicReference<Thread> readIssuedOn = new AtomicReference<>();

    private void setup(boolean decoratedThreads) throws Exception {
        if (decoratedThreads) {
            // Task tracing wraps the pool's threads in decorators; the handle must see through them
            OrderedExecutor pool = OrderedExecutor.newBuilder().name("inline-read-test").numThreads(1)
                    .traceTaskExecution(true).build();
            MockBookies mockBookies = new MockBookies();
            clientCtx = MockClientContext.create(mockBookies)
                    .setMainWorkerPool(pool)
                    .setBookieClient(new MockBookieClient(pool, mockBookies));
        } else {
            clientCtx = MockClientContext.create();
        }
        Versioned<LedgerMetadata> md = ClientUtil.setupLedger(clientCtx, 10L,
                LedgerMetadataBuilder.create().newEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3)));
        lh = new LedgerHandle(clientCtx, 10L, md, BookKeeper.DigestType.CRC32C, ClientUtil.PASSWD,
                WriteFlag.NONE);
        lh.append("entry".getBytes(UTF_8));

        // The hook runs synchronously inside the bookie client's readEntry, so it records the thread
        // that initiates the read request.
        clientCtx.getMockBookieClient().setPreReadHook((bookie, ledgerId, entryId) -> {
            readIssuedOn.compareAndSet(null, Thread.currentThread());
            return FutureUtils.value(null);
        });
    }

    @Test(timeout = 30000)
    public void testReadFromLedgerThreadIsInitiatedInline() throws Exception {
        setup(false);
        assertReadsFromLedgerThreadAreInitiatedInline();
    }

    @Test(timeout = 30000)
    public void testReadFromLedgerThreadIsInitiatedInlineWithDecoratedThreads() throws Exception {
        setup(true);
        assertReadsFromLedgerThreadAreInitiatedInline();
    }

    private void assertReadsFromLedgerThreadAreInitiatedInline() throws Exception {
        assertTrue(issuedBeforeReturnOnLedgerThread(result -> {
            lh.readAsync(0, 0).whenComplete((entries, ex) -> complete(result, entries, ex));
        }));

        readIssuedOn.set(null);
        assertTrue(issuedBeforeReturnOnLedgerThread(result -> {
            lh.asyncReadEntries(0, 0, (rc, handle, entries, ctx) -> {
                if (rc == BKException.Code.OK) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(BKException.create(rc));
                }
            }, null);
        }));
    }

    @Test(timeout = 30000)
    public void testReadFromOtherThreadIsQueuedOnLedgerThread() throws Exception {
        setup(false);
        try (LedgerEntries entries = lh.readAsync(0, 0).get(10, TimeUnit.SECONDS)) {
            assertFalse(Thread.currentThread() == readIssuedOn.get());
        }
        CompletableFuture<Thread> ledgerThread = new CompletableFuture<>();
        lh.executor.execute(() -> ledgerThread.complete(Thread.currentThread()));
        assertSame(ledgerThread.get(10, TimeUnit.SECONDS), readIssuedOn.get());
    }

    /**
     * Runs {@code read} on the ledger thread and returns whether the read request had reached the bookie
     * client, on that same thread, by the time the call returned. Also waits for the read to complete.
     */
    private boolean issuedBeforeReturnOnLedgerThread(Consumer<CompletableFuture<Void>> read) throws Exception {
        CompletableFuture<Void> completed = new CompletableFuture<>();
        CompletableFuture<Boolean> issuedBeforeReturn = new CompletableFuture<>();
        lh.executor.execute(() -> {
            read.accept(completed);
            issuedBeforeReturn.complete(readIssuedOn.get() == Thread.currentThread());
        });
        boolean inline = issuedBeforeReturn.get(10, TimeUnit.SECONDS);
        completed.get(10, TimeUnit.SECONDS);
        return inline;
    }

    private static void complete(CompletableFuture<Void> result, LedgerEntries entries, Throwable ex) {
        if (ex != null) {
            result.completeExceptionally(ex);
        } else {
            entries.close();
            result.complete(null);
        }
    }
}
