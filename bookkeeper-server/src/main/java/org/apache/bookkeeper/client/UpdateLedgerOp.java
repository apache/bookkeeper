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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.bookkeeper.bookie.BookieShell.UpdateLedgerNotifier;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates updating the ledger metadata operation.
 */
public class UpdateLedgerOp {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateLedgerOp.class);
    private final LedgerManager lm;
    private final BookKeeperAdmin admin;

    public UpdateLedgerOp(final BookKeeper bkc, final BookKeeperAdmin admin) {
        this.lm = bkc.getLedgerManager();
        this.admin = admin;
    }

    /**
     * Update the bookie id present in the ledger metadata.
     *
     * @param oldBookieId
     *            current bookie id
     * @param newBookieId
     *            new bookie id
     * @param rate
     *            number of ledgers updating per second (default 5 per sec)
     * @param limit
     *            maximum number of ledgers to update (default: no limit). Stop
     *            update if reaching limit
     * @param progressable
     *            report progress of the ledger updates
     * @throws IOException
     *             if there is an error when updating bookie id in ledger
     *             metadata
     */
    public void updateBookieIdInLedgers(final BookieSocketAddress oldBookieId, final BookieSocketAddress newBookieId,
                                        final int rate, final int limit, final UpdateLedgerNotifier progressable)
            throws IOException, InterruptedException {

        final AtomicInteger issuedLedgerCnt = new AtomicInteger();
        final AtomicInteger updatedLedgerCnt = new AtomicInteger();
        final AtomicBoolean errorOccurred = new AtomicBoolean(false);
        final Set<CompletableFuture<?>> outstanding =
            Collections.newSetFromMap(new ConcurrentHashMap<CompletableFuture<?>, Boolean>());
        final RateLimiter throttler = RateLimiter.create(rate);
        final Iterator<Long> ledgerItr = admin.listLedgers().iterator();

        // iterate through all the ledgers
        while (ledgerItr.hasNext() && !errorOccurred.get()
               && (limit == Integer.MIN_VALUE || issuedLedgerCnt.get() < limit)) {
            // throttler to control updates per second
            throttler.acquire();

            final long ledgerId = ledgerItr.next();
            issuedLedgerCnt.incrementAndGet();

            GenericCallbackFuture<LedgerMetadata> promise = new GenericCallbackFuture<>();
            lm.readLedgerMetadata(ledgerId, promise);
            promise.thenCompose((readMetadata) -> {
                    AtomicReference<LedgerMetadata> ref = new AtomicReference<>(readMetadata);
                    return new MetadataUpdateLoop(
                            lm, ledgerId,
                            ref::get,
                            (metadata) -> {
                                return metadata.getEnsembles().values().stream()
                                    .flatMap(Collection::stream)
                                    .filter(b -> b.equals(oldBookieId))
                                    .count() > 0;
                            },
                            (metadata) -> {
                                return replaceBookieInEnsembles(metadata, oldBookieId, newBookieId);
                            },
                            ref::compareAndSet).run();
                }).whenComplete((metadata, ex) -> {
                        LOG.info("Update of {} finished", ledgerId);
                        if (ex != null
                            && !(ex instanceof BKException.BKNoSuchLedgerExistsException)) {
                            LOG.error("Updating ledger metadata {} failed", ledgerId, ex);
                            errorOccurred.set(true);
                        } else {
                            updatedLedgerCnt.incrementAndGet();
                            progressable.progress(updatedLedgerCnt.get(), issuedLedgerCnt.get());
                        }
                    });

            outstanding.add(promise);
            promise.whenComplete((metadata, ex) -> outstanding.remove(promise));
        }

        try {
            CompletableFuture.allOf(outstanding.stream().toArray(CompletableFuture[]::new)).get();
            LOG.info("Total number of ledgers issued={} updated={}",
                     issuedLedgerCnt.get(), updatedLedgerCnt.get());
        } catch (ExecutionException e) {
            LOG.info("Error in execution", e);
            throw new IOException("Error executing update", e);
        }
    }

    private static LedgerMetadata replaceBookieInEnsembles(LedgerMetadata metadata,
                                                           BookieSocketAddress oldBookieId,
                                                           BookieSocketAddress newBookieId) {
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.from(metadata);
        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : metadata.getEnsembles().entrySet()) {
            List<BookieSocketAddress> newEnsemble = e.getValue().stream()
                .map(b -> b.equals(oldBookieId) ? newBookieId : b)
                .collect(Collectors.toList());
            builder.withEnsembleEntry(e.getKey(), newEnsemble);
        }

        return builder.build();
    }
}
