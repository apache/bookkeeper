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

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.bookkeeper.bookie.BookieShell.UpdateLedgerNotifier;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;
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
                                        final int rate, int maxOutstandingReads, final int limit,
                                        final UpdateLedgerNotifier progressable)
            throws IOException, InterruptedException {

        final AtomicInteger issuedLedgerCnt = new AtomicInteger();
        final AtomicInteger updatedLedgerCnt = new AtomicInteger();
        final CompletableFuture<Void> finalPromise = new CompletableFuture<>();
        final Set<CompletableFuture<?>> outstanding =
            Collections.newSetFromMap(new ConcurrentHashMap<CompletableFuture<?>, Boolean>());
        final RateLimiter throttler = RateLimiter.create(rate);
        final Semaphore outstandingReads = new Semaphore(maxOutstandingReads);
        final Iterator<Long> ledgerItr = admin.listLedgers().iterator();

        // iterate through all the ledgers
        while (ledgerItr.hasNext() && !finalPromise.isDone()
               && (limit == Integer.MIN_VALUE || issuedLedgerCnt.get() < limit)) {
            // semaphore to control reads according to update throttling
            outstandingReads.acquire();

            final long ledgerId = ledgerItr.next();
            issuedLedgerCnt.incrementAndGet();

            CompletableFuture<Versioned<LedgerMetadata>> writePromise = lm.readLedgerMetadata(ledgerId)
                .thenCompose((readMetadata) -> {
                    AtomicReference<Versioned<LedgerMetadata>> ref = new AtomicReference<>(readMetadata);
                    return new MetadataUpdateLoop(
                            lm, ledgerId,
                            ref::get,
                            (metadata) -> {
                                return metadata.getAllEnsembles().values().stream()
                                    .flatMap(Collection::stream)
                                    .anyMatch(b -> b.equals(oldBookieId));
                            },
                            (metadata) -> {
                                return replaceBookieInEnsembles(metadata, oldBookieId, newBookieId);
                            },
                            ref::compareAndSet, throttler).run();
                });

            outstanding.add(writePromise);
            writePromise.whenComplete((metadata, ex) -> {
                        if (ex != null
                            && !(ex instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException)) {
                            String error = String.format("Failed to update ledger metadata %s, replacing %s with %s",
                                                         ledgerId, oldBookieId, newBookieId);
                            LOG.error(error, ex);
                            finalPromise.completeExceptionally(new IOException(error, ex));
                        } else {
                            LOG.info("Updated ledger {} metadata, replacing {} with {}",
                                     ledgerId, oldBookieId, newBookieId);

                            updatedLedgerCnt.incrementAndGet();
                            progressable.progress(updatedLedgerCnt.get(), issuedLedgerCnt.get());
                        }
                        outstandingReads.release();
                        outstanding.remove(writePromise);
                    });
        }

        CompletableFuture.allOf(outstanding.stream().toArray(CompletableFuture[]::new))
            .whenComplete((res, ex) -> {
                    if (ex != null) {
                        finalPromise.completeExceptionally(ex);
                    } else {
                        finalPromise.complete(null);
                    }
                });

        try {
            finalPromise.get();
            LOG.info("Total number of ledgers issued={} updated={}",
                     issuedLedgerCnt.get(), updatedLedgerCnt.get());
        } catch (ExecutionException e) {
            String error = String.format("Error waiting for ledger metadata updates to complete (replacing %s with %s)",
                                         oldBookieId, newBookieId);
            LOG.info(error, e);
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(error, e);
            }
        }
    }

    private static LedgerMetadata replaceBookieInEnsembles(LedgerMetadata metadata,
                                                           BookieSocketAddress oldBookieId,
                                                           BookieSocketAddress newBookieId) {
        LedgerMetadataBuilder builder = LedgerMetadataBuilder.from(metadata);
        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : metadata.getAllEnsembles().entrySet()) {
            List<BookieSocketAddress> newEnsemble = e.getValue().stream()
                .map(b -> b.equals(oldBookieId) ? newBookieId : b)
                .collect(Collectors.toList());
            builder.replaceEnsembleEntry(e.getKey(), newEnsemble);
        }

        return builder.build();
    }
}
