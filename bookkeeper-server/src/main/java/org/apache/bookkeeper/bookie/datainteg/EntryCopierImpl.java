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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;

/**
 * Implementation for the EntryCopier interface. Handles the reading of entries
 * from peer bookies.
 */
@Slf4j
public class EntryCopierImpl implements EntryCopier {
    private static final long SINBIN_DURATION_MS = TimeUnit.MINUTES.toMillis(1);
    private final BookieId bookieId;
    private final BookieClient bookieClient;
    private final LedgerStorage storage;
    private final Ticker ticker;
    private final SinBin sinBin;

    public EntryCopierImpl(BookieId bookieId,
                    BookieClient bookieClient,
                    LedgerStorage storage,
                    Ticker ticker) {
        this.bookieId = bookieId;
        this.bookieClient = bookieClient;
        this.storage = storage;
        this.ticker = ticker;
        this.sinBin = new SinBin(ticker);
    }

    @Override
    public Batch newBatch(long ledgerId, LedgerMetadata metadata) throws IOException {
        if (!storage.ledgerExists(ledgerId)) {
            storage.setMasterKey(ledgerId, metadata.getPassword());
        }
        return new BatchImpl(bookieId, ledgerId, metadata, sinBin);
    }

    @VisibleForTesting
    class BatchImpl implements Batch {
        private final long ledgerId;
        private final LedgerMetadata metadata;
        private final SinBin sinBin;
        private volatile ImmutableSortedMap<Long, WriteSets> writeSets;

        BatchImpl(BookieId bookieId,
                  long ledgerId, LedgerMetadata metadata,
                  SinBin sinBin) {
            this.ledgerId = ledgerId;
            this.metadata = metadata;
            this.sinBin = sinBin;
            updateWriteSets();
        }

        private void updateWriteSets() {
            // clear non-erroring bookies

            // in theory we should be able to have a single set of writesets per ledger,
            // however, if there are multiple ensembles, bookies will move around, and we
            // still want to avoid erroring bookies
            this.writeSets = preferredBookieIndices(bookieId, metadata,
                                                    sinBin.getErrorBookies(), ledgerId)
                .entrySet().stream().collect(
                        ImmutableSortedMap.toImmutableSortedMap(
                                Comparator.naturalOrder(),
                                e -> e.getKey(),
                                e -> new WriteSets(e.getValue(),
                                                   metadata.getEnsembleSize(),
                                                   metadata.getWriteQuorumSize())));
        }

        @VisibleForTesting
        void notifyBookieError(BookieId bookie) {
            if (sinBin.addFailed(bookie)) {
                updateWriteSets();
            }
        }

        @Override
        public CompletableFuture<Long> copyFromAvailable(long entryId) {
            if (entryId < 0) {
                throw new IllegalArgumentException(
                        String.format("Entry ID (%d) can't be less than 0", entryId));
            }
            if (metadata.isClosed() && entryId > metadata.getLastEntryId()) {
                throw new IllegalArgumentException(
                        String.format("Invalid entry id (%d), last entry for ledger %d is %d",
                                      entryId, ledgerId, metadata.getLastEntryId()));
            }
            CompletableFuture<Long> promise = new CompletableFuture<>();
            fetchEntry(entryId).whenComplete((buffer, exception) -> {
                    if (exception != null) {
                        promise.completeExceptionally(exception);
                    } else {
                        try {
                            long length = buffer.readableBytes();
                            storage.addEntry(buffer);
                            promise.complete(length);
                        } catch (Throwable t) {
                            promise.completeExceptionally(t);
                        } finally {
                            ReferenceCountUtil.release(buffer);
                        }
                    }
                });
            return promise;
        }

        @VisibleForTesting
        CompletableFuture<ByteBuf> fetchEntry(long entryId) {
            List<BookieId> ensemble = metadata.getEnsembleAt(entryId);
            final Map.Entry<Long, WriteSets> writeSetsForEntryId = this.writeSets
                    .floorEntry(entryId);
            if (writeSetsForEntryId == null) {
                log.error("writeSets for entryId {} not found, writeSets {}", entryId, writeSets);
                throw new IllegalStateException("writeSets for entryId: " + entryId + " not found");
            }
            ImmutableList<Integer> writeSet = writeSetsForEntryId
                    .getValue()
                    .getForEntry(entryId);
            int attempt = 0;
            CompletableFuture<ByteBuf> promise = new CompletableFuture<>();
            fetchRetryLoop(entryId, attempt,
                           ensemble, writeSet,
                           promise, Optional.empty());
            return promise;
        }

        private void fetchRetryLoop(long entryId, int attempt,
                                    List<BookieId> ensemble,
                                    ImmutableList<Integer> writeSet,
                                    CompletableFuture<ByteBuf> promise,
                                    Optional<Throwable> firstException) {
            if (attempt >= writeSet.size()) {
                promise.completeExceptionally(
                        firstException.orElse(new BKException.BKReadException()));
                return;
            }
            BookieId bookie = ensemble.get(writeSet.get(attempt));
            readEntry(bookie, ledgerId, entryId)
                .whenComplete((buffer, exception) -> {
                        if (exception != null) {
                            notifyBookieError(bookie);
                            Optional<Throwable> firstException1 =
                                firstException.isPresent() ? firstException : Optional.of(exception);
                            fetchRetryLoop(entryId, attempt + 1,
                                           ensemble, writeSet, promise, firstException1);
                        } else {
                            promise.complete(buffer);
                        }
                    });
        }
    }

    // convert callback api to future api
    private CompletableFuture<ByteBuf> readEntry(BookieId bookieId,
                                                 long ledgerId, long entryId) {
        CompletableFuture<ByteBuf> promise = new CompletableFuture<>();
        bookieClient.readEntry(bookieId, ledgerId, entryId,
                               (rc, ledgerId1, entryId1, buffer, ctx1) -> {
                                   if (rc != BKException.Code.OK) {
                                       promise.completeExceptionally(BKException.create(rc));
                                   } else {
                                       buffer.retain();
                                       promise.complete(buffer);
                                   }
                               }, null, BookieProtocol.FLAG_NONE);
        return promise;
    }

    /**
     * Generate a map of preferred bookie indices. For each ensemble, generate the order
     * in which bookies should be tried for entries, notwithstanding errors.
     * For example, if a e5,w2,a2 ensemble has the bookies:
     * [bookie1, bookie2, bookie3, bookie4, bookie5]
     * and the current bookie is bookie2, then we should return something like:
     * [4, 2, 0, 3]
     * Then when retrieving an entry, even though it is only written to 2, we try the bookie
     * in the order from this list. This will cause more requests to go to the same bookie,
     * which should give us the benefit of read locality.
     * We don't want to simply sort by bookie id, as that would cause the same bookies to be
     * loaded for all ensembles.
     * Bookies which have presented errors are always tried last.
     */
    @VisibleForTesting
    static ImmutableSortedMap<Long, ImmutableList<Integer>> preferredBookieIndices(
            BookieId bookieId,
            LedgerMetadata metadata,
            Set<BookieId> errorBookies,
            long seed) {
        return metadata.getAllEnsembles().entrySet().stream()
            .collect(ImmutableSortedMap.toImmutableSortedMap(
                             Comparator.naturalOrder(),
                             e -> e.getKey(),
                             e -> {
                                 List<BookieId> ensemble = e.getValue();
                                 // get indices of the interesting bookies
                                 int myIndex = ensemble.indexOf(bookieId);
                                 Set<Integer> errorIndices = errorBookies.stream()
                                     .map(b -> ensemble.indexOf(b)).collect(Collectors.toSet());

                                 // turn bookies into positions and filter out my own
                                 // bookie id (we're not going to try to read from outself)
                                 List<Integer> indices = IntStream.range(0, ensemble.size())
                                     .filter(i -> i != myIndex).boxed().collect(Collectors.toList());

                                 // shuffle the indices based seed (normally ledgerId)
                                 Collections.shuffle(indices, new Random(seed));

                                 // Move the error bookies to the end
                                 // Collections#sort is stable, so everything else remains the same
                                 Collections.sort(indices, (a, b) -> {
                                         boolean aErr = errorIndices.contains(a);
                                         boolean bErr = errorIndices.contains(b);
                                         if (aErr && !bErr) {
                                             return 1;
                                         } else if (!aErr && bErr) {
                                             return -1;
                                         } else {
                                             return 0;
                                         }
                                     });
                                 return ImmutableList.copyOf(indices);
                             }));
    }

    @VisibleForTesting
    static class SinBin {
        private final Ticker ticker;
        private final ConcurrentMap<BookieId, Long> errorBookies = new ConcurrentHashMap<>();

        SinBin(Ticker ticker) {
            this.ticker = ticker;
        }

        /**
         * Returns true if this is the first error for this bookie.
         */
        boolean addFailed(BookieId bookie) {
            long newDeadline = TimeUnit.NANOSECONDS.toMillis(ticker.read()) + SINBIN_DURATION_MS;
            Long oldDeadline = errorBookies.put(bookie, newDeadline);
            return oldDeadline == null;
        }

        Set<BookieId> getErrorBookies() {
            long now = TimeUnit.NANOSECONDS.toMillis(ticker.read());
            Iterator<Map.Entry<BookieId, Long>> iterator = errorBookies.entrySet().iterator();
            while (iterator.hasNext()) {
                if (iterator.next().getValue() < now) {
                    iterator.remove();
                }
            }
            return errorBookies.keySet();
        }
    }
}
