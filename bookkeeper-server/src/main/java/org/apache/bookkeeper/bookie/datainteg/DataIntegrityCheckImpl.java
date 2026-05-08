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

import com.google.common.collect.ImmutableSortedMap;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage.StorageState;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;

/**
 * An implementation of the DataIntegrityCheck interface.
 */
@CustomLog
public class DataIntegrityCheckImpl implements DataIntegrityCheck {
    private static final int MAX_INFLIGHT = 300;
    private static final int MAX_ENTRIES_INFLIGHT = 3000;
    private static final int ZK_TIMEOUT_S = 30;
    private final BookieId bookieId;
    private final LedgerManager ledgerManager;
    private final LedgerStorage ledgerStorage;
    private final EntryCopier entryCopier;
    private final BookKeeperAdmin admin;
    private final Scheduler scheduler;
    private final AtomicReference<Map<Long, LedgerMetadata>> ledgersCacheRef =
        new AtomicReference<>(null);
    private CompletableFuture<Void> preBootFuture;

    public DataIntegrityCheckImpl(BookieId bookieId,
                                  LedgerManager ledgerManager,
                                  LedgerStorage ledgerStorage,
                                  EntryCopier entryCopier,
                                  BookKeeperAdmin admin,
                                  Scheduler scheduler) {
        this.bookieId = bookieId;
        this.ledgerManager = ledgerManager;
        this.ledgerStorage = ledgerStorage;
        this.entryCopier = entryCopier;
        this.admin = admin;
        this.scheduler = scheduler;
    }

    @Override
    public synchronized CompletableFuture<Void> runPreBootCheck(String reason) {
        // we only run this once, it could be kicked off by different checks
        if (preBootFuture == null) {
            preBootFuture = runPreBootSequence(reason);
        }
        return preBootFuture;

    }

    private CompletableFuture<Void> runPreBootSequence(String reason) {
        String runId = UUID.randomUUID().toString();
        log.info()
                .attr("event", Events.PREBOOT_START)
                .attr("runId", runId)
                .attr("reason", reason)
                .log("Preboot start");
        try {
            this.ledgerStorage.setStorageStateFlag(StorageState.NEEDS_INTEGRITY_CHECK);
        } catch (IOException ioe) {
            log.error()
                    .exception(ioe)
                    .attr("event", Events.PREBOOT_ERROR)
                    .attr("runId", runId)
                    .log("Preboot error");
            return FutureUtils.exception(ioe);
        }

        MetadataAsyncIterator iter = new MetadataAsyncIterator(scheduler,
                ledgerManager, MAX_INFLIGHT, ZK_TIMEOUT_S, TimeUnit.SECONDS);
        CompletableFuture<Void> promise = new CompletableFuture<>();
        Map<Long, LedgerMetadata> ledgersCache =
            new ConcurrentSkipListMap<>(Comparator.<Long>naturalOrder().reversed());
        iter.forEach((ledgerId, metadata) -> {
                if (ensemblesContainBookie(metadata, bookieId)) {
                    ledgersCache.put(ledgerId, metadata);
                    try {
                        if (!ledgerStorage.ledgerExists(ledgerId)) {
                            ledgerStorage.setMasterKey(ledgerId, new byte[0]);
                        }
                    } catch (IOException ioe) {
                        log.error()
                                .exception(ioe)
                                .attr("event", Events.ENSURE_LEDGER_ERROR)
                                .attr("runId", runId)
                                .attr("ledgerId", ledgerId)
                                .log("Ensure ledger error");
                        return FutureUtils.exception(ioe);
                    }
                }
                return processPreBoot(ledgerId, metadata, runId);
            })
            .whenComplete((ignore, exception) -> {
                    if (exception != null) {
                        log.error()
                                .exception(exception)
                                .attr("event", Events.PREBOOT_ERROR)
                                .attr("runId", runId)
                                .log("Preboot error");
                        promise.completeExceptionally(exception);
                    } else {
                        try {
                            this.ledgerStorage.flush();

                            updateMetadataCache(ledgersCache);

                            log.info()
                                    .attr("event", Events.PREBOOT_END)
                                    .attr("runId", runId)
                                    .attr("processed", ledgersCache.size())
                                    .log("Preboot end");
                            promise.complete(null);
                        } catch (Throwable t) {
                            log.error()
                                    .exception(t)
                                    .attr("event", Events.PREBOOT_ERROR)
                                    .attr("runId", runId)
                                    .log("Preboot error");
                            promise.completeExceptionally(t);
                        }
                    }
                });
        return promise;
    }

    @Override
    public boolean needsFullCheck() throws IOException {
        return this.ledgerStorage.getStorageStateFlags()
            .contains(StorageState.NEEDS_INTEGRITY_CHECK);
    }

    @Override
    public CompletableFuture<Void> runFullCheck() {
        String runId = UUID.randomUUID().toString();

        log.info()
                .attr("event", Events.FULL_CHECK_INIT)
                .attr("runId", runId)
                .log("Full check init");
        return getCachedOrReadMetadata(runId)
            .thenCompose(
                    (ledgers) -> {
                        log.info()
                                .attr("event", Events.FULL_CHECK_START)
                                .attr("runId", runId)
                                .attr("ledgerCount", ledgers.size())
                                .log("Full check start");
                        return checkAndRecoverLedgers(ledgers, runId).thenApply((resolved) -> {
                                for (LedgerResult r : resolved) {
                                    if (r.isMissing() || r.isOK()) {
                                        ledgers.remove(r.getLedgerId());
                                    } else if (r.isError()) {
                                        // if there was an error, make sure we have the latest
                                        // metadata for the next iteration
                                        ledgers.put(r.getLedgerId(), r.getMetadata());
                                    }
                                }
                                Optional<Throwable> firstError = resolved.stream().filter(r -> r.isError())
                                    .map(r -> r.getThrowable()).findFirst();

                                if (firstError.isPresent()) {
                                    log.error()
                                            .exception(firstError.get())
                                            .attr("event", Events.FULL_CHECK_END)
                                            .attr("runId", runId)
                                            .attr("ok", resolved.stream().filter(r -> r.isOK()).count())
                                            .attr("error", resolved.stream().filter(r -> r.isError()).count())
                                            .attr("missing", resolved.stream().filter(r -> r.isMissing()).count())
                                            .attr("ledgersToRetry", ledgers.size())
                                            .log("Full check end with errors");
                                } else {
                                    log.info()
                                            .attr("event", Events.FULL_CHECK_END)
                                            .attr("runId", runId)
                                            .attr("ok", resolved.stream().filter(r -> r.isOK()).count())
                                            .attr("error", 0)
                                            .attr("missing", resolved.stream().filter(r -> r.isMissing()).count())
                                            .attr("ledgersToRetry", ledgers.size())
                                            .log("Full check end");
                                }
                                return ledgers;
                            });
                    })
            .thenCompose(
                    (ledgers) -> {
                        CompletableFuture<Void> promise = new CompletableFuture<>();
                        try {
                            this.ledgerStorage.flush();
                            if (ledgers.isEmpty()) {
                                log.info()
                                        .attr("event", Events.CLEAR_INTEGCHECK_FLAG)
                                        .attr("runId", runId)
                                        .log("Clearing integrity check flag");
                                this.ledgerStorage.clearStorageStateFlag(
                                        StorageState.NEEDS_INTEGRITY_CHECK);
                            }
                            // not really needed as we are modifying the map in place
                            updateMetadataCache(ledgers);
                            log.info()
                                    .attr("event", Events.FULL_CHECK_COMPLETE)
                                    .attr("runId", runId)
                                    .log("Full check complete");
                            promise.complete(null);
                        } catch (IOException ioe) {
                            log.error()
                                    .exception(ioe)
                                    .attr("event", Events.FULL_CHECK_ERROR)
                                    .attr("runId", runId)
                                    .log("Full check error");
                            promise.completeExceptionally(ioe);
                        }
                        return promise;
                    });
    }

    void updateMetadataCache(Map<Long, LedgerMetadata> ledgers) {
        ledgersCacheRef.set(ledgers);
    }

    CompletableFuture<Map<Long, LedgerMetadata>> getCachedOrReadMetadata(String runId) {
        Map<Long, LedgerMetadata> map = ledgersCacheRef.get();
        if (map != null) {
            log.info()
                    .attr("event", Events.USE_CACHED_METADATA)
                    .attr("runId", runId)
                    .attr("ledgerCount", map.size())
                    .log("Using cached metadata");
            return CompletableFuture.completedFuture(map);
        } else {
            log.info()
                    .attr("event", Events.REFRESH_METADATA)
                    .attr("runId", runId)
                    .log("Refreshing metadata");
            MetadataAsyncIterator iter = new MetadataAsyncIterator(scheduler,
                    ledgerManager, MAX_INFLIGHT, ZK_TIMEOUT_S, TimeUnit.SECONDS);
            Map<Long, LedgerMetadata> ledgersCache =
                new ConcurrentSkipListMap<>(Comparator.<Long>naturalOrder().reversed());
            return iter.forEach((ledgerId, metadata) -> {
                    if (ensemblesContainBookie(metadata, bookieId)) {
                        ledgersCache.put(ledgerId, metadata);
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenApply(ignore -> {
                        updateMetadataCache(ledgersCache);
                        return ledgersCache;
                    });
        }
    }

    /**
     * Check whether the current bookie exists in the last ensemble of the bookie.
     * If it does, and the ledger is not closed, then this bookie may have accepted a fencing
     * request or an entry which it no longer contains. The only way to resolve this is to
     * open/recover the ledger. This bookie should not take part in the recovery, so the bookie
     * must be marked as in limbo. This will stop the bookie from responding to read requests for
     * that ledger, so clients will not be able to take into account the response of the bookie
     * during recovery. Effectively we are telling the client that we don't know whether we had
     * certain entries or not, so go look elsewhere.
     * We also fence all ledgers with this bookie in the last segment, to prevent any new writes,
     * so that after the limbo state is cleared, we won't accept any new writes.

     * We only need to consider final ensembles in non-closed ledgers at the moment of time that
     * the preboot check commences. If this bookie is added to a new ensemble after that point in
     * time, we know that we haven't received any entries for that segment, nor have we received
     * a fencing request, because we are still in the preboot sequence.
     */
    private CompletableFuture<Void> processPreBoot(long ledgerId, LedgerMetadata metadata,
                                                   String runId) {
        Map.Entry<Long, ? extends List<BookieId>> lastEnsemble = metadata.getAllEnsembles().lastEntry();
        CompletableFuture<Void> promise = new CompletableFuture<>();
        if (lastEnsemble == null) {
            log.error()
                    .attr("event", Events.INVALID_METADATA)
                    .attr("runId", runId)
                    .attr("metadata", metadata)
                    .attr("ledgerId", ledgerId)
                    .log("Invalid metadata");
            promise.completeExceptionally(
                    new IllegalStateException(
                            String.format("All metadata must have at least one ensemble, %d does not", ledgerId)));
            return promise;
        }


        if (!metadata.isClosed() && lastEnsemble.getValue().contains(bookieId)) {
            try {
                log.info()
                        .attr("event", Events.MARK_LIMBO)
                        .attr("runId", runId)
                        .attr("metadata", metadata)
                        .attr("ledgerId", ledgerId)
                        .log("Marking ledger as limbo");
                ledgerStorage.setLimboState(ledgerId);
                ledgerStorage.setFenced(ledgerId);
                promise.complete(null);
            } catch (IOException ioe) {
                log.info()
                        .exception(ioe)
                        .attr("event", Events.LIMBO_OR_FENCE_ERROR)
                        .attr("runId", runId)
                        .attr("metadata", metadata)
                        .attr("ledgerId", ledgerId)
                        .log("Limbo or fence error");
                promise.completeExceptionally(ioe);
            }
        } else {
            promise.complete(null);
        }
        return promise;
    }

    static class LedgerResult {
        enum State {
            MISSING, ERROR, OK
        }

        static LedgerResult missing(long ledgerId) {
            return new LedgerResult(State.MISSING, ledgerId, null, null);
        }

        static LedgerResult ok(long ledgerId, LedgerMetadata metadata) {
            return new LedgerResult(State.OK, ledgerId, metadata, null);
        }

        static LedgerResult error(long ledgerId, LedgerMetadata metadata, Throwable t) {
            return new LedgerResult(State.ERROR, ledgerId, metadata, t);
        }

        private final State state;
        private final long ledgerId;
        private final LedgerMetadata metadata;
        private final Throwable throwable;

        private LedgerResult(State state, long ledgerId,
                             LedgerMetadata metadata, Throwable throwable) {
            this.state = state;
            this.ledgerId = ledgerId;
            this.metadata = metadata;
            this.throwable = throwable;
        }

        boolean isMissing() {
            return state == State.MISSING;
        }
        boolean isOK() {
            return state == State.OK;
        }
        boolean isError() {
            return state == State.ERROR;
        }
        long getLedgerId() {
            return ledgerId;
        }
        LedgerMetadata getMetadata() {
            return metadata;
        }
        Throwable getThrowable() {
            return throwable;
        }
    }

    /**
     * Check each ledger passed.
     * If the ledger is in limbo, recover it.
     * Check that the bookie has all entries that it is expected to have.
     * Copy any entries that are missing.
     * @return The set of results for all ledgers passed. A result can be OK, Missing or Error.
     *         OK and missing ledgers do not need to be looked at again. Error should be retried.
     */
    CompletableFuture<Set<LedgerResult>> checkAndRecoverLedgers(Map<Long, LedgerMetadata> ledgers,
                                                                String runId) {
        CompletableFuture<Set<LedgerResult>> promise = new CompletableFuture<>();
        final Disposable disposable = Flowable.fromIterable(ledgers.entrySet())
                .subscribeOn(scheduler, false)
                .flatMapSingle((mapEntry) -> {
                            long ledgerId = mapEntry.getKey();
                            LedgerMetadata originalMetadata = mapEntry.getValue();
                            return recoverLedgerIfInLimbo(ledgerId, mapEntry.getValue(), runId)
                                    .map(newMetadata -> LedgerResult.ok(ledgerId, newMetadata))
                                    .onErrorReturn(t -> LedgerResult.error(ledgerId, originalMetadata, t))
                                    .defaultIfEmpty(LedgerResult.missing(ledgerId))
                                    .flatMap((res) -> {
                                        try {
                                            if (res.isOK()) {
                                                this.ledgerStorage.clearLimboState(ledgerId);
                                            }
                                            return Single.just(res);
                                        } catch (IOException ioe) {
                                            return Single.just(LedgerResult.error(res.getLedgerId(),
                                                    res.getMetadata(), ioe));
                                        }
                                    });
                        },
                        true /* delayErrors */,
                        MAX_INFLIGHT)
                .flatMapSingle((res) -> {
                            if (res.isOK()) {
                                return checkAndRecoverLedgerEntries(res.getLedgerId(),
                                        res.getMetadata(), runId)
                                        .map(ignore -> LedgerResult.ok(res.getLedgerId(),
                                                res.getMetadata()))
                                        .onErrorReturn(t -> LedgerResult.error(res.getLedgerId(),
                                                res.getMetadata(), t));
                            } else {
                                return Single.just(res);
                            }
                        },
                        true /* delayErrors */,
                        1 /* copy 1 ledger at a time to keep entries together in entrylog */)
                .collect(Collectors.toSet())
                .subscribe(resolved -> promise.complete(resolved),
                        throwable -> promise.completeExceptionally(throwable));
        promise.whenComplete((result, ex) -> disposable.dispose());
        return promise;
    }

    /**
     * Run ledger recovery on all a ledger if it has been marked as in limbo.
     * @return a maybe with the most up-to-date metadata we have for the ledger.
     *         If the ledger has been deleted, returns empty.
     */
    Maybe<LedgerMetadata> recoverLedgerIfInLimbo(long ledgerId, LedgerMetadata origMetadata,
                                                 String runId) {
        try {
            if (!this.ledgerStorage.ledgerExists(ledgerId)) {
                this.ledgerStorage.setMasterKey(ledgerId, new byte[0]);
            }
            if (this.ledgerStorage.hasLimboState(ledgerId)) {
                log.info()
                        .attr("event", Events.RECOVER_LIMBO_LEDGER)
                        .attr("runId", runId)
                        .attr("metadata", origMetadata)
                        .attr("ledgerId", ledgerId)
                        .log("Recovering limbo ledger");
                return recoverLedger(ledgerId, runId)
                    .toMaybe()
                    .onErrorResumeNext(t -> {
                            if (t instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException) {
                                log.info()
                                        .attr("event", Events.RECOVER_LIMBO_LEDGER_MISSING)
                                        .attr("runId", runId)
                                        .attr("metadata", origMetadata)
                                        .attr("ledgerId", ledgerId)
                                        .log("Recover limbo ledger missing");
                                return Maybe.empty();
                            } else {
                                log.info()
                                        .attr("event", Events.RECOVER_LIMBO_LEDGER_ERROR)
                                        .attr("runId", runId)
                                        .attr("metadata", origMetadata)
                                        .attr("ledgerId", ledgerId)
                                        .log("Recover limbo ledger error");
                                return Maybe.error(t);
                            }
                        });
            } else {
                return Maybe.just(origMetadata);
            }
        } catch (IOException ioe) {
            return Maybe.error(ioe);
        }
    }

    Single<LedgerMetadata> recoverLedger(long ledgerId, String runId) {
        return Single.create((emitter) ->
                admin.asyncOpenLedger(ledgerId, (rc, handle, ctx) -> {
                        if (rc != BKException.Code.OK) {
                            emitter.onError(BKException.create(rc));
                        } else {
                            LedgerMetadata metadata = handle.getLedgerMetadata();
                            handle.closeAsync().whenComplete((ignore, exception) -> {
                                    if (exception != null) {
                                        log.warn()
                                                .exception(exception)
                                                .attr("event", Events.RECOVER_LIMBO_LEDGER_CLOSE_ERROR)
                                                .attr("runId", runId)
                                                .attr("ledgerId", ledgerId)
                                                .log("Recover limbo ledger close error");
                                    }
                                });
                            emitter.onSuccess(metadata);
                        }
                    }, null));

    }

    /**
     * Check whether the local storage has all the entries as specified in the metadata.
     * If not, copy them from other available nodes.

     * Returns a single value which is the ledgerId or an error if any entry failed to copy
     * should throw error if any entry failed to copy.
     */
    Single<Long> checkAndRecoverLedgerEntries(long ledgerId, LedgerMetadata metadata,
                                              String runId) {
        WriteSets writeSets = new WriteSets(metadata.getEnsembleSize(),
                                            metadata.getWriteQuorumSize());

        NavigableMap<Long, Integer> bookieIndices = metadata.getAllEnsembles()
            .entrySet().stream()
            .collect(ImmutableSortedMap.toImmutableSortedMap(Comparator.naturalOrder(),
                                                             e -> e.getKey(),
                                                             e -> e.getValue().indexOf(bookieId)));

        long lastKnownEntry;
        if (metadata.isClosed()) {
            lastKnownEntry = metadata.getLastEntryId();
        } else {
            // if ledger is not closed, last known entry is the last entry of
            // the penultimate ensemble
            lastKnownEntry = metadata.getAllEnsembles().lastEntry().getKey() - 1;
        }
        if (lastKnownEntry < 0) {
            return Single.just(ledgerId);
        }

        EntryCopier.Batch batch;
        try {
            batch = entryCopier.newBatch(ledgerId, metadata);
        } catch (IOException ioe) {
            return Single.error(ioe);
        }
        AtomicLong byteCount = new AtomicLong(0);
        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicReference<Throwable> firstError = new AtomicReference<>(null);
        log.info()
                .attr("event", Events.LEDGER_CHECK_AND_COPY_START)
                .attr("runId", runId)
                .attr("metadata", metadata)
                .attr("ledgerId", ledgerId)
                .log("Ledger check and copy start");
        return Flowable.rangeLong(0, lastKnownEntry + 1)
            .subscribeOn(scheduler, false)
            .flatMapMaybe((entryId) -> {
                    return maybeCopyEntry(writeSets, bookieIndices, ledgerId, entryId, batch)
                        .doOnError((t) -> {
                                firstError.compareAndSet(null, t);
                                errorCount.incrementAndGet();
                            });
                }, true /* delayErrors */, MAX_ENTRIES_INFLIGHT)
            .doOnNext((bytes) -> {
                    byteCount.addAndGet(bytes);
                    count.incrementAndGet();
                })
            .count() // do nothing with result, but gives a single even if empty
            .doOnTerminate(() -> {
                    if (firstError.get() != null) {
                        log.warn()
                                .exception(firstError.get())
                                .attr("event", Events.LEDGER_CHECK_AND_COPY_END)
                                .attr("runId", runId)
                                .attr("metadata", metadata)
                                .attr("ledgerId", ledgerId)
                                .attr("entries", count.get())
                                .attr("bytes", byteCount.get())
                                .log("Ledger check and copy end with errors");
                    } else {
                        log.info()
                                .attr("event", Events.LEDGER_CHECK_AND_COPY_END)
                                .attr("runId", runId)
                                .attr("metadata", metadata)
                                .attr("ledgerId", ledgerId)
                                .attr("entries", count.get())
                                .attr("bytes", byteCount.get())
                                .attr("errors", 0)
                                .log("Ledger check and copy end");
                    }
                })
            .map(ignore -> ledgerId);
    }

    /**
     * @return the number of bytes copied.
     */
    Maybe<Long> maybeCopyEntry(WriteSets writeSets, NavigableMap<Long, Integer> bookieIndices,
                                     long ledgerId, long entryId, EntryCopier.Batch batch) {
        try {
            if (isEntryMissing(writeSets, bookieIndices, ledgerId, entryId)) {
                return Maybe.fromCompletionStage(batch.copyFromAvailable(entryId));
            } else {
                return Maybe.empty();
            }
        } catch (BookieException | IOException ioe) {
            return Maybe.error(ioe);
        }
    }

    boolean isEntryMissing(WriteSets writeSets, NavigableMap<Long, Integer> bookieIndices,
                           long ledgerId, long entryId) throws IOException, BookieException {
        int bookieIndexForEntry = bookieIndices.floorEntry(entryId).getValue();
        if (bookieIndexForEntry < 0) {
            return false;
        }

        return writeSets.getForEntry(entryId).contains(bookieIndexForEntry)
            && !ledgerStorage.entryExists(ledgerId, entryId);
    }

    static boolean ensemblesContainBookie(LedgerMetadata metadata, BookieId bookieId) {
        return metadata.getAllEnsembles().values().stream()
            .anyMatch(ensemble -> ensemble.contains(bookieId));
    }
}
