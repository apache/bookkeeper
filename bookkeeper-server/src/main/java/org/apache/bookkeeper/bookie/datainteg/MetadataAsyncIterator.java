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

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * An rxjava ledger metadata iterator.
 */
@Slf4j
public class MetadataAsyncIterator {
    private final Scheduler scheduler;
    private final LedgerManager ledgerManager;
    private final long zkTimeoutMs;
    private final int maxInFlight;

    MetadataAsyncIterator(Scheduler scheduler,
                          LedgerManager ledgerManager, int maxInFlight,
                          int zkTimeout, TimeUnit zkTimeoutUnit) {
        this.scheduler = scheduler;
        this.ledgerManager = ledgerManager;
        this.maxInFlight = maxInFlight;
        this.zkTimeoutMs = zkTimeoutUnit.toMillis(zkTimeout);
    }


    private static class FlatIterator {
        final LedgerManager.LedgerRangeIterator ranges;
        Iterator<Long> range = null;
        FlatIterator(LedgerManager.LedgerRangeIterator ranges) {
            this.ranges = ranges;
        }
        boolean hasNext() throws IOException {
            if (range == null || !range.hasNext()) {
                if (ranges.hasNext()) {
                    range = ranges.next().getLedgers().iterator();
                }
            }
            return range != null && range.hasNext();
        }
        Long next() throws IOException {
            return range.next();
        }
    }

    public CompletableFuture<Void> forEach(BiFunction<Long, LedgerMetadata, CompletableFuture<Void>> consumer) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        Flowable.<Long, FlatIterator>generate(
                () -> new FlatIterator(ledgerManager.getLedgerRanges(zkTimeoutMs)),
                (iter, emitter) -> {
                    try {
                        if (iter.hasNext()) {
                            emitter.onNext(iter.next());
                        } else {
                            emitter.onComplete();
                        }
                    } catch (Exception e) {
                        emitter.onError(e);
                    }
                })
            .subscribeOn(scheduler)
            .flatMapCompletable((ledgerId) -> Completable.fromCompletionStage(processOne(ledgerId, consumer)),
                                false /* delayErrors */,
                                maxInFlight)
            .subscribe(() -> promise.complete(null),
                       t -> promise.completeExceptionally(unwrap(t)));
        return promise;
    }

    private CompletableFuture<Void> processOne(long ledgerId,
                                               BiFunction<Long, LedgerMetadata, CompletableFuture<Void>> consumer) {
        return ledgerManager.readLedgerMetadata(ledgerId)
            .thenApply(Versioned::getValue)
            .thenCompose((metadata) -> consumer.apply(ledgerId, metadata))
            .exceptionally((e) -> {
                    Throwable realException = unwrap(e);
                    log.warn("Got exception processing ledger {}", ledgerId, realException);
                    if (realException instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException) {
                        return null;
                    } else {
                        throw new CompletionException(realException);
                    }
                });
    }

    static Throwable unwrap(Throwable e) {
        if (e instanceof CompletionException || e instanceof ExecutionException) {
            return unwrap(e.getCause());
        }
        return e;
    }
}
