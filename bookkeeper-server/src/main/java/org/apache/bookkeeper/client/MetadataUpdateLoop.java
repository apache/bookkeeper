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

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mechanism to safely update the metadata of a ledger.
 *
 * <p>The loop takes the following steps:
 * 1. Check if the metadata needs to be changed.
 * 2. Make a copy of the metadata and modify it.
 * 3. Write the modified copy to zookeeper.
 * 3.1 If the write succeeds, go to 6.
 * 3.2 If the write fails because of a failed compare and swap, go to 4.
 * 4. Read the metadata back from the store
 * 5. Update the local copy of the metadata with the metadata read in 4, go to 1.
 * 6. Update the local copy of the metadata with the metadata which has just been written.
 *
 * <p>All mutating operations are compare and swap operation. If the compare fails, another
 * iteration of the loop begins.
 */
class MetadataUpdateLoop {
    static final Logger LOG = LoggerFactory.getLogger(MetadataUpdateLoop.class);

    private final LedgerManager lm;
    private final long ledgerId;
    private final Supplier<Versioned<LedgerMetadata>> currentLocalValue;
    private final NeedsUpdatePredicate needsTransformation;
    private final MetadataTransform transform;
    private final LocalValueUpdater updateLocalValue;
    private final RateLimiter throttler;

    private final String logContext;
    private volatile int writeLoopCount = 0;
    private static final AtomicIntegerFieldUpdater<MetadataUpdateLoop> WRITE_LOOP_COUNT_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(MetadataUpdateLoop.class, "writeLoopCount");

    interface NeedsUpdatePredicate {
        boolean needsUpdate(LedgerMetadata metadata) throws Exception;
    }

    interface MetadataTransform {
        LedgerMetadata transform(LedgerMetadata metadata) throws Exception;
    }

    interface LocalValueUpdater {
        boolean updateValue(Versioned<LedgerMetadata> oldValue, Versioned<LedgerMetadata> newValue);
    }

    MetadataUpdateLoop(LedgerManager lm,
            long ledgerId,
            Supplier<Versioned<LedgerMetadata>> currentLocalValue,
            NeedsUpdatePredicate needsTransformation,
            MetadataTransform transform,
            LocalValueUpdater updateLocalValue) {
        this(lm, ledgerId, currentLocalValue, needsTransformation, transform, updateLocalValue, null);
    }
    /**
     * Construct the loop. This takes a set of functions which may be called multiple times
     * during the loop.
     *
     * @param lm the ledger manager used for reading and writing metadata
     * @param ledgerId the id of the ledger we will be operating on
     * @param currentLocalValue should return the current local value of the metadata
     * @param needsTransformation should return true, if the metadata needs to be modified.
     *                            should throw an exception, if this update doesn't make sense.
     * @param transform takes a metadata objects, transforms, and returns it, without modifying
     *                  the original
     * @param updateLocalValue if the local value matches the first parameter, update it to the
     *                         second parameter and return true, return false otherwise
     */
    MetadataUpdateLoop(LedgerManager lm,
            long ledgerId,
            Supplier<Versioned<LedgerMetadata>> currentLocalValue,
            NeedsUpdatePredicate needsTransformation,
            MetadataTransform transform,
            LocalValueUpdater updateLocalValue,
            RateLimiter throttler) {
        this.lm = lm;
        this.ledgerId = ledgerId;
        this.currentLocalValue = currentLocalValue;
        this.needsTransformation = needsTransformation;
        this.transform = transform;
        this.updateLocalValue = updateLocalValue;
        this.throttler = throttler;

        this.logContext = String.format("UpdateLoop(ledgerId=%d,loopId=%08x)", ledgerId, System.identityHashCode(this));
    }

    CompletableFuture<Versioned<LedgerMetadata>> run() {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();

        writeLoop(currentLocalValue.get(), promise);

        return promise;
    }

    private void writeLoop(Versioned<LedgerMetadata> currentLocal,
                           CompletableFuture<Versioned<LedgerMetadata>> promise) {
        LOG.debug("{} starting write loop iteration, attempt {}",
                  logContext, WRITE_LOOP_COUNT_UPDATER.incrementAndGet(this));
        try {
            if (needsTransformation.needsUpdate(currentLocal.getValue())) {
                LedgerMetadata transformed = transform.transform(currentLocal.getValue());
                if (throttler != null) {
                    // throttler to control updates per second
                    throttler.acquire();
                }
                lm.writeLedgerMetadata(ledgerId, transformed, currentLocal.getVersion())
                    .whenComplete((writtenMetadata, ex) -> {
                            if (ex == null) {
                                if (updateLocalValue.updateValue(currentLocal, writtenMetadata)) {
                                    LOG.debug("{} success", logContext);
                                    promise.complete(writtenMetadata);
                                } else {
                                    LOG.debug("{} local value changed while we were writing, try again", logContext);
                                    writeLoop(currentLocalValue.get(), promise);
                                }
                            } else if (ex instanceof BKException.BKMetadataVersionException) {
                                LOG.info("{} conflict writing metadata to store, update local value and try again",
                                         logContext);
                                updateLocalValueFromStore(ledgerId).whenComplete((readMetadata, readEx) -> {
                                        if (readEx == null) {
                                            writeLoop(readMetadata, promise);
                                        } else {
                                            promise.completeExceptionally(readEx);
                                        }
                                    });
                            } else {
                                LOG.error("{} Error writing metadata to store", logContext, ex);
                                promise.completeExceptionally(ex);
                            }
                        });
            } else {
                LOG.debug("{} Update not needed, completing", logContext);
                promise.complete(currentLocal);
            }
        } catch (Exception e) {
            LOG.error("{} Exception updating", logContext, e);
            promise.completeExceptionally(e);
        }
    }

    private CompletableFuture<Versioned<LedgerMetadata>> updateLocalValueFromStore(long ledgerId) {
        CompletableFuture<Versioned<LedgerMetadata>> promise = new CompletableFuture<>();

        readLoop(ledgerId, promise);

        return promise;
    }

    private void readLoop(long ledgerId, CompletableFuture<Versioned<LedgerMetadata>> promise) {
        Versioned<LedgerMetadata> current = currentLocalValue.get();

        lm.readLedgerMetadata(ledgerId).whenComplete(
                (read, exception) -> {
                    if (exception != null) {
                        LOG.error("{} Failed to read metadata from store",
                                  logContext, exception);
                        promise.completeExceptionally(exception);
                    } else if (current.getVersion().compare(read.getVersion()) == Version.Occurred.CONCURRENTLY) {
                        // no update needed, these are the same in the immutable world
                        promise.complete(current);
                    } else if (updateLocalValue.updateValue(current, read)) {
                        // updated local value successfully
                        promise.complete(read);
                    } else {
                        // local value changed while we were reading,
                        // look at new value, and try to read again
                        readLoop(ledgerId, promise);
                    }
                });
    }
}
