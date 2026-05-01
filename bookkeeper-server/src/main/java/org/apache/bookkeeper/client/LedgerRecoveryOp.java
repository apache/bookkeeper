/*
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

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.apache.bookkeeper.proto.checksum.DigestManager.RecoveryData;

/**
 * This class encapsulated the ledger recovery operation. It first does a read
 * with entry-id of -1 (BookieProtocol.LAST_ADD_CONFIRMED) to all bookies. Then
 * starting from the last confirmed entry (from hints in the ledger entries),
 * it reads forward until it is not able to find a particular entry.
 */
class LedgerRecoveryOp implements ReadEntryListener, AddCallback {

    private final Logger log;

    final LedgerHandle lh;
    final ClientContext clientCtx;
    final CompletableFuture<LedgerHandle> promise;

    final AtomicLong readCount, writeCount;
    volatile boolean readDone;
    volatile long startEntryToRead;
    volatile long endEntryToRead;

    // keep a copy of metadata for recovery.
    LedgerMetadata metadataForRecovery;

    // EntryListener Hook
    @VisibleForTesting
    ReadEntryListener entryListener = null;

    class RecoveryReadOp extends ListenerBasedPendingReadOp {

        RecoveryReadOp(LedgerHandle lh,
                       ClientContext clientCtx,
                       long startEntryId, long endEntryId,
                       ReadEntryListener cb, Object ctx) {
            super(lh, clientCtx, startEntryId, endEntryId, cb, ctx, true);
        }

        @Override
        protected LedgerMetadata getLedgerMetadata() {
            return metadataForRecovery;
        }

    }

    public LedgerRecoveryOp(LedgerHandle lh, ClientContext clientCtx) {
        this.log = Logger.get(LedgerRecoveryOp.class).with().attr("ledgerId", lh.getId()).build();
        readCount = new AtomicLong(0);
        writeCount = new AtomicLong(0);
        readDone = false;
        this.promise = new CompletableFuture<>();
        this.lh = lh;
        this.clientCtx = clientCtx;
    }

    /**
     * Set an entry listener to listen on individual recovery reads during recovery procedure.
     *
     * @param entryListener
     *          entry listener
     * @return ledger recovery operation
     */
    @VisibleForTesting
    LedgerRecoveryOp setEntryListener(ReadEntryListener entryListener) {
        this.entryListener = entryListener;
        return this;
    }

    public CompletableFuture<LedgerHandle> initiate() {
        ReadLastConfirmedOp rlcop = new ReadLastConfirmedOp(clientCtx.getBookieClient(),
                                                            lh.distributionSchedule,
                                                            lh.macManager,
                                                            lh.ledgerId,
                                                            lh.getCurrentEnsemble(),
                                                            lh.ledgerKey,
                new ReadLastConfirmedOp.LastConfirmedDataCallback() {
                    @Override
                    public void readLastConfirmedDataComplete(int rc, RecoveryData data) {
                        if (rc == BKException.Code.OK) {
                            synchronized (lh) {
                                /**
                                 The lowest an LAC can be for use in recovery is the first entry id
                                 of the current ensemble - 1.
                                 All ensembles prior to the current one, if any, are confirmed and
                                 immutable (so are not part of the recovery process).
                                 So we take the highest of:
                                 - the LAC returned by the current bookie ensemble (could be -1)
                                 - the first entry id of the current ensemble - 1.
                                 */
                                Long lastEnsembleEntryId = lh.getVersionedLedgerMetadata()
                                        .getValue()
                                        .getAllEnsembles()
                                        .lastEntry()
                                        .getKey();

                                lh.lastAddPushed = lh.lastAddConfirmed = Math.max(data.getLastAddConfirmed(),
                                        (lastEnsembleEntryId - 1));

                                lh.length.set(data.getLength());
                                lh.pendingAddsSequenceHead = lh.lastAddConfirmed;
                                startEntryToRead = endEntryToRead = lh.lastAddConfirmed;
                            }
                            // keep a copy of ledger metadata before proceeding
                            // ledger recovery
                            metadataForRecovery = lh.getLedgerMetadata();
                            doRecoveryRead();
                        } else if (rc == BKException.Code.TimeoutException) {
                            submitCallback(rc);
                        } else if (rc == BKException.Code.UnauthorizedAccessException) {
                            submitCallback(rc);
                        } else {
                            submitCallback(BKException.Code.ReadException);
                        }
                    }
                });

        /**
         * Enable fencing on this op. When the read request reaches the bookies
         * server it will fence off the ledger, stopping any subsequent operation
         * from writing to it.
         */
        rlcop.initiateWithFencing();

        return promise;
    }

    private void submitCallback(int rc) {
        if (BKException.Code.OK == rc) {
            clientCtx.getClientStats().getRecoverAddCountLogger().registerSuccessfulValue(writeCount.get());
            clientCtx.getClientStats().getRecoverReadCountLogger().registerSuccessfulValue(readCount.get());
            promise.complete(lh);
        } else {
            clientCtx.getClientStats().getRecoverAddCountLogger().registerFailedValue(writeCount.get());
            clientCtx.getClientStats().getRecoverReadCountLogger().registerFailedValue(readCount.get());
            promise.completeExceptionally(BKException.create(rc));
        }
    }

    /**
     * Try to read past the last confirmed.
     */
    private void doRecoveryRead() {
        if (!promise.isDone()) {
            startEntryToRead = endEntryToRead + 1;
            endEntryToRead = endEntryToRead + clientCtx.getConf().recoveryReadBatchSize;
            new RecoveryReadOp(lh, clientCtx, startEntryToRead, endEntryToRead, this, null)
                .initiate();
        }
    }

    @Override
    public void onEntryComplete(int rc, LedgerHandle lh, LedgerEntry entry, Object ctx) {
        // notify entry listener on individual entries being read during ledger recovery.
        ReadEntryListener listener = entryListener;
        if (null != listener) {
            listener.onEntryComplete(rc, lh, entry, ctx);
        }

        // we only trigger recovery add an entry when readDone == false && callbackDone == false
        if (!promise.isDone() && !readDone && rc == BKException.Code.OK) {
            readCount.incrementAndGet();
            byte[] data = entry.getEntry();

            /*
             * We will add this entry again to make sure it is written to enough
             * replicas. We subtract the length of the data itself, since it will
             * be added again when processing the call to add it.
             */
            synchronized (lh) {
                lh.length.set(entry.getLength() - (long) data.length);
                // check whether entry id is expected, so we won't overwritten any entries by mistake
                if (entry.getEntryId() != lh.lastAddPushed + 1) {
                    log.error()
                            .attr("entryId", entry.getEntryId())
                            .attr("expectedEntryId", (lh.lastAddPushed + 1))
                            .log("Unexpected entryId during recovery add");
                    rc = BKException.Code.UnexpectedConditionException;
                }
            }
            if (BKException.Code.OK == rc) {
                lh.asyncRecoveryAddEntry(data, 0, data.length, this, null);
                if (entry.getEntryId() == endEntryToRead) {
                    // trigger next batch read
                    doRecoveryRead();
                }
                return;
            }
        }

        // no entry found. stop recovery procedure but wait until recovery add finished.
        if (rc == BKException.Code.NoSuchEntryException || rc == BKException.Code.NoSuchLedgerExistsException) {
            readDone = true;
            if (readCount.get() == writeCount.get()) {
                submitCallback(BKException.Code.OK);
            }
            return;
        }

        // otherwise, some other error, we can't handle
        if (BKException.Code.OK != rc && !promise.isDone()) {
            log.error()
                    .attr("getMessage", BKException.getMessage(rc))
                    .attr("startEntryId", startEntryToRead)
                    .attr("endEntryId", endEntryToRead)
                    .attr("ledgerId", lh.getId())
                    .log("Failure while reading entries: ( - ), ledger: while recovering ledger");
            submitCallback(rc);
        } else if (BKException.Code.OK == rc) {
            // we are here is because we successfully read an entry but readDone was already set to true.
            // this would happen on recovery a ledger than has gaps in the tail.
            log.warn()
                    .attr("entryId", entry.getEntryId())
                    .attr("ledgerId", lh.getId())
                    .attr("readDone", readDone)
                    .log("Successfully read entry, but readDone is already set");
        }
        return;
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        if (rc != BKException.Code.OK) {
            log.error()
                    .attr("codeLogger", BKException.codeLogger(rc))
                    .attr("entryId", entryId + 1)
                    .attr("ledgerId", lh.ledgerId)
                    .log("Failure while writing entry while recovering ledger");
            submitCallback(rc);
            return;
        }
        long numAdd = writeCount.incrementAndGet();
        if (readDone && readCount.get() == numAdd) {
            submitCallback(rc);
        }
    }

}
