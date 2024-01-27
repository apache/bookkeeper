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

import com.google.common.util.concurrent.ListenableFuture;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReadOpBase implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReadOpBase.class);

    protected final CompletableFuture<LedgerEntries> future;
    protected final Set<BookieId> heardFromHosts;
    protected final BitSet heardFromHostsBitSet;
    protected final Set<BookieId> sentToHosts = new HashSet<BookieId>();
    LedgerHandle lh;
    protected ClientContext clientCtx;

    protected final long startEntryId;
    protected long requestTimeNanos;

    protected final int requiredBookiesMissingEntryForRecovery;
    protected final boolean isRecoveryRead;

    protected boolean parallelRead = false;
    protected final AtomicBoolean complete = new AtomicBoolean(false);
    protected boolean allowFailFast = false;
    long numPendingEntries;
    final long endEntryId;

    protected ReadOpBase(LedgerHandle lh, ClientContext clientCtx, long startEntryId, long endEntryId,
                         boolean isRecoveryRead) {
        this.lh = lh;
        this.future = new CompletableFuture<>();
        this.startEntryId = startEntryId;
        this.endEntryId = endEntryId;
        this.isRecoveryRead = isRecoveryRead;
        this.requiredBookiesMissingEntryForRecovery = getLedgerMetadata().getWriteQuorumSize()
                - getLedgerMetadata().getAckQuorumSize() + 1;
        this.heardFromHosts = new HashSet<>();
        this.heardFromHostsBitSet = new BitSet(getLedgerMetadata().getEnsembleSize());
        this.allowFailFast = false;
        this.clientCtx = clientCtx;
    }

    protected LedgerMetadata getLedgerMetadata() {
        return lh.getLedgerMetadata();
    }

    CompletableFuture<LedgerEntries> future() {
        return future;
    }

    void allowFailFastOnUnwritableChannel() {
        allowFailFast = true;
    }

    public void submit() {
        clientCtx.getMainWorkerPool().executeOrdered(lh.ledgerId, this);
    }

    @Override
    public void run() {
        initiate();
    }

    abstract void initiate();

    protected abstract void submitCallback(int code);

    abstract class LedgerEntryRequest implements SpeculativeRequestExecutor {

        final AtomicBoolean complete = new AtomicBoolean(false);

        int rc = BKException.Code.OK;
        int firstError = BKException.Code.OK;
        int numBookiesMissingEntry = 0;

        final long eId;

        final List<BookieId> ensemble;
        final DistributionSchedule.WriteSet writeSet;


        LedgerEntryRequest(List<BookieId> ensemble, final long eId) {
            this.ensemble = ensemble;
            this.eId = eId;
            if (clientCtx.getConf().enableReorderReadSequence) {
                writeSet = clientCtx.getPlacementPolicy()
                        .reorderReadSequence(
                                ensemble,
                                lh.getBookiesHealthInfo(),
                                lh.getWriteSetForReadOperation(eId));
            } else {
                writeSet = lh.getWriteSetForReadOperation(eId);
            }
        }

        public void close() {
            // this request has succeeded before, can't recycle writeSet again
            if (complete.compareAndSet(false, true)) {
                rc = BKException.Code.UnexpectedConditionException;
                writeSet.recycle();
            }
        }

        /**
         * Execute the read request.
         */
        abstract void read();

        /**
         * Fail the request with given result code <i>rc</i>.
         *
         * @param rc
         *          result code to fail the request.
         * @return true if we managed to fail the entry; otherwise return false if it already failed or completed.
         */
        boolean fail(int rc) {
            if (complete.compareAndSet(false, true)) {
                this.rc = rc;
                writeSet.recycle();
                submitCallback(rc);
                return true;
            } else {
                return false;
            }
        }

        /**
         * Log error <i>errMsg</i> and reattempt read from <i>host</i>.
         *
         * @param bookieIndex
         *          bookie index
         * @param host
         *          host that just respond
         * @param errMsg
         *          error msg to log
         * @param rc
         *          read result code
         */
        synchronized void logErrorAndReattemptRead(int bookieIndex, BookieId host, String errMsg, int rc) {
            if (BKException.Code.OK == firstError
                    || BKException.Code.NoSuchEntryException == firstError
                    || BKException.Code.NoSuchLedgerExistsException == firstError) {
                firstError = rc;
            } else if (BKException.Code.BookieHandleNotAvailableException == firstError
                    && BKException.Code.NoSuchEntryException != rc
                    && BKException.Code.NoSuchLedgerExistsException != rc) {
                // if other exception rather than NoSuchEntryException or NoSuchLedgerExistsException is
                // returned we need to update firstError to indicate that it might be a valid read but just
                // failed.
                firstError = rc;
            }
            if (BKException.Code.NoSuchEntryException == rc
                    || BKException.Code.NoSuchLedgerExistsException == rc) {
                ++numBookiesMissingEntry;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No such entry found on bookie.  L{} E{} bookie: {}",
                            lh.ledgerId, eId, host);
                }
            } else {
                if (LOG.isInfoEnabled()) {
                    LOG.info("{} while reading L{} E{} from bookie: {}",
                            errMsg, lh.ledgerId, eId, host);
                }
            }

            lh.recordReadErrorOnBookie(bookieIndex);
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         *
         * @param heardFromHostsBitSet
         *      the set of hosts that we already received responses.
         * @return host we sent to if we sent. null otherwise.
         */
        abstract BookieId maybeSendSpeculativeRead(BitSet heardFromHostsBitSet);

        /**
         * Whether the read request completed.
         *
         * @return true if the read request is completed.
         */
        boolean isComplete() {
            return complete.get();
        }

        /**
         * Get result code of this entry.
         *
         * @return result code.
         */
        int getRc() {
            return rc;
        }

        @Override
        public String toString() {
            return String.format("L%d-E%d", lh.getId(), eId);
        }

        /**
         * Issues a speculative request and indicates if more speculative
         * requests should be issued.
         *
         * @return whether more speculative requests should be issued
         */
        @Override
        public ListenableFuture<Boolean> issueSpeculativeRequest() {
            return clientCtx.getMainWorkerPool().submitOrdered(lh.getId(), new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    if (!isComplete() && null != maybeSendSpeculativeRead(heardFromHostsBitSet)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Send speculative read for {}. Hosts sent are {}, "
                                            + " Hosts heard are {}, ensemble is {}.",
                                    this, sentToHosts, heardFromHostsBitSet, ensemble);
                        }
                        return true;
                    }
                    return false;
                }
            });
        }
    }

    protected static class ReadContext implements BookkeeperInternalCallbacks.ReadEntryCallbackCtx {
        final int bookieIndex;
        final BookieId to;
        final PendingReadOp.LedgerEntryRequest entry;
        long lac = LedgerHandle.INVALID_ENTRY_ID;

        ReadContext(int bookieIndex, BookieId to, PendingReadOp.LedgerEntryRequest entry) {
            this.bookieIndex = bookieIndex;
            this.to = to;
            this.entry = entry;
        }

        @Override
        public void setLastAddConfirmed(long lac) {
            this.lac = lac;
        }

        @Override
        public long getLastAddConfirmed() {
            return lac;
        }
    }
}
