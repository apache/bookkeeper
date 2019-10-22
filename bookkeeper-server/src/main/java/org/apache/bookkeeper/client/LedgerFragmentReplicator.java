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

import static org.apache.bookkeeper.client.LedgerHandle.INVALID_ENTRY_ID;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BYTES_READ;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_BYTES_WRITTEN;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_READ;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_WRITTEN;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;

import io.netty.buffer.Unpooled;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.zookeeper.AsyncCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the helper class for replicating the fragments from one bookie to
 * another.
 */
@StatsDoc(
    name = REPLICATION_WORKER_SCOPE,
    help = "Ledger fragment replicator related stats"
)
public class LedgerFragmentReplicator {

    // BookKeeper instance
    private BookKeeper bkc;
    private StatsLogger statsLogger;
    @StatsDoc(
        name = NUM_ENTRIES_READ,
        help = "Number of entries read by the replicator"
    )
    private final Counter numEntriesRead;
    @StatsDoc(
        name = NUM_BYTES_READ,
        help = "The distribution of size of entries read by the replicator"
    )
    private final OpStatsLogger numBytesRead;
    @StatsDoc(
        name = NUM_ENTRIES_WRITTEN,
        help = "Number of entries written by the replicator"
    )
    private final Counter numEntriesWritten;
    @StatsDoc(
        name = NUM_BYTES_WRITTEN,
        help = "The distribution of size of entries written by the replicator"
    )
    private final OpStatsLogger numBytesWritten;

    public LedgerFragmentReplicator(BookKeeper bkc, StatsLogger statsLogger) {
        this.bkc = bkc;
        this.statsLogger = statsLogger;
        numEntriesRead = this.statsLogger.getCounter(NUM_ENTRIES_READ);
        numBytesRead = this.statsLogger.getOpStatsLogger(NUM_BYTES_READ);
        numEntriesWritten = this.statsLogger.getCounter(NUM_ENTRIES_WRITTEN);
        numBytesWritten = this.statsLogger.getOpStatsLogger(NUM_BYTES_WRITTEN);
    }

    public LedgerFragmentReplicator(BookKeeper bkc) {
        this(bkc, NullStatsLogger.INSTANCE);
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(LedgerFragmentReplicator.class);

    private void replicateFragmentInternal(final LedgerHandle lh,
            final LedgerFragment lf,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final Set<BookieSocketAddress> newBookies,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) throws InterruptedException {
        if (!lf.isClosed()) {
            LOG.error("Trying to replicate an unclosed fragment;"
                      + " This is not safe {}", lf);
            ledgerFragmentMcb.processResult(BKException.Code.UnclosedFragmentException,
                                            null, null);
            return;
        }
        Long startEntryId = lf.getFirstStoredEntryId();
        Long endEntryId = lf.getLastStoredEntryId();

        /*
         * if startEntryId is INVALID_ENTRY_ID then endEntryId should be
         * INVALID_ENTRY_ID and viceversa.
         */
        if (startEntryId == INVALID_ENTRY_ID ^ endEntryId == INVALID_ENTRY_ID) {
            LOG.error("For LedgerFragment: {}, seeing inconsistent firstStoredEntryId: {} and lastStoredEntryId: {}",
                    lf, startEntryId, endEntryId);
            assert false;
        }

        if (startEntryId > endEntryId || endEntryId <= INVALID_ENTRY_ID) {
            // for open ledger which there is no entry, the start entry id is 0,
            // the end entry id is -1.
            // we can return immediately to trigger forward read
            ledgerFragmentMcb.processResult(BKException.Code.OK, null, null);
            return;
        }

        /*
         * Add all the entries to entriesToReplicate list from
         * firstStoredEntryId to lastStoredEntryID.
         */
        List<Long> entriesToReplicate = new LinkedList<Long>();
        long lastStoredEntryId = lf.getLastStoredEntryId();
        for (long i = lf.getFirstStoredEntryId(); i <= lastStoredEntryId; i++) {
            entriesToReplicate.add(i);
        }
        /*
         * Now asynchronously replicate all of the entries for the ledger
         * fragment that were on the dead bookie.
         */
        MultiCallback ledgerFragmentEntryMcb = new MultiCallback(
                entriesToReplicate.size(), ledgerFragmentMcb, null, BKException.Code.OK,
                BKException.Code.LedgerRecoveryException);
        for (final Long entryId : entriesToReplicate) {
            recoverLedgerFragmentEntry(entryId, lh, ledgerFragmentEntryMcb,
                    newBookies, onReadEntryFailureCallback);
        }
    }

    /**
     * This method replicate a ledger fragment which is a contiguous portion of
     * a ledger that was stored in an ensemble that included the failed bookie.
     * It will Splits the fragment into multiple sub fragments by keeping the
     * max entries up to the configured value of rereplicationEntryBatchSize and
     * then it re-replicates that batched entry fragments one by one. After
     * re-replication of all batched entry fragments, it will update the
     * ensemble info with new Bookie once
     *
     * @param lh
     *            LedgerHandle for the ledger
     * @param lf
     *            LedgerFragment to replicate
     * @param ledgerFragmentMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param targetBookieAddresses
     *            New bookies we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    void replicate(final LedgerHandle lh, final LedgerFragment lf,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final Set<BookieSocketAddress> targetBookieAddresses,
            final BiConsumer<Long, Long> onReadEntryFailureCallback)
            throws InterruptedException {
        Set<LedgerFragment> partionedFragments = splitIntoSubFragments(lh, lf,
                bkc.getConf().getRereplicationEntryBatchSize());
        LOG.info("Replicating fragment {} in {} sub fragments.",
                lf, partionedFragments.size());
        replicateNextBatch(lh, partionedFragments.iterator(),
                ledgerFragmentMcb, targetBookieAddresses, onReadEntryFailureCallback);
    }

    /**
     * Replicate the batched entry fragments one after other.
     */
    private void replicateNextBatch(final LedgerHandle lh,
            final Iterator<LedgerFragment> fragments,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final Set<BookieSocketAddress> targetBookieAddresses,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) {
        if (fragments.hasNext()) {
            try {
                replicateFragmentInternal(lh, fragments.next(),
                        new AsyncCallback.VoidCallback() {
                            @Override
                            public void processResult(int rc, String v, Object ctx) {
                                if (rc != BKException.Code.OK) {
                                    ledgerFragmentMcb.processResult(rc, null,
                                            null);
                                } else {
                                    replicateNextBatch(lh, fragments,
                                            ledgerFragmentMcb,
                                            targetBookieAddresses,
                                            onReadEntryFailureCallback);
                                }
                            }

                        }, targetBookieAddresses, onReadEntryFailureCallback);
            } catch (InterruptedException e) {
                ledgerFragmentMcb.processResult(
                        BKException.Code.InterruptedException, null, null);
                Thread.currentThread().interrupt();
            }
        } else {
            ledgerFragmentMcb.processResult(BKException.Code.OK, null, null);
        }
    }

    /**
     * Split the full fragment into batched entry fragments by keeping
     * rereplicationEntryBatchSize of entries in each one and can treat them as
     * sub fragments.
     */
    static Set<LedgerFragment> splitIntoSubFragments(LedgerHandle lh,
            LedgerFragment ledgerFragment, long rereplicationEntryBatchSize) {
        Set<LedgerFragment> fragments = new HashSet<LedgerFragment>();
        if (rereplicationEntryBatchSize <= 0) {
            // rereplicationEntryBatchSize can not be 0 or less than 0,
            // returning with the current fragment
            fragments.add(ledgerFragment);
            return fragments;
        }

        long firstEntryId = ledgerFragment.getFirstStoredEntryId();
        long lastEntryId = ledgerFragment.getLastStoredEntryId();

        /*
         * if firstEntryId is INVALID_ENTRY_ID then lastEntryId should be
         * INVALID_ENTRY_ID and viceversa.
         */
        if (firstEntryId == INVALID_ENTRY_ID ^ lastEntryId == INVALID_ENTRY_ID) {
            LOG.error("For LedgerFragment: {}, seeing inconsistent firstStoredEntryId: {} and lastStoredEntryId: {}",
                    ledgerFragment, firstEntryId, lastEntryId);
            assert false;
        }

        long numberOfEntriesToReplicate = (lastEntryId - firstEntryId) + 1;
        long splitsWithFullEntries = numberOfEntriesToReplicate
                / rereplicationEntryBatchSize;

        if (splitsWithFullEntries == 0) {// only one fragment
            fragments.add(ledgerFragment);
            return fragments;
        }

        long fragmentSplitLastEntry = 0;
        for (int i = 0; i < splitsWithFullEntries; i++) {
            fragmentSplitLastEntry = (firstEntryId + rereplicationEntryBatchSize) - 1;
            fragments.add(new LedgerFragment(lh, firstEntryId,
                    fragmentSplitLastEntry, ledgerFragment.getBookiesIndexes()));
            firstEntryId = fragmentSplitLastEntry + 1;
        }

        long lastSplitWithPartialEntries = numberOfEntriesToReplicate
                % rereplicationEntryBatchSize;
        if (lastSplitWithPartialEntries > 0) {
            fragments.add(new LedgerFragment(lh, firstEntryId, firstEntryId
                    + lastSplitWithPartialEntries - 1, ledgerFragment
                    .getBookiesIndexes()));
        }
        return fragments;
    }

    /**
     * This method asynchronously recovers a specific ledger entry by reading
     * the values via the BookKeeper Client (which would read it from the other
     * replicas) and then writing it to the chosen new bookie.
     *
     * @param entryId
     *            Ledger Entry ID to recover.
     * @param lh
     *            LedgerHandle for the ledger
     * @param ledgerFragmentEntryMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger entry.
     * @param newBookies
     *            New bookies we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    private void recoverLedgerFragmentEntry(final Long entryId,
            final LedgerHandle lh,
            final AsyncCallback.VoidCallback ledgerFragmentEntryMcb,
            final Set<BookieSocketAddress> newBookies,
            final BiConsumer<Long, Long> onReadEntryFailureCallback) throws InterruptedException {
        final long ledgerId = lh.getId();
        final AtomicInteger numCompleted = new AtomicInteger(0);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final WriteCallback multiWriteCallback = new WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                if (rc != BKException.Code.OK) {
                    LOG.error("BK error writing entry for ledgerId: {}, entryId: {}, bookie: {}",
                            ledgerId, entryId, addr, BKException.create(rc));
                    if (completed.compareAndSet(false, true)) {
                        ledgerFragmentEntryMcb.processResult(rc, null, null);
                    }
                } else {
                    numEntriesWritten.inc();
                    if (ctx instanceof Long) {
                        numBytesWritten.registerSuccessfulValue((Long) ctx);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Success writing ledger id {}, entry id {} to a new bookie {}!",
                                ledgerId, entryId, addr);
                    }
                    if (numCompleted.incrementAndGet() == newBookies.size() && completed.compareAndSet(false, true)) {
                        ledgerFragmentEntryMcb.processResult(rc, null, null);
                    }
                }
            }
        };
        /*
         * Read the ledger entry using the LedgerHandle. This will allow us to
         * read the entry from one of the other replicated bookies other than
         * the dead one.
         */
        lh.asyncReadEntries(entryId, entryId, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh,
                    Enumeration<LedgerEntry> seq, Object ctx) {
                if (rc != BKException.Code.OK) {
                    LOG.error("BK error reading ledger entry: " + entryId,
                            BKException.create(rc));
                    onReadEntryFailureCallback.accept(ledgerId, entryId);
                    ledgerFragmentEntryMcb.processResult(rc, null, null);
                    return;
                }
                /*
                 * Now that we've read the ledger entry, write it to the new
                 * bookie we've selected.
                 */
                LedgerEntry entry = seq.nextElement();
                byte[] data = entry.getEntry();
                final long dataLength = data.length;
                numEntriesRead.inc();
                numBytesRead.registerSuccessfulValue(dataLength);
                ByteBufList toSend = lh.getDigestManager()
                        .computeDigestAndPackageForSending(entryId,
                                lh.getLastAddConfirmed(), entry.getLength(),
                                Unpooled.wrappedBuffer(data, 0, data.length));
                for (BookieSocketAddress newBookie : newBookies) {
                    bkc.getBookieClient().addEntry(newBookie, lh.getId(),
                            lh.getLedgerKey(), entryId, ByteBufList.clone(toSend),
                            multiWriteCallback, dataLength, BookieProtocol.FLAG_RECOVERY_ADD,
                            false, WriteFlag.NONE);
                }
                toSend.release();
            }
        }, null);
    }

    /**
     * Callback for recovery of a single ledger fragment. Once the fragment has
     * had all entries replicated, update the ensemble in zookeeper. Once
     * finished propogate callback up to ledgerFragmentsMcb which should be a
     * multicallback responsible for all fragments in a single ledger
     */
    static class SingleFragmentCallback implements AsyncCallback.VoidCallback {
        final AsyncCallback.VoidCallback ledgerFragmentsMcb;
        final LedgerHandle lh;
        final LedgerManager ledgerManager;
        final long fragmentStartId;
        final Map<BookieSocketAddress, BookieSocketAddress> oldBookie2NewBookie;

        SingleFragmentCallback(AsyncCallback.VoidCallback ledgerFragmentsMcb,
                               LedgerHandle lh, LedgerManager ledgerManager, long fragmentStartId,
                               Map<BookieSocketAddress, BookieSocketAddress> oldBookie2NewBookie) {
            this.ledgerFragmentsMcb = ledgerFragmentsMcb;
            this.lh = lh;
            this.ledgerManager = ledgerManager;
            this.fragmentStartId = fragmentStartId;
            this.oldBookie2NewBookie = oldBookie2NewBookie;
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != BKException.Code.OK) {
                LOG.error("BK error replicating ledger fragments for ledger: "
                        + lh.getId(), BKException.create(rc));
                ledgerFragmentsMcb.processResult(rc, null, null);
                return;
            }
            updateEnsembleInfo(ledgerManager, ledgerFragmentsMcb, fragmentStartId, lh, oldBookie2NewBookie);
        }
    }

    /**
     * Updates the ensemble with newBookie and notify the ensembleUpdatedCb.
     */
    private static void updateEnsembleInfo(
            LedgerManager ledgerManager, AsyncCallback.VoidCallback ensembleUpdatedCb, long fragmentStartId,
            LedgerHandle lh, Map<BookieSocketAddress, BookieSocketAddress> oldBookie2NewBookie) {

        MetadataUpdateLoop updateLoop = new MetadataUpdateLoop(
                ledgerManager,
                lh.getId(),
                lh::getVersionedLedgerMetadata,
                (metadata) -> {
                    // returns true if any of old bookies exist in ensemble
                    List<BookieSocketAddress> ensemble = metadata.getAllEnsembles().get(fragmentStartId);
                    return oldBookie2NewBookie.keySet().stream().anyMatch(ensemble::contains);
                },
                (currentMetadata) -> {
                    // replace all old bookies with new bookies in ensemble
                    List<BookieSocketAddress> newEnsemble = currentMetadata.getAllEnsembles().get(fragmentStartId)
                        .stream().map((bookie) -> oldBookie2NewBookie.getOrDefault(bookie, bookie))
                        .collect(Collectors.toList());
                    return LedgerMetadataBuilder.from(currentMetadata)
                        .replaceEnsembleEntry(fragmentStartId, newEnsemble).build();
                },
                lh::setLedgerMetadata);

        updateLoop.run().whenComplete((result, ex) -> {
                if (ex == null) {
                    LOG.info("Updated ZK for ledgerId: ({}:{}) to point ledger fragments"
                             + " from old bookies to new bookies: {}", oldBookie2NewBookie);

                    ensembleUpdatedCb.processResult(BKException.Code.OK, null, null);
                } else {
                    LOG.error("Error updating ledger config metadata for ledgerId {}", lh.getId(), ex);

                    ensembleUpdatedCb.processResult(
                            BKException.getExceptionCode(ex, BKException.Code.UnexpectedConditionException),
                            null, null);
                }
            });
    }
}
