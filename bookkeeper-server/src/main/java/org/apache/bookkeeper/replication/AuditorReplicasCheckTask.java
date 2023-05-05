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
package org.apache.bookkeeper.replication;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditorReplicasCheckTask extends AuditorTask {
    private static final Logger LOG = LoggerFactory.getLogger(AuditorReplicasCheckTask.class);

    private static final int MAX_CONCURRENT_REPLICAS_CHECK_LEDGER_REQUESTS = 100;
    private static final int REPLICAS_CHECK_TIMEOUT_IN_SECS = 120;
    private static final BitSet EMPTY_BITSET = new BitSet();

    private final int zkOpTimeoutMs;

    private final AtomicInteger numLedgersFoundHavingNoReplicaOfAnEntry;
    private final AtomicInteger numLedgersFoundHavingLessThanAQReplicasOfAnEntry;
    private final AtomicInteger numLedgersFoundHavingLessThanWQReplicasOfAnEntry;

    AuditorReplicasCheckTask(ServerConfiguration conf,
                             AuditorStats auditorStats, BookKeeperAdmin admin,
                             LedgerManager ledgerManager,
                             LedgerUnderreplicationManager ledgerUnderreplicationManager,
                             ShutdownTaskHandler shutdownTaskHandler,
                             BiConsumer<AtomicBoolean, Throwable> hasAuditCheckTask) {
        super(conf, auditorStats, admin, ledgerManager,
                ledgerUnderreplicationManager, shutdownTaskHandler, hasAuditCheckTask);
        this.zkOpTimeoutMs = conf.getZkTimeout() * 2;
        this.numLedgersFoundHavingNoReplicaOfAnEntry = new AtomicInteger(0);
        this.numLedgersFoundHavingLessThanAQReplicasOfAnEntry = new AtomicInteger(0);
        this.numLedgersFoundHavingLessThanWQReplicasOfAnEntry = new AtomicInteger(0);
    }

    @Override
    protected void runTask() {
        if (hasBookieCheckTask()) {
            LOG.info("Audit bookie task already scheduled; skipping periodic replicas check task");
            auditorStats.getNumSkippingCheckTaskTimes().inc();
            return;
        }

        try {
            if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                LOG.info("Ledger replication disabled, skipping replicasCheck task.");
                return;
            }
            Stopwatch stopwatch = Stopwatch.createStarted();
            LOG.info("Starting ReplicasCheck");
            replicasCheck();
            long replicasCheckDuration = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            int numLedgersFoundHavingNoReplicaOfAnEntryValue =
                    numLedgersFoundHavingNoReplicaOfAnEntry.get();
            int numLedgersFoundHavingLessThanAQReplicasOfAnEntryValue =
                    numLedgersFoundHavingLessThanAQReplicasOfAnEntry.get();
            int numLedgersFoundHavingLessThanWQReplicasOfAnEntryValue =
                    numLedgersFoundHavingLessThanWQReplicasOfAnEntry.get();
            LOG.info(
                    "Completed ReplicasCheck in {} milliSeconds numLedgersFoundHavingNoReplicaOfAnEntry {}"
                            + " numLedgersFoundHavingLessThanAQReplicasOfAnEntry {}"
                            + " numLedgersFoundHavingLessThanWQReplicasOfAnEntry {}.",
                    replicasCheckDuration, numLedgersFoundHavingNoReplicaOfAnEntryValue,
                    numLedgersFoundHavingLessThanAQReplicasOfAnEntryValue,
                    numLedgersFoundHavingLessThanWQReplicasOfAnEntryValue);
            auditorStats.getNumLedgersHavingNoReplicaOfAnEntryGuageValue()
                    .set(numLedgersFoundHavingNoReplicaOfAnEntryValue);
            auditorStats.getNumLedgersHavingLessThanAQReplicasOfAnEntryGuageValue()
                    .set(numLedgersFoundHavingLessThanAQReplicasOfAnEntryValue);
            auditorStats.getNumLedgersHavingLessThanWQReplicasOfAnEntryGuageValue()
                    .set(numLedgersFoundHavingLessThanWQReplicasOfAnEntryValue);
            auditorStats.getReplicasCheckTime().registerSuccessfulEvent(
                    replicasCheckDuration, TimeUnit.MILLISECONDS);
        } catch (ReplicationException.BKAuditException e) {
            LOG.error("BKAuditException running periodic replicas check.", e);
            int numLedgersFoundHavingNoReplicaOfAnEntryValue =
                    numLedgersFoundHavingNoReplicaOfAnEntry.get();
            if (numLedgersFoundHavingNoReplicaOfAnEntryValue > 0) {
                /*
                 * Though there is BKAuditException while doing
                 * replicasCheck, it found few ledgers having no replica
                 * of an entry. So reporting it.
                 */
                auditorStats.getNumLedgersHavingNoReplicaOfAnEntryGuageValue()
                        .set(numLedgersFoundHavingNoReplicaOfAnEntryValue);
            }
            int numLedgersFoundHavingLessThanAQReplicasOfAnEntryValue =
                    numLedgersFoundHavingLessThanAQReplicasOfAnEntry.get();
            if (numLedgersFoundHavingLessThanAQReplicasOfAnEntryValue > 0) {
                /*
                 * Though there is BKAuditException while doing
                 * replicasCheck, it found few ledgers having an entry
                 * less than AQ num of Replicas. So reporting it.
                 */
                auditorStats.getNumLedgersHavingLessThanAQReplicasOfAnEntryGuageValue()
                        .set(numLedgersFoundHavingLessThanAQReplicasOfAnEntryValue);
            }
            int numLedgersFoundHavingLessThanWQReplicasOfAnEntryValue =
                    numLedgersFoundHavingLessThanWQReplicasOfAnEntry.get();
            if (numLedgersFoundHavingLessThanWQReplicasOfAnEntryValue > 0) {
                /*
                 * Though there is BKAuditException while doing
                 * replicasCheck, it found few ledgers having an entry
                 * less than WQ num of Replicas. So reporting it.
                 */
                auditorStats.getNumLedgersHavingLessThanWQReplicasOfAnEntryGuageValue()
                        .set(numLedgersFoundHavingLessThanWQReplicasOfAnEntryValue);
            }
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Underreplication manager unavailable running periodic check", ue);
        }
    }

    @Override
    public void shutdown() {

    }

    void replicasCheck() throws ReplicationException.BKAuditException {
        ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithMissingEntries =
                new ConcurrentHashMap<Long, MissingEntriesInfoOfLedger>();
        ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithUnavailableBookies =
                new ConcurrentHashMap<Long, MissingEntriesInfoOfLedger>();
        LedgerManager.LedgerRangeIterator ledgerRangeIterator = ledgerManager.getLedgerRanges(zkOpTimeoutMs);
        final Semaphore maxConcurrentSemaphore = new Semaphore(MAX_CONCURRENT_REPLICAS_CHECK_LEDGER_REQUESTS);
        while (true) {
            LedgerManager.LedgerRange ledgerRange = null;
            try {
                if (ledgerRangeIterator.hasNext()) {
                    ledgerRange = ledgerRangeIterator.next();
                } else {
                    break;
                }
            } catch (IOException ioe) {
                LOG.error("Got IOException while iterating LedgerRangeIterator", ioe);
                throw new ReplicationException.BKAuditException(
                        "Got IOException while iterating LedgerRangeIterator", ioe);
            }
            ledgersWithMissingEntries.clear();
            ledgersWithUnavailableBookies.clear();
            numLedgersFoundHavingNoReplicaOfAnEntry.set(0);
            numLedgersFoundHavingLessThanAQReplicasOfAnEntry.set(0);
            numLedgersFoundHavingLessThanWQReplicasOfAnEntry.set(0);
            Set<Long> ledgersInRange = ledgerRange.getLedgers();
            int numOfLedgersInRange = ledgersInRange.size();
            // Final result after processing all the ledgers
            final AtomicInteger resultCode = new AtomicInteger();
            final CountDownLatch replicasCheckLatch = new CountDownLatch(1);

            ReplicasCheckFinalCallback finalCB = new ReplicasCheckFinalCallback(resultCode, replicasCheckLatch);
            MultiCallback mcbForThisLedgerRange = new MultiCallback(numOfLedgersInRange, finalCB, null,
                    BKException.Code.OK, BKException.Code.ReadException) {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    try {
                        super.processResult(rc, path, ctx);
                    } finally {
                        maxConcurrentSemaphore.release();
                    }
                }
            };
            if (LOG.isDebugEnabled()) {
                LOG.debug("Number of ledgers in the current LedgerRange : {}",
                        numOfLedgersInRange);
            }
            for (Long ledgerInRange : ledgersInRange) {
                try {
                    if (!maxConcurrentSemaphore.tryAcquire(REPLICAS_CHECK_TIMEOUT_IN_SECS, TimeUnit.SECONDS)) {
                        LOG.error("Timedout ({} secs) while waiting for acquiring semaphore",
                                REPLICAS_CHECK_TIMEOUT_IN_SECS);
                        throw new ReplicationException.BKAuditException(
                                "Timedout while waiting for acquiring semaphore");
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.error("Got InterruptedException while acquiring semaphore for replicascheck", ie);
                    throw new ReplicationException.BKAuditException(
                            "Got InterruptedException while acquiring semaphore for replicascheck", ie);
                }
                if (checkUnderReplicationForReplicasCheck(ledgerInRange, mcbForThisLedgerRange)) {
                    /*
                     * if ledger is marked underreplicated, then ignore this
                     * ledger for replicascheck.
                     */
                    continue;
                }
                ledgerManager.readLedgerMetadata(ledgerInRange)
                        .whenComplete(new ReadLedgerMetadataCallbackForReplicasCheck(ledgerInRange,
                                mcbForThisLedgerRange, ledgersWithMissingEntries, ledgersWithUnavailableBookies));
            }
            try {
                /*
                 * if mcbForThisLedgerRange is not calledback within
                 * REPLICAS_CHECK_TIMEOUT_IN_SECS secs then better give up
                 * doing replicascheck, since there could be an issue and
                 * blocking the single threaded auditor executor thread is not
                 * expected.
                 */
                if (!replicasCheckLatch.await(REPLICAS_CHECK_TIMEOUT_IN_SECS, TimeUnit.SECONDS)) {
                    LOG.error(
                            "For LedgerRange with num of ledgers : {} it didn't complete replicascheck"
                                    + " in {} secs, so giving up",
                            numOfLedgersInRange, REPLICAS_CHECK_TIMEOUT_IN_SECS);
                    throw new ReplicationException.BKAuditException(
                            "Got InterruptedException while doing replicascheck");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Got InterruptedException while doing replicascheck", ie);
                throw new ReplicationException.BKAuditException(
                        "Got InterruptedException while doing replicascheck", ie);
            }
            reportLedgersWithMissingEntries(ledgersWithMissingEntries);
            reportLedgersWithUnavailableBookies(ledgersWithUnavailableBookies);
            int resultCodeIntValue = resultCode.get();
            if (resultCodeIntValue != BKException.Code.OK) {
                throw new ReplicationException.BKAuditException("Exception while doing replicas check",
                        BKException.create(resultCodeIntValue));
            }
        }
        try {
            ledgerUnderreplicationManager.setReplicasCheckCTime(System.currentTimeMillis());
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            LOG.error("Non Recoverable Exception while reading from ZK", nre);
            submitShutdownTask();
        } catch (ReplicationException.UnavailableException ue) {
            LOG.error("Got exception while trying to set ReplicasCheckCTime", ue);
        }
    }

    private static class MissingEntriesInfo {
        // ledger id of missing entries
        private final long ledgerId;
        /*
         * segment details, like start entryid of the segment and ensemble List.
         */
        private final Entry<Long, ? extends List<BookieId>> segmentEnsemble;
        // bookie missing these entries
        private final BookieId bookieMissingEntries;
        /*
         * entries of this segment which are supposed to contain in this bookie
         * but missing in this bookie.
         */
        private final List<Long> unavailableEntriesList;

        private MissingEntriesInfo(long ledgerId, Entry<Long, ? extends List<BookieId>> segmentEnsemble,
                                   BookieId bookieMissingEntries, List<Long> unavailableEntriesList) {
            this.ledgerId = ledgerId;
            this.segmentEnsemble = segmentEnsemble;
            this.bookieMissingEntries = bookieMissingEntries;
            this.unavailableEntriesList = unavailableEntriesList;
        }

        private long getLedgerId() {
            return ledgerId;
        }

        private Entry<Long, ? extends List<BookieId>> getSegmentEnsemble() {
            return segmentEnsemble;
        }

        private BookieId getBookieMissingEntries() {
            return bookieMissingEntries;
        }

        private List<Long> getUnavailableEntriesList() {
            return unavailableEntriesList;
        }
    }

    private static class MissingEntriesInfoOfLedger {
        private final long ledgerId;
        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final List<MissingEntriesInfo> missingEntriesInfoList;

        private MissingEntriesInfoOfLedger(long ledgerId, int ensembleSize, int writeQuorumSize, int ackQuorumSize,
                                           List<MissingEntriesInfo> missingEntriesInfoList) {
            this.ledgerId = ledgerId;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.missingEntriesInfoList = missingEntriesInfoList;
        }

        private long getLedgerId() {
            return ledgerId;
        }

        private int getEnsembleSize() {
            return ensembleSize;
        }

        private int getWriteQuorumSize() {
            return writeQuorumSize;
        }

        private int getAckQuorumSize() {
            return ackQuorumSize;
        }

        private List<MissingEntriesInfo> getMissingEntriesInfoList() {
            return missingEntriesInfoList;
        }
    }

    private class ReadLedgerMetadataCallbackForReplicasCheck
            implements BiConsumer<Versioned<LedgerMetadata>, Throwable> {
        private final long ledgerInRange;
        private final MultiCallback mcbForThisLedgerRange;
        private final ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithMissingEntries;
        private final ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithUnavailableBookies;

        ReadLedgerMetadataCallbackForReplicasCheck(
                long ledgerInRange,
                MultiCallback mcbForThisLedgerRange,
                ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithMissingEntries,
                ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithUnavailableBookies) {
            this.ledgerInRange = ledgerInRange;
            this.mcbForThisLedgerRange = mcbForThisLedgerRange;
            this.ledgersWithMissingEntries = ledgersWithMissingEntries;
            this.ledgersWithUnavailableBookies = ledgersWithUnavailableBookies;
        }

        @Override
        public void accept(Versioned<LedgerMetadata> metadataVer, Throwable exception) {
            if (exception != null) {
                if (BKException
                        .getExceptionCode(exception) == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ignoring replicas check of already deleted ledger {}",
                                ledgerInRange);
                    }
                    mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
                    return;
                } else {
                    LOG.warn("Unable to read the ledger: {} information", ledgerInRange, exception);
                    mcbForThisLedgerRange.processResult(BKException.getExceptionCode(exception), null, null);
                    return;
                }
            }

            LedgerMetadata metadata = metadataVer.getValue();
            if (!metadata.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ledger: {} is not yet closed, "
                                    + "so skipping the replicas check analysis for now",
                            ledgerInRange);
                }
                mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
                return;
            }

            final long lastEntryId = metadata.getLastEntryId();
            if (lastEntryId == -1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ledger: {} is closed but it doesn't has any entries, "
                            + "so skipping the replicas check", ledgerInRange);
                }
                mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
                return;
            }

            int writeQuorumSize = metadata.getWriteQuorumSize();
            int ackQuorumSize = metadata.getAckQuorumSize();
            int ensembleSize = metadata.getEnsembleSize();
            RoundRobinDistributionSchedule distributionSchedule = new RoundRobinDistributionSchedule(writeQuorumSize,
                    ackQuorumSize, ensembleSize);
            List<Entry<Long, ? extends List<BookieId>>> segments = new LinkedList<>(
                    metadata.getAllEnsembles().entrySet());
            /*
             * since there are multiple segments, MultiCallback should be
             * created for (ensembleSize * segments.size()) calls.
             */
            MultiCallback mcbForThisLedger = new MultiCallback(ensembleSize * segments.size(),
                    mcbForThisLedgerRange, null, BKException.Code.OK, BKException.Code.ReadException);
            HashMap<BookieId, List<BookieExpectedToContainSegmentInfo>> bookiesSegmentInfoMap =
                    new HashMap<BookieId, List<BookieExpectedToContainSegmentInfo>>();
            for (int segmentNum = 0; segmentNum < segments.size(); segmentNum++) {
                final Entry<Long, ? extends List<BookieId>> segmentEnsemble = segments.get(segmentNum);
                final List<BookieId> ensembleOfSegment = segmentEnsemble.getValue();
                final long startEntryIdOfSegment = segmentEnsemble.getKey();
                final boolean lastSegment = (segmentNum == (segments.size() - 1));
                final long lastEntryIdOfSegment = lastSegment ? lastEntryId
                        : segments.get(segmentNum + 1).getKey() - 1;
                /*
                 * Segment can be empty. If last segment is empty, then
                 * startEntryIdOfSegment of it will be greater than lastEntryId
                 * of the ledger. If the segment in middle is empty, then its
                 * startEntry will be same as startEntry of the following
                 * segment.
                 */
                final boolean emptySegment = lastSegment ? (startEntryIdOfSegment > lastEntryId)
                        : (startEntryIdOfSegment == segments.get(segmentNum + 1).getKey());
                for (int bookieIndex = 0; bookieIndex < ensembleOfSegment.size(); bookieIndex++) {
                    final BookieId bookieInEnsemble = ensembleOfSegment.get(bookieIndex);
                    final BitSet entriesStripedToThisBookie = emptySegment ? EMPTY_BITSET
                            : distributionSchedule.getEntriesStripedToTheBookie(bookieIndex, startEntryIdOfSegment,
                            lastEntryIdOfSegment);
                    if (entriesStripedToThisBookie.cardinality() == 0) {
                        /*
                         * if no entry is expected to contain in this bookie,
                         * then there is no point in making
                         * getListOfEntriesOfLedger call for this bookie. So
                         * instead callback with success result.
                         */
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "For ledger: {}, in Segment: {}, no entry is expected to contain in"
                                            + " this bookie: {}. So skipping getListOfEntriesOfLedger call",
                                    ledgerInRange, segmentEnsemble, bookieInEnsemble);
                        }
                        mcbForThisLedger.processResult(BKException.Code.OK, null, null);
                        continue;
                    }
                    List<BookieExpectedToContainSegmentInfo> bookieSegmentInfoList = bookiesSegmentInfoMap
                            .get(bookieInEnsemble);
                    if (bookieSegmentInfoList == null) {
                        bookieSegmentInfoList = new ArrayList<BookieExpectedToContainSegmentInfo>();
                        bookiesSegmentInfoMap.put(bookieInEnsemble, bookieSegmentInfoList);
                    }
                    bookieSegmentInfoList.add(new BookieExpectedToContainSegmentInfo(startEntryIdOfSegment,
                            lastEntryIdOfSegment, segmentEnsemble, entriesStripedToThisBookie));
                }
            }
            for (Entry<BookieId, List<BookieExpectedToContainSegmentInfo>> bookiesSegmentInfoTuple :
                    bookiesSegmentInfoMap.entrySet()) {
                final BookieId bookieInEnsemble = bookiesSegmentInfoTuple.getKey();
                final List<BookieExpectedToContainSegmentInfo> bookieSegmentInfoList = bookiesSegmentInfoTuple
                        .getValue();
                admin.asyncGetListOfEntriesOfLedger(bookieInEnsemble, ledgerInRange)
                        .whenComplete(new GetListOfEntriesOfLedgerCallbackForReplicasCheck(ledgerInRange, ensembleSize,
                                writeQuorumSize, ackQuorumSize, bookieInEnsemble, bookieSegmentInfoList,
                                ledgersWithMissingEntries, ledgersWithUnavailableBookies, mcbForThisLedger));
            }
        }
    }

    private static class BookieExpectedToContainSegmentInfo {
        private final long startEntryIdOfSegment;
        private final long lastEntryIdOfSegment;
        private final Entry<Long, ? extends List<BookieId>> segmentEnsemble;
        private final BitSet entriesOfSegmentStripedToThisBookie;

        private BookieExpectedToContainSegmentInfo(long startEntryIdOfSegment, long lastEntryIdOfSegment,
                                                   Entry<Long, ? extends List<BookieId>> segmentEnsemble,
                                                   BitSet entriesOfSegmentStripedToThisBookie) {
            this.startEntryIdOfSegment = startEntryIdOfSegment;
            this.lastEntryIdOfSegment = lastEntryIdOfSegment;
            this.segmentEnsemble = segmentEnsemble;
            this.entriesOfSegmentStripedToThisBookie = entriesOfSegmentStripedToThisBookie;
        }

        public long getStartEntryIdOfSegment() {
            return startEntryIdOfSegment;
        }

        public long getLastEntryIdOfSegment() {
            return lastEntryIdOfSegment;
        }

        public Entry<Long, ? extends List<BookieId>> getSegmentEnsemble() {
            return segmentEnsemble;
        }

        public BitSet getEntriesOfSegmentStripedToThisBookie() {
            return entriesOfSegmentStripedToThisBookie;
        }
    }

    private static class GetListOfEntriesOfLedgerCallbackForReplicasCheck
            implements BiConsumer<AvailabilityOfEntriesOfLedger, Throwable> {
        private final long ledgerInRange;
        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final BookieId bookieInEnsemble;
        private final List<BookieExpectedToContainSegmentInfo> bookieExpectedToContainSegmentInfoList;
        private final ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithMissingEntries;
        private final ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithUnavailableBookies;
        private final MultiCallback mcbForThisLedger;

        private GetListOfEntriesOfLedgerCallbackForReplicasCheck(
                long ledgerInRange,
                int ensembleSize,
                int writeQuorumSize,
                int ackQuorumSize,
                BookieId bookieInEnsemble,
                List<BookieExpectedToContainSegmentInfo> bookieExpectedToContainSegmentInfoList,
                ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithMissingEntries,
                ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithUnavailableBookies,
                MultiCallback mcbForThisLedger) {
            this.ledgerInRange = ledgerInRange;
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.bookieInEnsemble = bookieInEnsemble;
            this.bookieExpectedToContainSegmentInfoList = bookieExpectedToContainSegmentInfoList;
            this.ledgersWithMissingEntries = ledgersWithMissingEntries;
            this.ledgersWithUnavailableBookies = ledgersWithUnavailableBookies;
            this.mcbForThisLedger = mcbForThisLedger;
        }

        @Override
        public void accept(AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger,
                           Throwable listOfEntriesException) {

            if (listOfEntriesException != null) {
                if (BKException
                        .getExceptionCode(listOfEntriesException) == BKException.Code.NoSuchLedgerExistsException) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Got NoSuchLedgerExistsException for ledger: {} from bookie: {}",
                                ledgerInRange, bookieInEnsemble);
                    }
                    /*
                     * in the case of NoSuchLedgerExistsException, it should be
                     * considered as empty AvailabilityOfEntriesOfLedger.
                     */
                    availabilityOfEntriesOfLedger = AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER;
                } else {
                    LOG.warn("Unable to GetListOfEntriesOfLedger for ledger: {} from: {}", ledgerInRange,
                            bookieInEnsemble, listOfEntriesException);
                    MissingEntriesInfoOfLedger unavailableBookiesInfoOfThisLedger = ledgersWithUnavailableBookies
                            .get(ledgerInRange);
                    if (unavailableBookiesInfoOfThisLedger == null) {
                        ledgersWithUnavailableBookies.putIfAbsent(ledgerInRange,
                                new MissingEntriesInfoOfLedger(ledgerInRange, ensembleSize, writeQuorumSize,
                                        ackQuorumSize,
                                        Collections.synchronizedList(new ArrayList<MissingEntriesInfo>())));
                        unavailableBookiesInfoOfThisLedger = ledgersWithUnavailableBookies.get(ledgerInRange);
                    }
                    List<MissingEntriesInfo> missingEntriesInfoList =
                            unavailableBookiesInfoOfThisLedger.getMissingEntriesInfoList();
                    for (BookieExpectedToContainSegmentInfo bookieExpectedToContainSegmentInfo
                            : bookieExpectedToContainSegmentInfoList) {
                        missingEntriesInfoList.add(
                                new MissingEntriesInfo(ledgerInRange,
                                bookieExpectedToContainSegmentInfo.getSegmentEnsemble(),
                                        bookieInEnsemble, null));
                        /*
                         * though GetListOfEntriesOfLedger has failed with
                         * exception, mcbForThisLedger should be called back
                         * with OK response, because we dont consider this as
                         * fatal error in replicasCheck and dont want
                         * replicasCheck to exit just because of this issue. So
                         * instead maintain the state of
                         * ledgersWithUnavailableBookies, so that replicascheck
                         * will report these ledgers/bookies appropriately.
                         */
                        mcbForThisLedger.processResult(BKException.Code.OK, null, null);
                    }
                    return;
                }
            }

            for (BookieExpectedToContainSegmentInfo bookieExpectedToContainSegmentInfo
                    : bookieExpectedToContainSegmentInfoList) {
                final long startEntryIdOfSegment = bookieExpectedToContainSegmentInfo.getStartEntryIdOfSegment();
                final long lastEntryIdOfSegment = bookieExpectedToContainSegmentInfo.getLastEntryIdOfSegment();
                final BitSet entriesStripedToThisBookie = bookieExpectedToContainSegmentInfo
                        .getEntriesOfSegmentStripedToThisBookie();
                final Entry<Long, ? extends List<BookieId>> segmentEnsemble =
                        bookieExpectedToContainSegmentInfo.getSegmentEnsemble();
                final List<Long> unavailableEntriesList = availabilityOfEntriesOfLedger
                        .getUnavailableEntries(startEntryIdOfSegment,
                                lastEntryIdOfSegment, entriesStripedToThisBookie);
                if ((unavailableEntriesList != null) && (!unavailableEntriesList.isEmpty())) {
                    MissingEntriesInfoOfLedger missingEntriesInfoOfThisLedger = ledgersWithMissingEntries
                            .get(ledgerInRange);
                    if (missingEntriesInfoOfThisLedger == null) {
                        ledgersWithMissingEntries.putIfAbsent(ledgerInRange,
                                new MissingEntriesInfoOfLedger(ledgerInRange, ensembleSize, writeQuorumSize,
                                        ackQuorumSize,
                                        Collections.synchronizedList(new ArrayList<MissingEntriesInfo>())));
                        missingEntriesInfoOfThisLedger = ledgersWithMissingEntries.get(ledgerInRange);
                    }
                    missingEntriesInfoOfThisLedger.getMissingEntriesInfoList().add(
                            new MissingEntriesInfo(ledgerInRange, segmentEnsemble,
                                    bookieInEnsemble, unavailableEntriesList));
                }
                /*
                 * here though unavailableEntriesList is not empty,
                 * mcbForThisLedger should be called back with OK response,
                 * because we dont consider this as fatal error in replicasCheck
                 * and dont want replicasCheck to exit just because of this
                 * issue. So instead maintain the state of
                 * missingEntriesInfoOfThisLedger, so that replicascheck will
                 * report these ledgers/bookies/missingentries appropriately.
                 */
                mcbForThisLedger.processResult(BKException.Code.OK, null, null);
            }
        }
    }

    private static class ReplicasCheckFinalCallback implements AsyncCallback.VoidCallback {
        final AtomicInteger resultCode;
        final CountDownLatch replicasCheckLatch;

        private ReplicasCheckFinalCallback(AtomicInteger resultCode, CountDownLatch replicasCheckLatch) {
            this.resultCode = resultCode;
            this.replicasCheckLatch = replicasCheckLatch;
        }

        @Override
        public void processResult(int rc, String s, Object obj) {
            resultCode.set(rc);
            replicasCheckLatch.countDown();
        }
    }

    private void reportLedgersWithMissingEntries(
            ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithMissingEntries) {
        StringBuilder errMessage = new StringBuilder();
        HashMultiset<Long> missingEntries = HashMultiset.create();
        int writeQuorumSize;
        int ackQuorumSize;
        for (Map.Entry<Long, MissingEntriesInfoOfLedger> missingEntriesInfoOfLedgerEntry : ledgersWithMissingEntries
                .entrySet()) {
            missingEntries.clear();
            errMessage.setLength(0);
            long ledgerWithMissingEntries = missingEntriesInfoOfLedgerEntry.getKey();
            MissingEntriesInfoOfLedger missingEntriesInfoOfLedger = missingEntriesInfoOfLedgerEntry.getValue();
            List<MissingEntriesInfo> missingEntriesInfoList = missingEntriesInfoOfLedger.getMissingEntriesInfoList();
            writeQuorumSize = missingEntriesInfoOfLedger.getWriteQuorumSize();
            ackQuorumSize = missingEntriesInfoOfLedger.getAckQuorumSize();
            errMessage.append("Ledger : " + ledgerWithMissingEntries + " has following missing entries : ");
            for (int listInd = 0; listInd < missingEntriesInfoList.size(); listInd++) {
                MissingEntriesInfo missingEntriesInfo = missingEntriesInfoList.get(listInd);
                List<Long> unavailableEntriesList = missingEntriesInfo.getUnavailableEntriesList();
                Entry<Long, ? extends List<BookieId>> segmentEnsemble =
                        missingEntriesInfo.getSegmentEnsemble();
                missingEntries.addAll(unavailableEntriesList);
                errMessage.append("In segment starting at " + segmentEnsemble.getKey() + " with ensemble "
                        + segmentEnsemble.getValue() + ", following entries " + unavailableEntriesList
                        + " are missing in bookie: " + missingEntriesInfo.getBookieMissingEntries());
                if (listInd < (missingEntriesInfoList.size() - 1)) {
                    errMessage.append(", ");
                }
            }
            LOG.error(errMessage.toString());
            Set<Multiset.Entry<Long>> missingEntriesSet = missingEntries.entrySet();
            int maxNumOfMissingReplicas = 0;
            long entryWithMaxNumOfMissingReplicas = -1L;
            for (Multiset.Entry<Long> missingEntryWithCount : missingEntriesSet) {
                if (missingEntryWithCount.getCount() > maxNumOfMissingReplicas) {
                    maxNumOfMissingReplicas = missingEntryWithCount.getCount();
                    entryWithMaxNumOfMissingReplicas = missingEntryWithCount.getElement();
                }
            }
            int leastNumOfReplicasOfAnEntry = writeQuorumSize - maxNumOfMissingReplicas;
            if (leastNumOfReplicasOfAnEntry == 0) {
                numLedgersFoundHavingNoReplicaOfAnEntry.incrementAndGet();
                LOG.error("Ledger : {} entryId : {} is missing all replicas", ledgerWithMissingEntries,
                        entryWithMaxNumOfMissingReplicas);
            } else if (leastNumOfReplicasOfAnEntry < ackQuorumSize) {
                numLedgersFoundHavingLessThanAQReplicasOfAnEntry.incrementAndGet();
                LOG.error("Ledger : {} entryId : {} is having: {} replicas, less than ackQuorum num of replicas : {}",
                        ledgerWithMissingEntries, entryWithMaxNumOfMissingReplicas, leastNumOfReplicasOfAnEntry,
                        ackQuorumSize);
            } else if (leastNumOfReplicasOfAnEntry < writeQuorumSize) {
                numLedgersFoundHavingLessThanWQReplicasOfAnEntry.incrementAndGet();
                LOG.error("Ledger : {} entryId : {} is having: {} replicas, less than writeQuorum num of replicas : {}",
                        ledgerWithMissingEntries, entryWithMaxNumOfMissingReplicas, leastNumOfReplicasOfAnEntry,
                        writeQuorumSize);
            }
        }
    }

    private void reportLedgersWithUnavailableBookies(
            ConcurrentHashMap<Long, MissingEntriesInfoOfLedger> ledgersWithUnavailableBookies) {
        StringBuilder errMessage = new StringBuilder();
        for (Map.Entry<Long, MissingEntriesInfoOfLedger> ledgerWithUnavailableBookiesInfo :
                ledgersWithUnavailableBookies.entrySet()) {
            errMessage.setLength(0);
            long ledgerWithUnavailableBookies = ledgerWithUnavailableBookiesInfo.getKey();
            List<MissingEntriesInfo> missingBookiesInfoList = ledgerWithUnavailableBookiesInfo.getValue()
                    .getMissingEntriesInfoList();
            errMessage.append("Ledger : " + ledgerWithUnavailableBookies + " has following unavailable bookies : ");
            for (int listInd = 0; listInd < missingBookiesInfoList.size(); listInd++) {
                MissingEntriesInfo missingBookieInfo = missingBookiesInfoList.get(listInd);
                Entry<Long, ? extends List<BookieId>> segmentEnsemble =
                        missingBookieInfo.getSegmentEnsemble();
                errMessage.append("In segment starting at " + segmentEnsemble.getKey() + " with ensemble "
                        + segmentEnsemble.getValue() + ", following bookie has not responded "
                        + missingBookieInfo.getBookieMissingEntries());
                if (listInd < (missingBookiesInfoList.size() - 1)) {
                    errMessage.append(", ");
                }
            }
            LOG.error(errMessage.toString());
        }
    }

    boolean checkUnderReplicationForReplicasCheck(long ledgerInRange, VoidCallback mcbForThisLedgerRange) {
        try {
            if (ledgerUnderreplicationManager.getLedgerUnreplicationInfo(ledgerInRange) == null) {
                return false;
            }
            /*
             * this ledger is marked underreplicated, so ignore it for
             * replicasCheck.
             */
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ledger: {} is marked underrreplicated, ignore this ledger for replicasCheck",
                        ledgerInRange);
            }
            mcbForThisLedgerRange.processResult(BKException.Code.OK, null, null);
            return true;
        } catch (ReplicationException.NonRecoverableReplicationException nre) {
            LOG.error("Non Recoverable Exception while reading from ZK", nre);
            submitShutdownTask();
            return true;
        } catch (ReplicationException.UnavailableException une) {
            LOG.error("Got exception while trying to check if ledger: {} is underreplicated", ledgerInRange, une);
            mcbForThisLedgerRange.processResult(BKException.getExceptionCode(une), null, null);
            return true;
        }
    }
}
