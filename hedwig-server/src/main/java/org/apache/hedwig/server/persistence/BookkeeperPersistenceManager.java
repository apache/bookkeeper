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
package org.apache.hedwig.server.persistence;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.server.common.UnexpectedError;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.TopicPersistenceManager;
import org.apache.hedwig.server.persistence.ScanCallback.ReasonForFinish;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.zookeeper.SafeAsynBKCallback;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * This persistence manager uses zookeeper and bookkeeper to store messages.
 *
 * Information about topics are stored in zookeeper with a znode named after the
 * topic that contains an ASCII encoded list with records of the following form:
 *
 * <pre>
 * startSeqId(included)\tledgerId\n
 * </pre>
 *
 */

public class BookkeeperPersistenceManager implements PersistenceManagerWithRangeScan, TopicOwnershipChangeListener {
    static Logger logger = LoggerFactory.getLogger(BookkeeperPersistenceManager.class);
    static byte[] passwd = "sillysecret".getBytes();
    private BookKeeper bk;
    private TopicPersistenceManager tpManager;
    private ServerConfiguration cfg;
    private TopicManager tm;

    private static final long START_SEQ_ID = 1L;
    // max number of entries allowed in a ledger
    private static final long UNLIMITED_ENTRIES = 0L;
    private final long maxEntriesPerLedger;

    static class InMemoryLedgerRange {
        LedgerRange range;
        LedgerHandle handle;

        public InMemoryLedgerRange(LedgerRange range, LedgerHandle handle) {
            this.range = range;
            this.handle = handle;
        }

        public InMemoryLedgerRange(LedgerRange range) {
            this(range, null);
        }

        public long getStartSeqIdIncluded() {
            assert range.hasStartSeqIdIncluded();
            return range.getStartSeqIdIncluded();
        }
    }

    static class TopicInfo {
        /**
         * stores the last message-seq-id vector that has been pushed to BK for
         * persistence (but not necessarily acked yet by BK)
         *
         */
        MessageSeqId lastSeqIdPushed;

        /**
         * stores the last message-id that has been acked by BK. This number is
         * basically used for limiting scans to not read past what has been
         * persisted by BK
         */
        long lastEntryIdAckedInCurrentLedger = -1; // because BK ledgers starts
        // at 0

        /**
         * stores a sorted structure of the ledgers for a topic, mapping from
         * the endSeqIdIncluded to the ledger info. This structure does not
         * include the current ledger
         */
        TreeMap<Long, InMemoryLedgerRange> ledgerRanges = new TreeMap<Long, InMemoryLedgerRange>();
        Version ledgerRangesVersion = Version.NEW;

        /**
         * This is the handle of the current ledger that is being used to write
         * messages
         */
        InMemoryLedgerRange currentLedgerRange;

        /**
         * Flag to release topic when encountering unrecoverable exceptions
         */
        AtomicBoolean doRelease = new AtomicBoolean(false);

        /**
         * Flag indicats the topic is changing ledger
         */
        AtomicBoolean doChangeLedger = new AtomicBoolean(false);
        /**
         * Last seq id to change ledger.
         */
        long lastSeqIdBeforeLedgerChange = -1;
        /**
         * List to buffer all persist requests during changing ledger.
         */
        LinkedList<PersistRequest> deferredRequests = null;

        final static int UNLIMITED = 0;
        int messageBound = UNLIMITED;
    }

    Map<ByteString, TopicInfo> topicInfos = new ConcurrentHashMap<ByteString, TopicInfo>();

    TopicOpQueuer queuer;

    /**
     * Instantiates a BookKeeperPersistence manager.
     *
     * @param bk
     *            a reference to bookkeeper to use.
     * @param metaManagerFactory
     *            a metadata manager factory handle to use.
     * @param tm
     *            a reference to topic manager.
     * @param cfg
     *            Server configuration object
     * @param executor
     *            A executor
     */
    public BookkeeperPersistenceManager(BookKeeper bk, MetadataManagerFactory metaManagerFactory,
                                        TopicManager tm, ServerConfiguration cfg,
                                        ScheduledExecutorService executor) {
        this.bk = bk;
        this.tpManager = metaManagerFactory.newTopicPersistenceManager();
        this.cfg = cfg;
        this.tm = tm;
        this.maxEntriesPerLedger = cfg.getMaxEntriesPerLedger();
        queuer = new TopicOpQueuer(executor);
        tm.addTopicOwnershipChangeListener(this);
    }

    private static LedgerRange buildLedgerRange(long ledgerId, long startOfLedger,
                                                MessageSeqId endOfLedger) {
        LedgerRange.Builder builder =
            LedgerRange.newBuilder().setLedgerId(ledgerId).setStartSeqIdIncluded(startOfLedger)
                       .setEndSeqIdIncluded(endOfLedger);
        return builder.build();
    }

    class RangeScanOp extends TopicOpQueuer.SynchronousOp {
        RangeScanRequest request;
        int numMessagesRead = 0;
        long totalSizeRead = 0;
        TopicInfo topicInfo;
        long startSeqIdToScan;

        public RangeScanOp(RangeScanRequest request) {
            this(request, -1L, 0, 0L);
        }

        public RangeScanOp(RangeScanRequest request, long startSeqId, int numMessagesRead, long totalSizeRead) {
            queuer.super(request.topic);
            this.request = request;
            this.startSeqIdToScan = startSeqId;
            this.numMessagesRead = numMessagesRead;
            this.totalSizeRead = totalSizeRead;
        }

        @Override
        protected void runInternal() {
            topicInfo = topicInfos.get(topic);

            if (topicInfo == null) {
                request.callback.scanFailed(request.ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }

            // if startSeqIdToScan is less than zero, which means it is an unfinished scan request
            // we continue the scan from the provided position
            startReadingFrom(startSeqIdToScan < 0 ? request.startSeqId : startSeqIdToScan);
        }

        protected void read(final InMemoryLedgerRange imlr, final long startSeqId, final long endSeqId) {
            // Verify whether startSeqId falls in ledger range.
            // Only the left endpoint of range needs to be checked.
            if (imlr.getStartSeqIdIncluded() > startSeqId) {
                logger.error(
                        "Invalid RangeScan read, startSeqId {} doesn't fall in ledger range [{} ~ {}]",
                        va(startSeqId, imlr.getStartSeqIdIncluded(), imlr.range.hasEndSeqIdIncluded() ? imlr.range
                                .getEndSeqIdIncluded().getLocalComponent() : ""));
                request.callback.scanFailed(request.ctx, new PubSubException.UnexpectedConditionException("Scan request is out of range"));

                // try release topic to reset the state
                lostTopic(topic);
                return;
            }

            if (imlr.handle == null) {

                bk.asyncOpenLedger(imlr.range.getLedgerId(), DigestType.CRC32, passwd,
                new SafeAsynBKCallback.OpenCallback() {
                    @Override
                    public void safeOpenComplete(int rc, LedgerHandle ledgerHandle, Object ctx) {
                        if (rc == BKException.Code.OK) {
                            imlr.handle = ledgerHandle;
                            read(imlr, startSeqId, endSeqId);
                            return;
                        }
                        BKException bke = BKException.create(rc);
                        logger.error("Could not open ledger: " + imlr.range.getLedgerId() + " for topic: "
                                     + topic);
                        request.callback.scanFailed(ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }
                }, request.ctx);
                return;
            }

            // ledger handle is not null, we can read from it
            long correctedEndSeqId = Math.min(startSeqId + request.messageLimit - numMessagesRead - 1, endSeqId);

            if (logger.isDebugEnabled()) {
                logger.debug("Issuing a bk read for ledger: " + imlr.handle.getId() + " from entry-id: "
                             + (startSeqId - imlr.getStartSeqIdIncluded()) + " to entry-id: "
                             + (correctedEndSeqId - imlr.getStartSeqIdIncluded()));
            }

            imlr.handle.asyncReadEntries(startSeqId - imlr.getStartSeqIdIncluded(), correctedEndSeqId
            - imlr.getStartSeqIdIncluded(), new SafeAsynBKCallback.ReadCallback() {

                long expectedEntryId = startSeqId - imlr.getStartSeqIdIncluded();

                @Override
                public void safeReadComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                    if (rc != BKException.Code.OK || !seq.hasMoreElements()) {
                        if (rc == BKException.Code.OK) {
                            // means that there is no entries read, provide a meaningful exception
                            rc = BKException.Code.NoSuchEntryException;
                        }
                        BKException bke = BKException.create(rc);
                        logger.error("Error while reading from ledger: " + imlr.range.getLedgerId() + " for topic: "
                                     + topic.toStringUtf8(), bke);
                        request.callback.scanFailed(request.ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }

                    LedgerEntry entry = null;
                    while (seq.hasMoreElements()) {
                        entry = seq.nextElement();
                        Message message;
                        try {
                            message = Message.parseFrom(entry.getEntryInputStream());
                        } catch (IOException e) {
                            String msg = "Unreadable message found in ledger: " + imlr.range.getLedgerId()
                                         + " for topic: " + topic.toStringUtf8();
                            logger.error(msg, e);
                            request.callback.scanFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                            return;
                        }

                        logger.debug("Read response from ledger: {} entry-id: {}",
                                     lh.getId(), entry.getEntryId());

                        assert expectedEntryId == entry.getEntryId() : "expectedEntryId (" + expectedEntryId
                        + ") != entry.getEntryId() (" + entry.getEntryId() + ")";
                        assert (message.getMsgId().getLocalComponent() - imlr.getStartSeqIdIncluded()) == expectedEntryId;

                        expectedEntryId++;
                        request.callback.messageScanned(ctx, message);
                        numMessagesRead++;
                        totalSizeRead += message.getBody().size();

                        if (numMessagesRead >= request.messageLimit) {
                            request.callback.scanFinished(ctx, ReasonForFinish.NUM_MESSAGES_LIMIT_EXCEEDED);
                            return;
                        }

                        if (totalSizeRead >= request.sizeLimit) {
                            request.callback.scanFinished(ctx, ReasonForFinish.SIZE_LIMIT_EXCEEDED);
                            return;
                        }
                    }

                    // continue scanning messages
                    scanMessages(request, imlr.getStartSeqIdIncluded() + entry.getEntryId() + 1, numMessagesRead, totalSizeRead);
                }
            }, request.ctx);
        }

        protected void startReadingFrom(long startSeqId) {

            Map.Entry<Long, InMemoryLedgerRange> entry = topicInfo.ledgerRanges.ceilingEntry(startSeqId);

            if (entry == null) {
                // None of the old ledgers have this seq-id, we must use the
                // current ledger
                long endSeqId = topicInfo.currentLedgerRange.getStartSeqIdIncluded()
                                + topicInfo.lastEntryIdAckedInCurrentLedger;

                if (endSeqId < startSeqId) {
                    request.callback.scanFinished(request.ctx, ReasonForFinish.NO_MORE_MESSAGES);
                    return;
                }

                read(topicInfo.currentLedgerRange, startSeqId, endSeqId);
            } else {
                read(entry.getValue(), startSeqId, entry.getValue().range.getEndSeqIdIncluded().getLocalComponent());
            }

        }

    }

    @Override
    public void scanMessages(RangeScanRequest request) {
        queuer.pushAndMaybeRun(request.topic, new RangeScanOp(request));
    }

    protected void scanMessages(RangeScanRequest request, long scanSeqId, int numMsgsRead, long totalSizeRead) {
        queuer.pushAndMaybeRun(request.topic, new RangeScanOp(request, scanSeqId, numMsgsRead, totalSizeRead));
    }

    public void deliveredUntil(ByteString topic, Long seqId) {
        // Nothing to do here. this is just a hint that we cannot use.
    }

    class UpdateLedgerOp extends TopicOpQueuer.AsynchronousOp<Void> {
        private Set<Long> ledgersDeleted;

        public UpdateLedgerOp(ByteString topic, final Callback<Void> cb, final Object ctx,
                              Set<Long> ledgersDeleted) {
            queuer.super(topic, cb, ctx);
            this.ledgersDeleted = ledgersDeleted;
        }

        @Override
        public void run() {
            final TopicInfo topicInfo = topicInfos.get(topic);
            if (topicInfo == null) {
                logger.error("Server is not responsible for topic!");
                cb.operationFailed(ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }
            LedgerRanges.Builder builder = LedgerRanges.newBuilder();
            final Set<Long> keysToRemove = new HashSet<Long>();
            boolean foundUnconsumedLedger = false;
            for (Map.Entry<Long, InMemoryLedgerRange> e : topicInfo.ledgerRanges.entrySet()) {
                LedgerRange lr = e.getValue().range;
                long ledgerId = lr.getLedgerId();
                if (!foundUnconsumedLedger && ledgersDeleted.contains(ledgerId)) {
                    keysToRemove.add(e.getKey());
                    if (!lr.hasEndSeqIdIncluded()) {
                        String msg = "Should not remove unclosed ledger " + ledgerId + " for topic " + topic.toStringUtf8();
                        logger.error(msg);
                        cb.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                        return;
                    }
                } else {
                    foundUnconsumedLedger = true;
                    builder.addRanges(lr);
                }
            }
            builder.addRanges(topicInfo.currentLedgerRange.range);

            if (!keysToRemove.isEmpty()) {
                final LedgerRanges newRanges = builder.build();
                tpManager.writeTopicPersistenceInfo(
                topic, newRanges, topicInfo.ledgerRangesVersion, new Callback<Version>() {
                    public void operationFinished(Object ctx, Version newVersion) {
                        // Finally, all done
                        for (Long k : keysToRemove) {
                            topicInfo.ledgerRanges.remove(k);
                        }
                        topicInfo.ledgerRangesVersion = newVersion;
                        cb.operationFinished(ctx, null);
                    }
                    public void operationFailed(Object ctx, PubSubException exception) {
                        cb.operationFailed(ctx, exception);
                    }
                }, ctx);
            } else {
                cb.operationFinished(ctx, null);
            }
        }
    }

    class ConsumeUntilOp extends TopicOpQueuer.SynchronousOp {
        private final long seqId;

        public ConsumeUntilOp(ByteString topic, long seqId) {
            queuer.super(topic);
            this.seqId = seqId;
        }

        @Override
        public void runInternal() {
            TopicInfo topicInfo = topicInfos.get(topic);
            if (topicInfo == null) {
                logger.error("Server is not responsible for topic!");
                return;
            }

            final LinkedList<Long> ledgersToDelete = new LinkedList<Long>();
            for (Long endSeqIdIncluded : topicInfo.ledgerRanges.keySet()) {
                if (endSeqIdIncluded <= seqId) {
                    // This ledger's message entries have all been consumed already
                    // so it is safe to delete it from BookKeeper.
                    long ledgerId = topicInfo.ledgerRanges.get(endSeqIdIncluded).range.getLedgerId();
                    ledgersToDelete.add(ledgerId);
                } else {
                    break;
                }
            }

            // no ledgers need to delete
            if (ledgersToDelete.isEmpty()) {
                return;
            }

            Set<Long> ledgersDeleted = new HashSet<Long>();
            deleteLedgersAndUpdateLedgersRange(topic, ledgersToDelete, ledgersDeleted);
        }
    }

    private void deleteLedgersAndUpdateLedgersRange(final ByteString topic,
                                                    final LinkedList<Long> ledgersToDelete,
                                                    final Set<Long> ledgersDeleted) {
        if (ledgersToDelete.isEmpty()) {
            Callback<Void> cb = new Callback<Void>() {
                public void operationFinished(Object ctx, Void result) {
                    // do nothing, op is async to stop other ops
                    // occurring on the topic during the update
                }
                public void operationFailed(Object ctx, PubSubException exception) {
                    logger.error("Failed to update ledger znode for topic {} deleting ledgers {} : {}",
                                 va(topic.toStringUtf8(), ledgersDeleted, exception.getMessage()));
                }
            };
            queuer.pushAndMaybeRun(topic, new UpdateLedgerOp(topic, cb, null, ledgersDeleted));
            return;
        }

        final Long ledger = ledgersToDelete.poll();
        if (null == ledger) {
            deleteLedgersAndUpdateLedgersRange(topic, ledgersToDelete, ledgersDeleted);
            return;
        }

        bk.asyncDeleteLedger(ledger, new DeleteCallback() {
            @Override
            public void deleteComplete(int rc, Object ctx) {
                if (BKException.Code.NoSuchLedgerExistsException == rc ||
                    BKException.Code.OK == rc) {
                    ledgersDeleted.add(ledger);
                    deleteLedgersAndUpdateLedgersRange(topic, ledgersToDelete, ledgersDeleted);
                    return;
                } else {
                    logger.warn("Exception while deleting consumed ledger {}, stop deleting other ledgers {} "
                                + "and update ledger ranges with deleted ledgers {} : {}",
                                va(ledger, ledgersToDelete, ledgersDeleted, BKException.create(rc)));
                    // We should not continue when failed to delete ledger
                    Callback<Void> cb = new Callback<Void>() {
                        public void operationFinished(Object ctx, Void result) {
                            // do nothing, op is async to stop other ops
                            // occurring on the topic during the update
                        }
                        public void operationFailed(Object ctx, PubSubException exception) {
                            logger.error("Failed to update ledger znode for topic {} deleting ledgers {} : {}",
                                         va(topic, ledgersDeleted, exception.getMessage()));
                        }
                    };
                    queuer.pushAndMaybeRun(topic, new UpdateLedgerOp(topic, cb, null, ledgersDeleted));
                    return;
                }
            }
        }, null);
    }

    public void consumedUntil(ByteString topic, Long seqId) {
        queuer.pushAndMaybeRun(topic, new ConsumeUntilOp(topic, Math.max(seqId, getMinSeqIdForTopic(topic))));
    }

    public void consumeToBound(ByteString topic) {
        TopicInfo topicInfo = topicInfos.get(topic);

        if (topicInfo == null || topicInfo.messageBound == topicInfo.UNLIMITED) {
            return;
        }
        queuer.pushAndMaybeRun(topic, new ConsumeUntilOp(topic, getMinSeqIdForTopic(topic)));
    }

    public long getMinSeqIdForTopic(ByteString topic) {
        TopicInfo topicInfo = topicInfos.get(topic);

        if (topicInfo == null || topicInfo.messageBound == topicInfo.UNLIMITED) {
            return Long.MIN_VALUE;
        } else {
            return (topicInfo.lastSeqIdPushed.getLocalComponent() - topicInfo.messageBound) + 1;
        }
    }

    public MessageSeqId getCurrentSeqIdForTopic(ByteString topic) throws ServerNotResponsibleForTopicException {
        TopicInfo topicInfo = topicInfos.get(topic);

        if (topicInfo == null) {
            throw new PubSubException.ServerNotResponsibleForTopicException("");
        }

        return topicInfo.lastSeqIdPushed;
    }

    public long getSeqIdAfterSkipping(ByteString topic, long seqId, int skipAmount) {
        return Math.max(seqId + skipAmount, getMinSeqIdForTopic(topic));
    }

    /**
     * Release topic on failure
     *
     * @param topic
     *          Topic Name
     * @param e
     *          Failure Exception
     * @param ctx
     *          Callback context
     */
    protected void releaseTopicIfRequested(final ByteString topic, Exception e, Object ctx) {
        TopicInfo topicInfo = topicInfos.get(topic);
        if (topicInfo == null) {
            logger.warn("No topic found when trying to release ownership of topic " + topic.toStringUtf8()
                      + " on failure.");
            return;
        }
        // do release owner ship of topic
        if (topicInfo.doRelease.compareAndSet(false, true)) {
            logger.info("Release topic " + topic.toStringUtf8() + " when bookkeeper persistence mananger encounters failure :",
                        e);
            tm.releaseTopic(topic, new Callback<Void>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    logger.error("Exception found on releasing topic " + topic.toStringUtf8()
                               + " when encountering exception from bookkeeper:", exception);
                }
                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    logger.info("successfully releasing topic {} when encountering"
                              + " exception from bookkeeper", topic.toStringUtf8());
                }
            }, null);
        }
        // if release happens when the topic is changing ledger
        // we need to fail all queued persist requests
        if (topicInfo.doChangeLedger.get()) {
            for (PersistRequest pr : topicInfo.deferredRequests) {
                pr.getCallback().operationFailed(ctx, new PubSubException.ServiceDownException(e));
            }
            topicInfo.deferredRequests.clear();
            topicInfo.lastSeqIdBeforeLedgerChange = -1;
        }
    }

    public class PersistOp extends TopicOpQueuer.SynchronousOp {
        PersistRequest request;

        public PersistOp(PersistRequest request) {
            queuer.super(request.topic);
            this.request = request;
        }

        @Override
        public void runInternal() {
            doPersistMessage(request);
        }
    }

    /**
     * Persist a message by executing a persist request.
     */
    protected void doPersistMessage(final PersistRequest request) {
        final ByteString topic = request.topic;
        final TopicInfo topicInfo = topicInfos.get(topic);

        if (topicInfo == null) {
            request.getCallback().operationFailed(request.ctx,
                                             new PubSubException.ServerNotResponsibleForTopicException(""));
            return;
        }

        if (topicInfo.doRelease.get()) {
            request.getCallback().operationFailed(request.ctx, new PubSubException.ServiceDownException(
                "The ownership of the topic is releasing due to unrecoverable issue."));
            return;
        }

        // if the topic is changing ledger, queue following persist requests until ledger is changed
        if (topicInfo.doChangeLedger.get()) {
            logger.info("Topic {} is changing ledger, so queue persist request for message.",
                        topic.toStringUtf8());
            topicInfo.deferredRequests.add(request);
            return;
        }

        final long localSeqId = topicInfo.lastSeqIdPushed.getLocalComponent() + 1;
        MessageSeqId.Builder builder = MessageSeqId.newBuilder();
        if (request.message.hasMsgId()) {
            MessageIdUtils.takeRegionMaximum(builder, topicInfo.lastSeqIdPushed, request.message.getMsgId());
        } else {
            builder.addAllRemoteComponents(topicInfo.lastSeqIdPushed.getRemoteComponentsList());
        }
        builder.setLocalComponent(localSeqId);

        // check whether reach the threshold of a ledger, if it does,
        // open a ledger to write
        long entriesInThisLedger = localSeqId - topicInfo.currentLedgerRange.getStartSeqIdIncluded() + 1;
        if (UNLIMITED_ENTRIES != maxEntriesPerLedger &&
            entriesInThisLedger >= maxEntriesPerLedger) {
            if (topicInfo.doChangeLedger.compareAndSet(false, true)) {
                // for order guarantees, we should wait until all the adding operations for current ledger
                // are succeed. so we just mark it as lastSeqIdBeforeLedgerChange
                // when the lastSeqIdBeforeLedgerChange acked, we do changing the ledger
                if (null == topicInfo.deferredRequests) {
                    topicInfo.deferredRequests = new LinkedList<PersistRequest>();
                }
                topicInfo.lastSeqIdBeforeLedgerChange = localSeqId;
            }
        }

        topicInfo.lastSeqIdPushed = builder.build();
        Message msgToSerialize = Message.newBuilder(request.message).setMsgId(topicInfo.lastSeqIdPushed).build();

        final MessageSeqId responseSeqId = msgToSerialize.getMsgId();
        topicInfo.currentLedgerRange.handle.asyncAddEntry(msgToSerialize.toByteArray(),
        new SafeAsynBKCallback.AddCallback() {
            AtomicBoolean processed = new AtomicBoolean(false);
            @Override
            public void safeAddComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {

                // avoid double callback by mistake, since we may do change ledger in this callback.
                if (!processed.compareAndSet(false, true)) {
                    return;
                }
                if (rc != BKException.Code.OK) {
                    BKException bke = BKException.create(rc);
                    logger.error("Error while persisting entry to ledger: " + lh.getId() + " for topic: "
                                 + topic.toStringUtf8(), bke);
                    request.getCallback().operationFailed(ctx, new PubSubException.ServiceDownException(bke));

                    // To preserve ordering guarantees, we
                    // should give up the topic and not let
                    // other operations through
                    releaseTopicIfRequested(request.topic, bke, ctx);
                    return;
                }

                if (entryId + topicInfo.currentLedgerRange.getStartSeqIdIncluded() != localSeqId) {
                    String msg = "Expected BK to assign entry-id: "
                                 + (localSeqId - topicInfo.currentLedgerRange.getStartSeqIdIncluded())
                                 + " but it instead assigned entry-id: " + entryId + " topic: "
                                 + topic.toStringUtf8() + "ledger: " + lh.getId();
                    logger.error(msg);
                    throw new UnexpectedError(msg);
                }

                topicInfo.lastEntryIdAckedInCurrentLedger = entryId;
                request.getCallback().operationFinished(ctx, responseSeqId);
                // if this acked entry is the last entry of current ledger
                // we can add a ChangeLedgerOp to execute to change ledger
                if (topicInfo.doChangeLedger.get() &&
                    entryId + topicInfo.currentLedgerRange.getStartSeqIdIncluded() == topicInfo.lastSeqIdBeforeLedgerChange) {
                    // change ledger
                    changeLedger(topic, new Callback<Void>() {
                        @Override
                        public void operationFailed(Object ctx, PubSubException exception) {
                            logger.error("Failed to change ledger for topic " + topic.toStringUtf8(), exception);
                            // change ledger failed, we should give up topic
                            releaseTopicIfRequested(request.topic, exception, ctx);
                        }
                        @Override
                        public void operationFinished(Object ctx, Void resultOfOperation) {
                            topicInfo.doChangeLedger.set(false);
                            topicInfo.lastSeqIdBeforeLedgerChange = -1;
                            // the ledger is changed, persist queued requests
                            // if the number of queued persist requests is more than maxEntriesPerLedger
                            // we just persist maxEntriesPerLedger requests, other requests are still queued
                            // until next ledger changed.
                            int numRequests = 0;
                            while (!topicInfo.deferredRequests.isEmpty() &&
                                   numRequests < maxEntriesPerLedger) {
                                PersistRequest pr = topicInfo.deferredRequests.removeFirst();
                                doPersistMessage(pr);
                                ++numRequests;
                            }
                            logger.debug("Finished persisting {} queued requests, but there are still {} requests in queue.",
                                         numRequests, topicInfo.deferredRequests.size());
                        }
                    }, ctx);
                }
            }
        }, request.ctx);
    }

    public void persistMessage(PersistRequest request) {
        queuer.pushAndMaybeRun(request.topic, new PersistOp(request));
    }

    public void scanSingleMessage(ScanRequest request) {
        throw new RuntimeException("Not implemented");
    }

    static SafeAsynBKCallback.CloseCallback noOpCloseCallback = new SafeAsynBKCallback.CloseCallback() {
        @Override
        public void safeCloseComplete(int rc, LedgerHandle ledgerHandle, Object ctx) {
        };
    };

    class AcquireOp extends TopicOpQueuer.AsynchronousOp<Void> {
        public AcquireOp(ByteString topic, Callback<Void> cb, Object ctx) {
            queuer.super(topic, cb, ctx);
        }

        @Override
        public void run() {
            if (topicInfos.containsKey(topic)) {
                // Already acquired, do nothing
                cb.operationFinished(ctx, null);
                return;
            }

            // read persistence info
            tpManager.readTopicPersistenceInfo(topic, new Callback<Versioned<LedgerRanges>>() {
                @Override
                public void operationFinished(Object ctx, Versioned<LedgerRanges> ranges) {
                    if (null != ranges) {
                        processTopicLedgerRanges(ranges.getValue(), ranges.getVersion());
                    } else {
                        processTopicLedgerRanges(LedgerRanges.getDefaultInstance(), Version.NEW);
                    }
                }
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    cb.operationFailed(ctx, exception);
                }
            }, ctx);
        }

        void processTopicLedgerRanges(final LedgerRanges ranges, final Version version) {
            final List<LedgerRange> rangesList = ranges.getRangesList();
            if (!rangesList.isEmpty()) {
                LedgerRange range = rangesList.get(0);
                if (range.hasStartSeqIdIncluded()) {
                    // we already have start seq id
                    processTopicLedgerRanges(rangesList, version, range.getStartSeqIdIncluded());
                    return;
                }
                getStartSeqIdToProcessTopicLedgerRanges(rangesList, version);
                return;
            }
            // process topic ledger ranges directly
            processTopicLedgerRanges(rangesList, version, START_SEQ_ID);
        }

        /**
         * Process old version ledger ranges to fetch start seq id.
         */
        void getStartSeqIdToProcessTopicLedgerRanges(
            final List<LedgerRange> rangesList, final Version version) {

            final LedgerRange range = rangesList.get(0);

            if (!range.hasEndSeqIdIncluded()) {
                // process topic ledger ranges directly
                processTopicLedgerRanges(rangesList, version, START_SEQ_ID);
                return;
            }

            final long ledgerId = range.getLedgerId();
            // open the first ledger to compute right start seq id
            bk.asyncOpenLedger(ledgerId, DigestType.CRC32, passwd,
            new SafeAsynBKCallback.OpenCallback() {

                @Override
                public void safeOpenComplete(int rc, LedgerHandle ledgerHandle, Object ctx) {

                    if (rc == BKException.Code.NoSuchLedgerExistsException) {
                        // process next ledger 
                        processTopicLedgerRanges(rangesList, version, START_SEQ_ID);
                        return;
                    } else if (rc != BKException.Code.OK) {
                        BKException bke = BKException.create(rc);
                        logger.error("Could not open ledger {} to get start seq id while acquiring topic {} : {}",
                                     va(ledgerId, topic.toStringUtf8(), bke));
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }

                    final long numEntriesInLastLedger = ledgerHandle.getLastAddConfirmed() + 1;

                    // the ledger is closed before, calling close is just a nop operation.
                    try {
                        ledgerHandle.close();
                    } catch (InterruptedException ie) {
                        // the exception would never be thrown for a read only ledger handle.
                    } catch (BKException bke) {
                        // the exception would never be thrown for a read only ledger handle.
                    }

                    if (numEntriesInLastLedger <= 0) {
                        String msg = "No entries found in a have-end-seq-id ledger " + ledgerId
                                     + " when acquiring topic " + topic.toStringUtf8() + ".";
                        logger.error(msg);
                        cb.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                        return;
                    }
                    long endOfLedger = range.getEndSeqIdIncluded().getLocalComponent();
                    long startOfLedger = endOfLedger - numEntriesInLastLedger + 1;

                    processTopicLedgerRanges(rangesList, version, startOfLedger);
                }

            }, ctx);
        }

        void processTopicLedgerRanges(final List<LedgerRange> rangesList, final Version version,
                                      long startOfLedger) {
            logger.info("Process {} ledgers for topic {} starting from seq id {}.",
                        va(rangesList.size(), topic.toStringUtf8(), startOfLedger));

            Iterator<LedgerRange> lrIterator = rangesList.iterator();

            TopicInfo topicInfo = new TopicInfo();
            while (lrIterator.hasNext()) {
                LedgerRange range = lrIterator.next();

                if (range.hasEndSeqIdIncluded()) {
                    // this means it was a valid and completely closed ledger
                    long endOfLedger = range.getEndSeqIdIncluded().getLocalComponent();
                    if (range.hasStartSeqIdIncluded()) {
                        startOfLedger = range.getStartSeqIdIncluded();
                    } else {
                        range = buildLedgerRange(range.getLedgerId(), startOfLedger,
                                                 range.getEndSeqIdIncluded());
                    }
                    topicInfo.ledgerRanges.put(endOfLedger, new InMemoryLedgerRange(range));
                    if (startOfLedger < endOfLedger + 1) {
                        startOfLedger = endOfLedger + 1;
                    }
                    continue;
                }

                // If it doesn't have a valid end, it must be the last ledger
                if (lrIterator.hasNext()) {
                    String msg = "Ledger-id: " + range.getLedgerId() + " for topic: " + topic.toStringUtf8()
                                 + " is not the last one but still does not have an end seq-id";
                    logger.error(msg);
                    cb.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                    return;
                }

                if (range.hasStartSeqIdIncluded()) {
                    startOfLedger = range.getStartSeqIdIncluded();
                }

                // The last ledger does not have a valid seq-id, lets try to
                // find it out
                recoverLastTopicLedgerAndOpenNewOne(range.getLedgerId(), startOfLedger,
                                                    version, topicInfo);
                return;
            }

            // All ledgers were found properly closed, just start a new one
            openNewTopicLedger(topic, version, topicInfo, startOfLedger, false, cb, ctx);
        }

        /**
         * Recovers the last ledger, opens a new one, and persists the new
         * information to ZK
         *
         * @param ledgerId
         *            Ledger to be recovered
         * @param expectedStartSeqId 
         *            Start seq id of the ledger to recover
         * @param expectedVersionOfLedgerNode
         *            Expected version to update ledgers range
         * @param topicInfo
         *            Topic info
         */
        private void recoverLastTopicLedgerAndOpenNewOne(final long ledgerId, final long expectedStartSeqId,
                final Version expectedVersionOfLedgerNode, final TopicInfo topicInfo) {

            bk.asyncOpenLedger(ledgerId, DigestType.CRC32, passwd, new SafeAsynBKCallback.OpenCallback() {
                @Override
                public void safeOpenComplete(int rc, LedgerHandle ledgerHandle, Object ctx) {

                    if (rc != BKException.Code.OK) {
                        BKException bke = BKException.create(rc);
                        logger.error("While acquiring topic: " + topic.toStringUtf8()
                                     + ", could not open unrecovered ledger: " + ledgerId, bke);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }

                    final long numEntriesInLastLedger = ledgerHandle.getLastAddConfirmed() + 1;

                    if (numEntriesInLastLedger <= 0) {
                        // this was an empty ledger that someone created but
                        // couldn't write to, so just ignore it
                        logger.info("Pruning empty ledger: " + ledgerId + " for topic: " + topic.toStringUtf8());
                        closeLedger(ledgerHandle);
                        openNewTopicLedger(topic, expectedVersionOfLedgerNode, topicInfo,
                                           expectedStartSeqId, false, cb, ctx);
                        return;
                    }

                    // we have to read the last entry of the ledger to find
                    // out the last seq-id

                    ledgerHandle.asyncReadEntries(numEntriesInLastLedger - 1, numEntriesInLastLedger - 1,
                    new SafeAsynBKCallback.ReadCallback() {
                        @Override
                        public void safeReadComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
                        Object ctx) {
                            if (rc != BKException.Code.OK || !seq.hasMoreElements()) {
                                if (rc == BKException.Code.OK) {
                                    // means that there is no entries read, provide a meaningful exception
                                    rc = BKException.Code.NoSuchEntryException;
                                }
                                logger.info("Received error code {}", rc);
                                BKException bke = BKException.create(rc);
                                logger.error("While recovering ledger: " + ledgerId + " for topic: "
                                             + topic.toStringUtf8() + ", could not read last entry", bke);
                                cb.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                                return;
                            }

                            Message lastMessage;
                            try {
                                lastMessage = Message.parseFrom(seq.nextElement().getEntry());
                            } catch (InvalidProtocolBufferException e) {
                                String msg = "While recovering ledger: " + ledgerId + " for topic: "
                                             + topic.toStringUtf8() + ", could not deserialize last message";
                                logger.error(msg, e);
                                cb.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                                return;
                            }

                            long endOfLedger  = lastMessage.getMsgId().getLocalComponent();
                            long startOfLedger = endOfLedger - numEntriesInLastLedger + 1;

                            if (startOfLedger != expectedStartSeqId) {
                                // gap would be introduced by old version when gc consumed ledgers
                                String msg = "Expected start seq id of recovered ledger " + ledgerId
                                             + " to be " + expectedStartSeqId + " but it was "
                                             + startOfLedger + ".";
                                logger.warn(msg);
                            }

                            LedgerRange lr = buildLedgerRange(ledgerId, startOfLedger, lastMessage.getMsgId());
                            topicInfo.ledgerRanges.put(endOfLedger,
                                    new InMemoryLedgerRange(lr, lh));

                            logger.info("Recovered unclosed ledger: {} for topic: {} with {} entries starting from seq id {}",
                                        va(ledgerId, topic.toStringUtf8(), numEntriesInLastLedger, startOfLedger));

                            openNewTopicLedger(topic, expectedVersionOfLedgerNode, topicInfo, endOfLedger + 1, false, cb, ctx);
                        }
                    }, ctx);

                }

            }, ctx);
        }
    }

    /**
     * Open New Ledger to write for a topic.
     *
     * @param topic
     *          Topic Name
     * @param expectedVersionOfLedgersNode
     *          Expected Version to Update Ledgers Node.
     * @param topicInfo
     *          Topic Information
     * @param startSeqId
     *          Start of sequence id for new ledger
     * @param changeLedger
     *          Whether is it called when changing ledger
     * @param cb
     *          Callback to trigger after opening new ledger.
     * @param ctx
     *          Callback context.
     */
    void openNewTopicLedger(final ByteString topic,
                            final Version expectedVersionOfLedgersNode, final TopicInfo topicInfo,
                            final long startSeqId, final boolean changeLedger,
                            final Callback<Void> cb, final Object ctx) {
        bk.asyncCreateLedger(cfg.getBkEnsembleSize(), cfg.getBkWriteQuorumSize(),
                             cfg.getBkAckQuorumSize(), DigestType.CRC32, passwd,
        new SafeAsynBKCallback.CreateCallback() {
            AtomicBoolean processed = new AtomicBoolean(false);

            @Override
            public void safeCreateComplete(int rc, LedgerHandle lh, Object ctx) {
                if (!processed.compareAndSet(false, true)) {
                    return;
                }

                if (rc != BKException.Code.OK) {
                    BKException bke = BKException.create(rc);
                    logger.error("Could not create new ledger while acquiring topic: "
                                 + topic.toStringUtf8(), bke);
                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                    return;
                }

                // compute last seq id
                if (!changeLedger) {
                    topicInfo.lastSeqIdPushed = topicInfo.ledgerRanges.isEmpty() ? MessageSeqId.newBuilder()
                                                .setLocalComponent(startSeqId - 1).build() : topicInfo.ledgerRanges.lastEntry().getValue().range
                                                .getEndSeqIdIncluded();
                }

                LedgerRange lastRange = LedgerRange.newBuilder().setLedgerId(lh.getId())
                                        .setStartSeqIdIncluded(startSeqId).build();
                topicInfo.currentLedgerRange = new InMemoryLedgerRange(lastRange, lh);
                topicInfo.lastEntryIdAckedInCurrentLedger = -1;

                // Persist the fact that we started this new
                // ledger to ZK

                LedgerRanges.Builder builder = LedgerRanges.newBuilder();
                for (InMemoryLedgerRange imlr : topicInfo.ledgerRanges.values()) {
                    builder.addRanges(imlr.range);
                }
                builder.addRanges(lastRange);

                tpManager.writeTopicPersistenceInfo(
                topic, builder.build(), expectedVersionOfLedgersNode, new Callback<Version>() {
                    @Override
                    public void operationFinished(Object ctx, Version newVersion) {
                        // Finally, all done
                        topicInfo.ledgerRangesVersion = newVersion;
                        topicInfos.put(topic, topicInfo);
                        cb.operationFinished(ctx, null);
                    }
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        cb.operationFailed(ctx, exception);
                    }
                }, ctx);
                return;
            }
        }, ctx);
    }

    /**
     * acquire ownership of a topic, doing whatever is needed to be able to
     * perform reads and writes on that topic from here on
     *
     * @param topic
     * @param callback
     * @param ctx
     */
    @Override
    public void acquiredTopic(ByteString topic, Callback<Void> callback, Object ctx) {
        queuer.pushAndMaybeRun(topic, new AcquireOp(topic, callback, ctx));
    }

    /**
     * Change ledger to write for a topic.
     */
    class ChangeLedgerOp extends TopicOpQueuer.AsynchronousOp<Void> {

        public ChangeLedgerOp(ByteString topic, Callback<Void> cb, Object ctx) {
            queuer.super(topic, cb, ctx);
        }

        @Override
        public void run() {
            TopicInfo topicInfo = topicInfos.get(topic);
            if (null == topicInfo) {
                logger.error("Weired! hub server doesn't own topic " + topic.toStringUtf8()
                           + " when changing ledger to write.");
                cb.operationFailed(ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }
            closeLastTopicLedgerAndOpenNewOne(topicInfo);
        }

        private void closeLastTopicLedgerAndOpenNewOne(final TopicInfo topicInfo) {
            final long ledgerId = topicInfo.currentLedgerRange.handle.getId();
            topicInfo.currentLedgerRange.handle.asyncClose(new CloseCallback() {
                AtomicBoolean processed = new AtomicBoolean(false);
                @Override
                public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                    if (!processed.compareAndSet(false, true)) {
                        return;
                    }
                    if (BKException.Code.OK != rc) {
                        BKException bke = BKException.create(rc);
                        logger.error("Could not close ledger " + ledgerId
                                   + " while changing ledger of topic " + topic.toStringUtf8(), bke);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }
                    long endSeqId = topicInfo.lastSeqIdPushed.getLocalComponent();
                    // update last range
                    LedgerRange lastRange =
                        buildLedgerRange(ledgerId, topicInfo.currentLedgerRange.getStartSeqIdIncluded(),
                                         topicInfo.lastSeqIdPushed);

                    topicInfo.currentLedgerRange.range = lastRange;
                    // put current ledger to ledger ranges
                    topicInfo.ledgerRanges.put(endSeqId, topicInfo.currentLedgerRange);
                    logger.info("Closed written ledger " + ledgerId + " for topic "
                              + topic.toStringUtf8() + " to change ledger.");
                    openNewTopicLedger(topic, topicInfo.ledgerRangesVersion,
                                       topicInfo, endSeqId + 1, true, cb, ctx);
                }
            }, ctx);
        }

    }

    /**
     * Change ledger to write for a topic.
     *
     * @param topic
     *          Topic Name
     */
    protected void changeLedger(ByteString topic, Callback<Void> cb, Object ctx) {
        queuer.pushAndMaybeRun(topic, new ChangeLedgerOp(topic, cb, ctx));
    }

    public void closeLedger(LedgerHandle lh) {
        // try {
        // lh.asyncClose(noOpCloseCallback, null);
        // } catch (InterruptedException e) {
        // logger.error(e);
        // Thread.currentThread().interrupt();
        // }
    }

    class ReleaseOp extends TopicOpQueuer.SynchronousOp {

        public ReleaseOp(ByteString topic) {
            queuer.super(topic);
        }

        @Override
        public void runInternal() {
            TopicInfo topicInfo = topicInfos.remove(topic);

            if (topicInfo == null) {
                return;
            }

            for (InMemoryLedgerRange imlr : topicInfo.ledgerRanges.values()) {
                if (imlr.handle != null) {
                    closeLedger(imlr.handle);
                }
            }

            if (topicInfo.currentLedgerRange != null && topicInfo.currentLedgerRange.handle != null) {
                closeLedger(topicInfo.currentLedgerRange.handle);
            }
        }
    }

    /**
     * Release any resources for the topic that might be currently held. There
     * wont be any subsequent reads or writes on that topic coming
     *
     * @param topic
     */
    @Override
    public void lostTopic(ByteString topic) {
        queuer.pushAndMaybeRun(topic, new ReleaseOp(topic));
    }

    class SetMessageBoundOp extends TopicOpQueuer.SynchronousOp {
        final int bound;

        public SetMessageBoundOp(ByteString topic, int bound) {
            queuer.super(topic);
            this.bound = bound;
        }

        @Override
        public void runInternal() {
            TopicInfo topicInfo = topicInfos.get(topic);
            if (topicInfo != null) {
                topicInfo.messageBound = bound;
            }
        }
    }

    public void setMessageBound(ByteString topic, Integer bound) {
        queuer.pushAndMaybeRun(topic, new SetMessageBoundOp(topic, bound));
    }

    public void clearMessageBound(ByteString topic) {
        queuer.pushAndMaybeRun(topic, new SetMessageBoundOp(topic, TopicInfo.UNLIMITED));
    }

    @Override
    public void stop() {
        try {
            tpManager.close();
        } catch (IOException ioe) {
            logger.warn("Exception closing topic persistence manager : ", ioe);
        }
    }
}
