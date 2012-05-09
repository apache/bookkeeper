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
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

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
import org.apache.hedwig.server.persistence.ScanCallback.ReasonForFinish;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.zookeeper.SafeAsynBKCallback;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback;
import org.apache.hedwig.zookeeper.ZkUtils;

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
    private ZooKeeper zk;
    private ServerConfiguration cfg;
    private TopicManager tm;

    static class InMemoryLedgerRange {
        LedgerRange range;
        long startSeqIdIncluded; // included, for the very first ledger, this
        // value is 1
        LedgerHandle handle;

        public InMemoryLedgerRange(LedgerRange range, long startSeqId, LedgerHandle handle) {
            this.range = range;
            this.startSeqIdIncluded = startSeqId;
            this.handle = handle;
        }

        public InMemoryLedgerRange(LedgerRange range, long startSeqId) {
            this(range, startSeqId, null);
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
        int ledgerRangesZnodeVersion = -1;

        /**
         * This is the handle of the current ledger that is being used to write
         * messages
         */
        InMemoryLedgerRange currentLedgerRange;

        /**
         * Flag to release topic when encountering unrecoverable exceptions
         */
        AtomicBoolean doRelease = new AtomicBoolean(false);

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
     * @param zk
     *            a zookeeper handle to use.
     * @param zkPrefix
     *            the zookeeper subtree that stores the topic to ledger
     *            information. if this prefix does not exist, it will be
     *            created.
     */
    public BookkeeperPersistenceManager(BookKeeper bk, ZooKeeper zk, TopicManager tm, ServerConfiguration cfg,
                                        ScheduledExecutorService executor) {
        this.bk = bk;
        this.zk = zk;
        this.cfg = cfg;
        this.tm = tm;
        queuer = new TopicOpQueuer(executor);
        tm.addTopicOwnershipChangeListener(this);
    }

    class RangeScanOp extends TopicOpQueuer.SynchronousOp {
        RangeScanRequest request;
        int numMessagesRead = 0;
        long totalSizeRead = 0;
        TopicInfo topicInfo;
        long startSeqIdToScan;

        public RangeScanOp(RangeScanRequest request) {
            this(request, -1L);
        }

        public RangeScanOp(RangeScanRequest request, long startSeqId) {
            queuer.super(request.topic);
            this.request = request;
            this.startSeqIdToScan = startSeqId;
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
                             + (startSeqId - imlr.startSeqIdIncluded) + " to entry-id: "
                             + (correctedEndSeqId - imlr.startSeqIdIncluded));
            }

            imlr.handle.asyncReadEntries(startSeqId - imlr.startSeqIdIncluded, correctedEndSeqId
            - imlr.startSeqIdIncluded, new SafeAsynBKCallback.ReadCallback() {

                long expectedEntryId = startSeqId - imlr.startSeqIdIncluded;

                @Override
                public void safeReadComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                    if (rc != BKException.Code.OK || !seq.hasMoreElements()) {
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

                        if (logger.isDebugEnabled()) {
                            logger.debug("Read response from ledger: " + lh.getId() + " entry-id: "
                                         + entry.getEntryId());
                        }

                        assert expectedEntryId == entry.getEntryId() : "expectedEntryId (" + expectedEntryId
                        + ") != entry.getEntryId() (" + entry.getEntryId() + ")";
                        assert (message.getMsgId().getLocalComponent() - imlr.startSeqIdIncluded) == expectedEntryId;

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
                    scanMessages(request, imlr.startSeqIdIncluded + entry.getEntryId() + 1);
                }
            }, request.ctx);
        }

        protected void startReadingFrom(long startSeqId) {

            Map.Entry<Long, InMemoryLedgerRange> entry = topicInfo.ledgerRanges.ceilingEntry(startSeqId);

            if (entry == null) {
                // None of the old ledgers have this seq-id, we must use the
                // current ledger
                long endSeqId = topicInfo.currentLedgerRange.startSeqIdIncluded
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

    protected void scanMessages(RangeScanRequest request, long scanSeqId) {
        queuer.pushAndMaybeRun(request.topic, new RangeScanOp(request, scanSeqId));
    }

    public void deliveredUntil(ByteString topic, Long seqId) {
        // Nothing to do here. this is just a hint that we cannot use.
    }

    class UpdateLedgerOp extends TopicOpQueuer.AsynchronousOp<Void> {
        private long ledgerDeleted;

        public UpdateLedgerOp(ByteString topic, final Callback<Void> cb, final Object ctx, final long ledgerDeleted) {
            queuer.super(topic, cb, ctx);
            this.ledgerDeleted = ledgerDeleted;
        }

        @Override
        public void run() {
            final TopicInfo topicInfo = topicInfos.get(topic);
            if (topicInfo == null) {
                logger.error("Server is not responsible for topic!");
                return;
            }
            boolean needsUpdate = false;
            LedgerRanges.Builder builder = LedgerRanges.newBuilder();
            final Set<Long> keysToRemove = new HashSet<Long>();
            for (Map.Entry<Long, InMemoryLedgerRange> e : topicInfo.ledgerRanges.entrySet()) {
                if (e.getValue().range.getLedgerId() == ledgerDeleted) {
                    needsUpdate = true;
                    keysToRemove.add(e.getKey());
                } else {
                    builder.addRanges(e.getValue().range);
                }
            }
            builder.addRanges(topicInfo.currentLedgerRange.range);

            if (needsUpdate) {
                final LedgerRanges newRanges = builder.build();
                updateLedgerRangesNode(topic, newRanges, topicInfo.ledgerRangesZnodeVersion,
                                       new Callback<Integer>() {
                                           public void operationFinished(Object ctx, Integer newVersion) {
                                               // Finally, all done
                                               for (Long k : keysToRemove) {
                                                   topicInfo.ledgerRanges.remove(k);
                                               }
                                               topicInfo.ledgerRangesZnodeVersion = newVersion;
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

            for (Long endSeqIdIncluded : topicInfo.ledgerRanges.keySet()) {
                if (endSeqIdIncluded <= seqId) {
                    // This ledger's message entries have all been consumed already
                    // so it is safe to delete it from BookKeeper.
                    long ledgerId = topicInfo.ledgerRanges.get(endSeqIdIncluded).range.getLedgerId();
                    try {
                        bk.deleteLedger(ledgerId);
                        Callback<Void> cb = new Callback<Void>() {
                            public void operationFinished(Object ctx, Void result) {
                                // do nothing, op is async to stop other ops
                                // occurring on the topic during the update
                            }
                            public void operationFailed(Object ctx, PubSubException exception) {
                                logger.error("Failed to update ledger znode", exception);
                            }
                        };
                        queuer.pushAndMaybeRun(topic, new UpdateLedgerOp(topic, cb, null, ledgerId));
                    } catch (Exception e) {
                        // For now, just log an exception error message. In the
                        // future, we can have more complicated retry logic to
                        // delete a consumed ledger. The next time the ledger
                        // garbage collection job runs, we'll once again try to
                        // delete this ledger.
                        logger.error("Exception while deleting consumed ledgerId: " + ledgerId, e);
                    }
                } else
                    break;
            }
        }
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

    public class PersistOp extends TopicOpQueuer.SynchronousOp {
        PersistRequest request;

        public PersistOp(PersistRequest request) {
            queuer.super(request.topic);
            this.request = request;
        }

        @Override
        public void runInternal() {
            final TopicInfo topicInfo = topicInfos.get(topic);

            if (topicInfo == null) {
                request.callback.operationFailed(request.ctx,
                                                 new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }

            if (topicInfo.doRelease.get()) {
                request.callback.operationFailed(request.ctx, new PubSubException.ServiceDownException(
                    "The ownership of the topic is releasing due to unrecoverable issue."));
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

            topicInfo.lastSeqIdPushed = builder.build();
            Message msgToSerialize = Message.newBuilder(request.message).setMsgId(topicInfo.lastSeqIdPushed).build();

            topicInfo.currentLedgerRange.handle.asyncAddEntry(msgToSerialize.toByteArray(),
            new SafeAsynBKCallback.AddCallback() {
                @Override
                public void safeAddComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (rc != BKException.Code.OK) {
                        BKException bke = BKException.create(rc);
                        logger.error("Error while persisting entry to ledger: " + lh.getId() + " for topic: "
                                     + topic.toStringUtf8(), bke);

                        // To preserve ordering guarantees, we
                        // should give up the topic and not let
                        // other operations through
                        if (topicInfo.doRelease.compareAndSet(false, true)) {
                            tm.releaseTopic(request.topic, new Callback<Void>() {
                                @Override
                                public void operationFailed(Object ctx, PubSubException exception) {
                                    logger.error("Exception found on releasing topic " + request.topic.toStringUtf8()
                                               + " when encountering exception from bookkeeper:", exception);
                                }
                                @Override
                                public void operationFinished(Object ctx, Void resultOfOperation) {
                                    logger.debug("successfully releasing topic {} when encountering"
                                                 + " exception from bookkeeper", request.topic.toStringUtf8());
                                }
                            }, null);
                        }
                        request.callback.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }

                    if (entryId + topicInfo.currentLedgerRange.startSeqIdIncluded != localSeqId) {
                        String msg = "Expected BK to assign entry-id: "
                                     + (localSeqId - topicInfo.currentLedgerRange.startSeqIdIncluded)
                                     + " but it instead assigned entry-id: " + entryId + " topic: "
                                     + topic.toStringUtf8() + "ledger: " + lh.getId();
                        logger.error(msg);
                        throw new UnexpectedError(msg);
                    }

                    topicInfo.lastEntryIdAckedInCurrentLedger = entryId;
                    request.callback.operationFinished(ctx, localSeqId);
                }
            }, request.ctx);

        }
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

    String ledgersPath(ByteString topic) {
        return cfg.getZkTopicPath(new StringBuilder(), topic).append("/ledgers").toString();
    }

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
            // read topic ledgers node data
            final String zNodePath = ledgersPath(topic);

            zk.getData(zNodePath, false, new SafeAsyncZKCallback.DataCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (rc == Code.OK.intValue()) {
                        processTopicLedgersNodeData(data, stat.getVersion());
                        return;
                    }

                    if (rc == Code.NONODE.intValue()) {
                        // create it
                        final byte[] initialData = LedgerRanges.getDefaultInstance().toByteArray();
                        ZkUtils.createFullPathOptimistic(zk, zNodePath, initialData, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT, new SafeAsyncZKCallback.StringCallback() {
                            @Override
                            public void safeProcessResult(int rc, String path, Object ctx, String name) {
                                if (rc != Code.OK.intValue()) {
                                    KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                                                             "Could not create ledgers node for topic: " + topic.toStringUtf8(),
                                                             path, rc);
                                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                                    return;
                                }
                                // initial version is version 1
                                // (guessing)
                                processTopicLedgersNodeData(initialData, 0);
                            }
                        }, ctx);
                        return;
                    }

                    // otherwise some other error
                    KeeperException ke = ZkUtils.logErrorAndCreateZKException("Could not read ledgers node for topic: "
                                         + topic.toStringUtf8(), path, rc);
                    cb.operationFailed(ctx, new PubSubException.ServiceDownException(ke));

                }
            }, ctx);
        }

        void processTopicLedgersNodeData(byte[] data, int version) {

            final LedgerRanges ranges;
            try {
                ranges = LedgerRanges.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                String msg = "Ledger ranges for topic:" + topic.toStringUtf8() + " could not be deserialized";
                logger.error(msg, e);
                cb.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                return;
            }

            Iterator<LedgerRange> lrIterator = ranges.getRangesList().iterator();
            TopicInfo topicInfo = new TopicInfo();

            long startOfLedger = 1;

            while (lrIterator.hasNext()) {
                LedgerRange range = lrIterator.next();

                if (range.hasEndSeqIdIncluded()) {
                    // this means it was a valid and completely closed ledger
                    long endOfLedger = range.getEndSeqIdIncluded().getLocalComponent();
                    topicInfo.ledgerRanges.put(endOfLedger, new InMemoryLedgerRange(range, startOfLedger));
                    startOfLedger = endOfLedger + 1;
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

                // The last ledger does not have a valid seq-id, lets try to
                // find it out
                recoverLastTopicLedgerAndOpenNewOne(range.getLedgerId(), version, topicInfo);
                return;
            }

            // All ledgers were found properly closed, just start a new one
            openNewTopicLedger(version, topicInfo);
        }

        /**
         * Recovers the last ledger, opens a new one, and persists the new
         * information to ZK
         *
         * @param ledgerId
         *            Ledger to be recovered
         */
        private void recoverLastTopicLedgerAndOpenNewOne(final long ledgerId, final int expectedVersionOfLedgerNode,
                final TopicInfo topicInfo) {

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
                        openNewTopicLedger(expectedVersionOfLedgerNode, topicInfo);
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

                            long prevLedgerEnd = topicInfo.ledgerRanges.isEmpty() ? 0 : topicInfo.ledgerRanges
                                                 .lastKey();
                            LedgerRange lr = LedgerRange.newBuilder().setLedgerId(ledgerId)
                                             .setEndSeqIdIncluded(lastMessage.getMsgId()).build();
                            topicInfo.ledgerRanges.put(lr.getEndSeqIdIncluded().getLocalComponent(),
                                    new InMemoryLedgerRange(lr, prevLedgerEnd + 1, lh));

                            logger.info("Recovered unclosed ledger: " + ledgerId + " for topic: "
                                        + topic.toStringUtf8() + " with " + numEntriesInLastLedger + " entries");

                            openNewTopicLedger(expectedVersionOfLedgerNode, topicInfo);
                        }
                    }, ctx);

                }

            }, ctx);
        }

        /**
         *
         * @param requiredVersionOfLedgersNode
         *            The version of the ledgers node when we read it, should be
         *            the same when we try to write
         */
        private void openNewTopicLedger(final int expectedVersionOfLedgersNode, final TopicInfo topicInfo) {
            bk.asyncCreateLedger(cfg.getBkEnsembleSize(), cfg.getBkQuorumSize(), DigestType.CRC32, passwd,
            new SafeAsynBKCallback.CreateCallback() {
                boolean processed = false;

                @Override
                public void safeCreateComplete(int rc, LedgerHandle lh, Object ctx) {
                    if (processed) {
                        return;
                    } else {
                        processed = true;
                    }

                    if (rc != BKException.Code.OK) {
                        BKException bke = BKException.create(rc);
                        logger.error("Could not create new ledger while acquiring topic: "
                                     + topic.toStringUtf8(), bke);
                        cb.operationFailed(ctx, new PubSubException.ServiceDownException(bke));
                        return;
                    }

                    topicInfo.lastSeqIdPushed = topicInfo.ledgerRanges.isEmpty() ? MessageSeqId.newBuilder()
                                                .setLocalComponent(0).build() : topicInfo.ledgerRanges.lastEntry().getValue().range
                                                .getEndSeqIdIncluded();

                    LedgerRange lastRange = LedgerRange.newBuilder().setLedgerId(lh.getId()).build();
                    topicInfo.currentLedgerRange = new InMemoryLedgerRange(lastRange, topicInfo.lastSeqIdPushed
                            .getLocalComponent() + 1, lh);

                    // Persist the fact that we started this new
                    // ledger to ZK

                    LedgerRanges.Builder builder = LedgerRanges.newBuilder();
                    for (InMemoryLedgerRange imlr : topicInfo.ledgerRanges.values()) {
                        builder.addRanges(imlr.range);
                    }
                    builder.addRanges(lastRange);

                    updateLedgerRangesNode(topic, builder.build(), expectedVersionOfLedgersNode,
                                           new Callback<Integer>() {
                                               public void operationFinished(Object ctx, Integer newVersion) {
                                                   // Finally, all done
                                                   topicInfo.ledgerRangesZnodeVersion = newVersion;
                                                   topicInfos.put(topic, topicInfo);
                                                   cb.operationFinished(ctx, null);
                                               }
                                               public void operationFailed(Object ctx, PubSubException exception) {
                                                   cb.operationFailed(ctx, exception);
                                               }
                                           }, ctx);
                    return;
                }
            }, ctx);
        }
    }

    public void updateLedgerRangesNode(final ByteString topic, LedgerRanges ranges,
                                       int version, final Callback<Integer> callback, Object ctx) {
        final String zNodePath = ledgersPath(topic);

        zk.setData(zNodePath, ranges.toByteArray(), version, new SafeAsyncZKCallback.StatCallback() {
                @Override
                public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc != KeeperException.Code.OK.intValue()) {
                        KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                                "Could not write ledgers node for topic: " + topic.toStringUtf8(), path, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                        return;
                    }
                    callback.operationFinished(ctx, stat.getVersion());
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
}
