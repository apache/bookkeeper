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
package org.apache.distributedlog;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.exceptions.AlreadyTruncatedTransactionException;
import org.apache.distributedlog.exceptions.BKTransmitException;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.exceptions.EndOfStreamException;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.LogReadException;
import org.apache.distributedlog.exceptions.LogRecordTooLongException;
import org.apache.distributedlog.exceptions.TransactionIdOutOfOrderException;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.impl.ZKLogSegmentMetadataStore;
import org.apache.distributedlog.io.Abortables;
import org.apache.distributedlog.logsegment.LogSegmentMetadataStore;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import org.apache.distributedlog.metadata.MetadataUpdater;
import org.apache.distributedlog.util.Utils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Cases for {@link DistributedLogManager}.
 */
@Slf4j
public class TestBKDistributedLogManager extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestBKDistributedLogManager.class);

    private static final Random RAND = new Random(System.currentTimeMillis());

    protected static int numBookies = 1;
    static {
        conf.setEnsembleSize(numBookies)
            .setAckQuorumSize(numBookies)
            .setWriteQuorumSize(numBookies);
    }

    @Rule
    public TestName testNames = new TestName();

    private static final long DEFAULT_SEGMENT_SIZE = 1000;

    @BeforeClass
    public static void setupCluster() throws Exception {
        setupCluster(numBookies);
    }

    private void testNonPartitionedWritesInternal(String name, DistributedLogConfiguration conf) throws Exception {
        BKDistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();
            writer.closeAndComplete();
            BKLogWriteHandler blplm = dlm.createWriteHandler(true);
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                    perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            Utils.ioResult(blplm.asyncClose());
        }

        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));
        }
        writer.flush();
        writer.commit();
        writer.close();
        assertEquals(txid - 1, dlm.getLastTxId());

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        long lastTxId = -1;
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            assert (lastTxId < record.getTransactionId());
            lastTxId = record.getTransactionId();
            numTrans++;
            record = reader.readNext(false);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testSimpleWrite() throws Exception {
        BKDistributedLogManager dlm = createNewDLM(conf, "distrlog-simplewrite");
        BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
        out.closeAndComplete();

        BKLogWriteHandler blplm = dlm.createWriteHandler(true);
        assertNotNull(zkc.exists(blplm.completedLedgerZNode(1, 100,
                perStreamLogWriter.getLogSegmentSequenceNumber()), false));
        Utils.ioResult(blplm.asyncClose());
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testNumberOfTransactions() throws Exception {
        String name = "distrlog-txncount";
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 100; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        dlm.close();
        dlm = createNewDLM(conf, name);

        long numTrans = DLMTestUtil.getNumberofLogRecords(dlm, 1);
        assertEquals(100, numTrans);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testContinuousReaders() throws Exception {
        String name = "distrlog-continuous";
        BKDistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = dlm.createWriteHandler(true);

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            Utils.ioResult(blplm.asyncClose());
        }

        BKSyncLogWriter out = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.flush();
        out.commit();
        out.close();

        assertEquals(txid - 1, dlm.getLastTxId());
        dlm.close();

        dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        LogRecord record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        assertEquals(txid - 1, dlm.getLogRecordCount());
        reader.close();
        dlm.close();
    }

    /**
     * Create a bkdlm namespace, write a journal from txid 1, close stream.
     * Try to create a new journal from txid 1. Should throw an exception.
     */
    @Test(timeout = 60000)
    public void testWriteRestartFrom1() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, "distrlog-restartFrom1");
        long txid = 1;
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        txid = 1;
        try {
            out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(txid));
            fail("Shouldn't be able to start another journal from " + txid
                + " when one already exists");
        } catch (Exception ioe) {
            LOG.info("Caught exception as expected", ioe);
        } finally {
            out.close();
        }

        // test border case
        txid = DEFAULT_SEGMENT_SIZE - 1;
        try {
            out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            out.write(DLMTestUtil.getLogRecordInstance(txid));
            fail("Shouldn't be able to start another journal from " + txid
                + " when one already exists");
        } catch (TransactionIdOutOfOrderException rste) {
            LOG.info("Caught exception as expected", rste);
        } finally {
            out.close();
        }

        // open journal continuing from before
        txid = DEFAULT_SEGMENT_SIZE + 1;
        out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        assertNotNull(out);

        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.closeAndComplete();

        // open journal arbitarily far in the future
        txid = DEFAULT_SEGMENT_SIZE * 4;
        out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        out.write(DLMTestUtil.getLogRecordInstance(txid));
        out.close();
        dlm.close();
    }

    @Test(timeout = 90000)
    public void testTwoWritersOnLockDisabled() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setWriteLockEnabled(false);
        String name = "distrlog-two-writers-lock-disabled";
        BKDistributedLogManager manager = createNewDLM(confLocal, name);
        AsyncLogWriter writer1 = Utils.ioResult(manager.openAsyncLogWriter());
        Utils.ioResult(writer1.write(DLMTestUtil.getLogRecordInstance(1L)));
        Assert.assertEquals(1L, writer1.getLastTxId());
        AsyncLogWriter writer2 = Utils.ioResult(manager.openAsyncLogWriter());
        Utils.ioResult(writer2.write(DLMTestUtil.getLogRecordInstance(2L)));
        Assert.assertEquals(2L, writer2.getLastTxId());

        // write a record to writer 1 again
        try {
            Utils.ioResult(writer1.write(DLMTestUtil.getLogRecordInstance(3L)));
            fail("Should fail writing record to writer 1 again as writer 2 took over the ownership");
        } catch (BKTransmitException bkte) {
            assertEquals(BKException.Code.LedgerFencedException, bkte.getBKResultCode());
        }
        Utils.ioResult(writer1.asyncClose());
        Utils.ioResult(writer2.asyncClose());
        manager.close();
    }

    @Test(timeout = 60000)
    public void testSimpleRead() throws Exception {
        String name = "distrlog-simpleread";
        DistributedLogManager dlm = createNewDLM(conf, name);
        final long numTransactions = 10000;
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i <= numTransactions; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();
        dlm.close();

        dlm = createNewDLM(conf, name);

        assertEquals(numTransactions, DLMTestUtil.getNumberofLogRecords(dlm, 1));
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testNumberOfTransactionsWithInprogressAtEnd() throws Exception {
        String name = "distrlog-inprogressAtEnd";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            Utils.ioResult(blplm.asyncClose());
        }
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.flush();
        out.commit();
        out.closeAndComplete();

        assertEquals(txid - 1, dlm.getLastTxId());
        dlm.close();

        dlm = createNewDLM(conf, name);

        long numTrans = DLMTestUtil.getNumberofLogRecords(dlm, 1);
        assertEquals((txid - 1), numTrans);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testContinuousReaderBulk() throws Exception {
        String name = "distrlog-continuous-bulk";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.flush();
            out.commit();
            out.closeAndComplete();
        }

        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.flush();
        out.commit();
        out.closeAndComplete();
        assertEquals(txid - 1, dlm.getLastTxId());
        dlm.close();

        dlm = createNewDLM(conf, name);

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        List<LogRecordWithDLSN> recordList = reader.readBulk(false, 13);
        long lastTxId = -1;
        while (!recordList.isEmpty()) {
            for (LogRecord record : recordList) {
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                DLMTestUtil.verifyLogRecord(record);
                numTrans++;
            }
            recordList = reader.readBulk(false, 13);
        }
        reader.close();
        assertEquals((txid - 1), numTrans);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testContinuousReadersWithEmptyLedgers() throws Exception {
        String name = "distrlog-continuous-emptyledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter writer = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      writer.getLogSegmentSequenceNumber()), false));
            BKLogSegmentWriter perStreamLogWriter = blplm.startLogSegment(txid - 1);
            blplm.completeAndCloseLogSegment(perStreamLogWriter.getLogSegmentSequenceNumber(),
                    perStreamLogWriter.getLogSegmentId(), txid - 1, txid - 1, 0);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(txid - 1, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            Utils.ioResult(blplm.asyncClose());
        }

        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
            out.write(op);
        }
        out.flush();
        out.commit();
        out.closeAndComplete();
        assertEquals(txid - 1, dlm.getLastTxId());
        dlm.close();

        dlm = createNewDLM(conf, name);

        AsyncLogReader asyncreader = dlm.getAsyncLogReader(DLSN.InvalidDLSN);
        long numTrans = 0;
        LogRecordWithDLSN record = Utils.ioResult(asyncreader.readNext());
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            if (numTrans >= (txid - 1)) {
                break;
            }
            record = Utils.ioResult(asyncreader.readNext());
        }
        assertEquals((txid - 1), numTrans);
        Utils.close(asyncreader);

        LogReader reader = dlm.getInputStream(1);
        numTrans = 0;
        record = reader.readNext(false);
        while (null != record) {
            DLMTestUtil.verifyLogRecord(record);
            numTrans++;
            record = reader.readNext(false);
        }
        assertEquals((txid - 1), numTrans);
        reader.close();
        assertEquals(txid - 1, dlm.getLogRecordCount());
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testNonPartitionedWrites() throws Exception {
        String name = "distrlog-non-partitioned-bulk";
        testNonPartitionedWritesInternal(name, conf);
    }

    @Test(timeout = 60000)
    public void testCheckLogExists() throws Exception {
        String name = "distrlog-check-log-exists";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));
        }
        writer.flush();
        writer.commit();
        writer.close();
        assertEquals(txid - 1, dlm.getLastTxId());
        dlm.close();

        URI uri = createDLMURI("/" + name);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        assertTrue(namespace.logExists(name));
        assertFalse(namespace.logExists("non-existent-log"));
        URI nonExistentUri = createDLMURI("/" + "non-existent-ns");
        Namespace nonExistentNS = NamespaceBuilder.newBuilder()
                .conf(conf).uri(nonExistentUri).build();
        assertFalse(nonExistentNS.logExists(name));

        int logCount = 0;
        Iterator<String> logIter = namespace.getLogs();
        while (logIter.hasNext()) {
            String log = logIter.next();
            logCount++;
            assertEquals(name, log);
        }
        assertEquals(1, logCount);

        namespace.close();
    }

    @SuppressWarnings("deprecation")
    @Test(timeout = 60000)
    public void testMetadataAccessor() throws Exception {
        String name = "distrlog-metadata-accessor";
        org.apache.distributedlog.api.MetadataAccessor metadata =
            DLMTestUtil.createNewMetadataAccessor(conf, name, createDLMURI("/" + name));
        assertEquals(name, metadata.getStreamName());
        metadata.createOrUpdateMetadata(name.getBytes());
        assertEquals(name, new String(metadata.getMetadata()));
        metadata.deleteMetadata();
        assertEquals(null, metadata.getMetadata());
    }

    @Test(timeout = 60000)
    public void testSubscriptionsStore() throws Exception {
        String name = "distrlog-subscriptions-store";
        String subscriber0 = "subscriber-0";
        String subscriber1 = "subscriber-1";
        String subscriber2 = "subscriber-2";

        DLSN commitPosition0 = new DLSN(4, 33, 5);
        DLSN commitPosition1 = new DLSN(4, 34, 5);
        DLSN commitPosition2 = new DLSN(5, 34, 5);
        DLSN commitPosition3 = new DLSN(6, 35, 6);

        DistributedLogManager dlm = createNewDLM(conf, name);

        SubscriptionsStore store = dlm.getSubscriptionsStore();

        // no data
        assertEquals(Utils.ioResult(store.getLastCommitPosition(subscriber0)), DLSN.NonInclusiveLowerBound);
        assertEquals(Utils.ioResult(store.getLastCommitPosition(subscriber1)), DLSN.NonInclusiveLowerBound);
        assertEquals(Utils.ioResult(store.getLastCommitPosition(subscriber2)), DLSN.NonInclusiveLowerBound);
        // empty
        assertTrue(Utils.ioResult(store.getLastCommitPositions()).isEmpty());

        // subscriber 0 advance
        Utils.ioResult(store.advanceCommitPosition(subscriber0, commitPosition0));
        assertEquals(commitPosition0, Utils.ioResult(store.getLastCommitPosition(subscriber0)));
        Map<String, DLSN> committedPositions = Utils.ioResult(store.getLastCommitPositions());
        assertEquals(1, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));

        // subscriber 1 advance
        Utils.ioResult(store.advanceCommitPosition(subscriber1, commitPosition1));
        assertEquals(commitPosition1, Utils.ioResult(store.getLastCommitPosition(subscriber1)));
        committedPositions = Utils.ioResult(store.getLastCommitPositions());
        assertEquals(2, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));
        assertEquals(commitPosition1, committedPositions.get(subscriber1));

        // subscriber 2 advance
        Utils.ioResult(store.advanceCommitPosition(subscriber2, commitPosition2));
        assertEquals(commitPosition2, Utils.ioResult(store.getLastCommitPosition(subscriber2)));
        committedPositions = Utils.ioResult(store.getLastCommitPositions());
        assertEquals(3, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));
        assertEquals(commitPosition1, committedPositions.get(subscriber1));
        assertEquals(commitPosition2, committedPositions.get(subscriber2));

        // subscriber 2 advance again
        DistributedLogManager newDLM = createNewDLM(conf, name);
        SubscriptionsStore newStore = newDLM.getSubscriptionsStore();
        Utils.ioResult(newStore.advanceCommitPosition(subscriber2, commitPosition3));
        newStore.close();
        newDLM.close();

        committedPositions = Utils.ioResult(store.getLastCommitPositions());
        assertEquals(3, committedPositions.size());
        assertEquals(commitPosition0, committedPositions.get(subscriber0));
        assertEquals(commitPosition1, committedPositions.get(subscriber1));
        assertEquals(commitPosition3, committedPositions.get(subscriber2));

        dlm.close();

    }

    private long writeAndMarkEndOfStream(DistributedLogManager dlm, long txid) throws Exception {
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter writer = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();

            if (i < 2) {
                writer.closeAndComplete();
                BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                        perStreamLogWriter.getLogSegmentSequenceNumber()), false));
                Utils.ioResult(blplm.asyncClose());
            } else {
                writer.markEndOfStream();
                writer.closeAndComplete();
                BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
                assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, DistributedLogConstants.MAX_TXID,
                        perStreamLogWriter.getLogSegmentSequenceNumber()), false));
                Utils.ioResult(blplm.asyncClose());
            }
            ((BKDistributedLogManager) dlm).getScheduler().submit(() -> {}).get();
        }
        return txid;
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStream() throws Exception {
        String name = "distrlog-mark-end-of-stream";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        txid = writeAndMarkEndOfStream(dlm, txid);

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        boolean exceptionEncountered = false;
        LogRecord record = null;
        try {
            record = reader.readNext(false);
            long expectedTxId = 1;
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(expectedTxId, record.getTransactionId());
                expectedTxId++;
                numTrans++;
                record = reader.readNext(false);
            }
        } catch (EndOfStreamException exc) {
            LOG.info("Encountered EndOfStream on reading records after {}", record);
            exceptionEncountered = true;
        }
        assertEquals((txid - 1), numTrans);
        assertTrue(exceptionEncountered);
        exceptionEncountered = false;
        try {
            reader.readNext(false);
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assertTrue(exceptionEncountered);
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testWriteFailsAfterMarkEndOfStream() throws Exception {
        String name = "distrlog-mark-end-failure";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        txid = writeAndMarkEndOfStream(dlm, txid);

        assertEquals(txid - 1, dlm.getLastTxId());
        LogRecord last = dlm.getLastLogRecord();
        assertEquals(txid - 1, last.getTransactionId());
        DLMTestUtil.verifyLogRecord(last);
        assertTrue(dlm.isEndOfStreamMarked());

        LogWriter writer = null;
        boolean exceptionEncountered = false;
        try {
            writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE / 2; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        writer.close();
        assertTrue(exceptionEncountered);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStreamOnEmptyStream() throws Exception {
        markEndOfStreamOnEmptyLogSegment(0);
    }

    @Test(timeout = 60000)
    public void testMarkEndOfStreamOnClosedStream() throws Exception {
        markEndOfStreamOnEmptyLogSegment(3);
    }

    private void markEndOfStreamOnEmptyLogSegment(int numCompletedSegments) throws Exception {
        String name = "distrlog-mark-end-empty-" + numCompletedSegments;

        DistributedLogManager dlm = createNewDLM(conf, name);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, numCompletedSegments, DEFAULT_SEGMENT_SIZE);

        BKSyncLogWriter writer = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        writer.markEndOfStream();
        writer.closeAndComplete();

        LogReader reader = dlm.getInputStream(1);
        long numTrans = 0;
        boolean exceptionEncountered = false;
        try {
            LogRecord record = reader.readNext(false);
            long lastTxId = -1;
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                numTrans++;
                record = reader.readNext(false);
            }
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assertEquals(numCompletedSegments * DEFAULT_SEGMENT_SIZE, numTrans);
        assertTrue(exceptionEncountered);
        exceptionEncountered = false;
        try {
            reader.readNext(false);
        } catch (EndOfStreamException exc) {
            exceptionEncountered = true;
        }
        assertTrue(exceptionEncountered);
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000, expected = LogRecordTooLongException.class)
    public void testMaxLogRecSize() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, "distrlog-maxlogRecSize");
        AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
        Utils.ioResult(writer.write(new LogRecord(1L, DLMTestUtil.repeatString(
                                DLMTestUtil.repeatString("abcdefgh", 256), 512).getBytes())));
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testMaxTransmissionSize() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(1024 * 1024);
        BKDistributedLogManager dlm =
                createNewDLM(confLocal, "distrlog-transmissionSize");
        AsyncLogWriter out = Utils.ioResult(dlm.openAsyncLogWriter());
        boolean exceptionEncountered = false;
        byte[] largePayload = new byte[(LogRecord.MAX_LOGRECORDSET_SIZE / 2) + 2];
        RAND.nextBytes(largePayload);
        try {
            LogRecord op = new LogRecord(1L, largePayload);
            CompletableFuture<DLSN> firstWriteFuture = out.write(op);
            op = new LogRecord(2L, largePayload);
            // the second write will flush the first one, since we reached the maximum transmission size.
            out.write(op);
            Utils.ioResult(firstWriteFuture);
        } catch (LogRecordTooLongException exc) {
            exceptionEncountered = true;
        } finally {
            Utils.ioResult(out.asyncClose());
        }
        assertFalse(exceptionEncountered);
        Abortables.abortQuietly(out);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void deleteDuringRead() throws Exception {
        String name = "distrlog-delete-with-reader";
        DistributedLogManager dlm = createNewDLM(conf, name);

        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter writer = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }

            BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();

            writer.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
            assertNotNull(zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                    perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            Utils.ioResult(blplm.asyncClose());
        }

        LogReader reader = dlm.getInputStream(1);
        LogRecord record = reader.readNext(false);
        assert (null != record);
        DLMTestUtil.verifyLogRecord(record);
        long lastTxId = record.getTransactionId();

        dlm.delete();

        boolean exceptionEncountered;
        try {
            record = reader.readNext(false);
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assert (lastTxId < record.getTransactionId());
                lastTxId = record.getTransactionId();
                record = reader.readNext(false);
            }
            // make sure the exception is thrown from readahead
            while (true) {
                reader.readNext(false);
            }
        } catch (LogReadException | LogNotFoundException | DLIllegalStateException e) {
            exceptionEncountered = true;
        }
        assertTrue(exceptionEncountered);
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testImmediateFlush() throws Exception {
        String name = "distrlog-immediate-flush";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setOutputBufferSize(0);
        testNonPartitionedWritesInternal(name, confLocal);
    }

    @Test(timeout = 60000)
    public void testLastLogRecordWithEmptyLedgers() throws Exception {
        String name = "distrlog-lastLogRec-emptyledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            BKLogSegmentWriter perStreamLogWriter = out.getCachedLogWriter();
            out.closeAndComplete();
            BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);

            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(start, txid - 1,
                                                      perStreamLogWriter.getLogSegmentSequenceNumber()), false));
            BKLogSegmentWriter writer = blplm.startLogSegment(txid - 1);
            blplm.completeAndCloseLogSegment(writer.getLogSegmentSequenceNumber(),
                    writer.getLogSegmentId(), txid - 1, txid - 1, 0);
            assertNotNull(
                zkc.exists(blplm.completedLedgerZNode(txid - 1, txid - 1,
                                                      writer.getLogSegmentSequenceNumber()), false));
            Utils.ioResult(blplm.asyncClose());
        }

        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        LogRecord op = DLMTestUtil.getLogRecordInstance(txid);
        op.setControl();
        out.write(op);
        out.flush();
        out.commit();
        out.abort();
        dlm.close();

        dlm = createNewDLM(conf, name);

        assertEquals(txid - 1, dlm.getLastTxId());
        LogRecord last = dlm.getLastLogRecord();
        assertEquals(txid - 1, last.getTransactionId());
        DLMTestUtil.verifyLogRecord(last);
        assertEquals(txid - 1, dlm.getLogRecordCount());

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testLogSegmentListener() throws Exception {
        String name = "distrlog-logsegment-listener";
        int numSegments = 3;
        final CountDownLatch[] latches = new CountDownLatch[numSegments + 1];
        for (int i = 0; i < numSegments + 1; i++) {
            latches[i] = new CountDownLatch(1);
        }

        final AtomicInteger numFailures = new AtomicInteger(0);
        final AtomicReference<Collection<LogSegmentMetadata>> receivedStreams =
                new AtomicReference<Collection<LogSegmentMetadata>>();

        BKDistributedLogManager dlm = createNewDLM(conf, name);

        Utils.ioResult(dlm.getWriterMetadataStore().getLog(dlm.getUri(), name, true, true));
        dlm.registerListener(new LogSegmentListener() {
            @Override
            public void onSegmentsUpdated(List<LogSegmentMetadata> segments) {
                int updates = segments.size();
                boolean hasIncompletedLogSegments = false;
                for (LogSegmentMetadata l : segments) {
                    if (l.isInProgress()) {
                        hasIncompletedLogSegments = true;
                        break;
                    }
                }
                if (hasIncompletedLogSegments) {
                    return;
                }
                if (updates >= 1) {
                    if (segments.get(segments.size() - 1).getLogSegmentSequenceNumber() != updates) {
                        numFailures.incrementAndGet();
                    }
                }
                receivedStreams.set(segments);
                latches[updates].countDown();
            }

            @Override
            public void onLogStreamDeleted() {
                // no-op
            }
        });
        long txid = 1;
        for (int i = 0; i < numSegments; i++) {
            LOG.info("Waiting for creating log segment {}.", i);
            latches[i].await();
            LOG.info("Creating log segment {}.", i);
            BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
            LOG.info("Created log segment {}.", i);
            for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
                LogRecord op = DLMTestUtil.getLogRecordInstance(txid++);
                out.write(op);
            }
            out.closeAndComplete();
            LOG.info("Completed log segment {}.", i);
        }
        latches[numSegments].await();
        assertEquals(0, numFailures.get());
        assertNotNull(receivedStreams.get());
        assertEquals(numSegments, receivedStreams.get().size());
        int seqno = 1;
        for (LogSegmentMetadata m : receivedStreams.get()) {
            assertEquals(seqno, m.getLogSegmentSequenceNumber());
            assertEquals((seqno - 1) * DEFAULT_SEGMENT_SIZE + 1, m.getFirstTxId());
            assertEquals(seqno * DEFAULT_SEGMENT_SIZE, m.getLastTxId());
            ++seqno;
        }

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLastDLSN() throws Exception {
        String name = "distrlog-get-last-dlsn";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setFirstNumEntriesPerReadLastRecordScan(2);
        confLocal.setMaxNumEntriesPerReadLastRecordScan(4);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        long txid = 1;
        LOG.info("Writing 10 control records");
        for (int i = 0; i < 10; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            record.setControl();
            Utils.ioResult(writer.writeControlRecord(record));
        }
        LOG.info("10 control records are written");

        try {
            dlm.getLastDLSN();
            fail("Should fail on getting last dlsn from an empty log.");
        } catch (LogEmptyException lee) {
            // expected
        }

        writer.closeAndComplete();
        LOG.info("Completed first log segment");

        writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
        LOG.info("Completed second log segment");

        LOG.info("Writing another 10 control records");
        for (int i = 1; i < 10; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(txid++);
            record.setControl();
            Utils.ioResult(writer.write(record));
        }

        assertEquals(new DLSN(2, 0, 0), dlm.getLastDLSN());

        writer.closeAndComplete();
        LOG.info("Completed third log segment");

        assertEquals(new DLSN(2, 0, 0), dlm.getLastDLSN());

        writer.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountAsync() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, testNames.getMethodName());
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 2, 10);

        CompletableFuture<Long> futureCount = dlm.getLogRecordCountAsync(DLSN.InitialDLSN);
        Long count = Utils.ioResult(futureCount, 2, TimeUnit.SECONDS);
        assertEquals(20, count.longValue());

        writer.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testInvalidStreamFromInvalidZkPath() throws Exception {
        String baseName = testNames.getMethodName();
        String streamName = "\0blah";
        URI uri = createDLMURI("/" + baseName);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();

        DistributedLogManager dlm = null;
        AsyncLogWriter writer = null;
        try {
            dlm = namespace.openLog(streamName);
            writer = dlm.startAsyncLogSegmentNonPartitioned();
            fail("should have thrown");
        } catch (InvalidStreamNameException e) {
        } finally {
            if (null != writer) {
                Utils.close(writer);
            }
            if (null != dlm) {
                dlm.close();
            }
            namespace.close();
        }
    }

    @Test(timeout = 60000)
    public void testTruncationValidation() throws Exception {
        String name = "distrlog-truncation-validation";
        URI uri = createDLMURI("/" + name);
        ZooKeeperClient zookeeperClient = TestZooKeeperClientBuilder.newBuilder()
            .uri(uri)
            .build();
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-truncation-validation")
                .numThreads(1)
                .build();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setDLLedgerMetadataLayoutVersion(LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentCacheEnabled(false);

        LogSegmentMetadataStore metadataStore = new ZKLogSegmentMetadataStore(confLocal, zookeeperClient, scheduler);

        BKDistributedLogManager dlm = createNewDLM(confLocal, name);
        DLSN truncDLSN = DLSN.InitialDLSN;
        DLSN beyondTruncDLSN = DLSN.InitialDLSN;
        long beyondTruncTxId = 1;
        long txid = 1;
        for (long i = 0; i < 3; i++) {
            long start = txid;
            BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid++);
                CompletableFuture<DLSN> dlsn = writer.write(record);

                if (i == 1 && j == 2) {
                    truncDLSN = Utils.ioResult(dlsn);
                } else if (i == 2 && j == 3) {
                    beyondTruncDLSN = Utils.ioResult(dlsn);
                    beyondTruncTxId = record.getTransactionId();
                } else if (j == 10) {
                    Utils.ioResult(dlsn);
                }
            }

            writer.close();
        }

        {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue((record != null) && (record.getDlsn().compareTo(DLSN.InitialDLSN) == 0));
            reader.close();
        }

        Map<Long, LogSegmentMetadata> segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                LogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments before truncating first segment : {}", segmentList);

        MetadataUpdater updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(
                confLocal, metadataStore);
        Utils.ioResult(updater.setLogSegmentTruncated(segmentList.get(1L)));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                LogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments after truncated first segment : {}", segmentList);

        {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue("Unexpected record : " + record,
                    (record != null) && (record.getDlsn().compareTo(new DLSN(2, 0, 0)) == 0));
            reader.close();
        }

        {
            LogReader reader = dlm.getInputStream(1);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue((record != null) && (record.getDlsn().compareTo(new DLSN(2, 0, 0)) == 0));
            reader.close();
        }

        updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal, metadataStore);
        Utils.ioResult(updater.setLogSegmentActive(segmentList.get(1L)));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                LogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments after marked first segment as active : {}", segmentList);

        updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal, metadataStore);
        Utils.ioResult(updater.setLogSegmentTruncated(segmentList.get(2L)));

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                LogMetadata.getLogSegmentsPath(uri, name, confLocal.getUnpartitionedStreamName()));

        LOG.info("Read segments after truncated second segment : {}", segmentList);

        {
            AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
            long expectedTxId = 1L;
            boolean exceptionEncountered = false;
            try {
                for (int i = 0; i < 3 * 10; i++) {
                    LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
                    DLMTestUtil.verifyLargeLogRecord(record);
                    assertEquals(expectedTxId, record.getTransactionId());
                    expectedTxId++;
                }
            } catch (AlreadyTruncatedTransactionException exc) {
                exceptionEncountered = true;
            }
            assertTrue(exceptionEncountered);
            Utils.close(reader);
        }

        updater = LogSegmentMetadataStoreUpdater.createMetadataUpdater(conf, metadataStore);
        Utils.ioResult(updater.setLogSegmentActive(segmentList.get(2L)));

        BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        assertTrue(Utils.ioResult(writer.truncate(truncDLSN)));
        BKLogWriteHandler handler = writer.getCachedWriteHandler();
        List<LogSegmentMetadata> cachedSegments = handler.getCachedLogSegments(LogSegmentMetadata.COMPARATOR);
        for (LogSegmentMetadata segment: cachedSegments) {
            if (segment.getLastDLSN().compareTo(truncDLSN) < 0) {
                assertTrue(segment.isTruncated());
                assertTrue(!segment.isPartiallyTruncated());
            } else if (segment.getFirstDLSN().compareTo(truncDLSN) < 0) {
                assertTrue(!segment.isTruncated());
                assertTrue(segment.isPartiallyTruncated());
            } else {
                assertTrue(!segment.isTruncated());
                assertTrue(!segment.isPartiallyTruncated());
            }
        }

        segmentList = DLMTestUtil.readLogSegments(zookeeperClient,
                LogMetadata.getLogSegmentsPath(uri, name, conf.getUnpartitionedStreamName()));

        assertTrue(segmentList.get(truncDLSN.getLogSegmentSequenceNo())
                .getMinActiveDLSN().compareTo(truncDLSN) == 0);

        {
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(truncDLSN, record.getDlsn());
            reader.close();
        }

        {
            LogReader reader = dlm.getInputStream(1);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(truncDLSN, record.getDlsn());
            reader.close();
        }

        {
            AsyncLogReader reader = dlm.getAsyncLogReader(DLSN.InitialDLSN);
            LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
            assertTrue(record != null);
            assertEquals(truncDLSN, record.getDlsn());
            Utils.close(reader);
        }


        {
            LogReader reader = dlm.getInputStream(beyondTruncDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(beyondTruncDLSN, record.getDlsn());
            reader.close();
        }

        {
            LogReader reader = dlm.getInputStream(beyondTruncTxId);
            LogRecordWithDLSN record = reader.readNext(false);
            assertTrue(record != null);
            assertEquals(beyondTruncDLSN, record.getDlsn());
            assertEquals(beyondTruncTxId, record.getTransactionId());
            reader.close();
        }

        {
            AsyncLogReader reader = dlm.getAsyncLogReader(beyondTruncDLSN);
            LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
            assertTrue(record != null);
            assertEquals(beyondTruncDLSN, record.getDlsn());
            Utils.close(reader);
        }

        dlm.close();
        zookeeperClient.close();
    }

    @Test(timeout = 60000)
    public void testDeleteLog() throws Exception {
        String name = "delete-log-should-delete-ledgers";
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        // Create the log and write some records
        BKSyncLogWriter writer = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long j = 1; j <= DEFAULT_SEGMENT_SIZE; j++) {
            writer.write(DLMTestUtil.getLogRecordInstance(txid++));
        }
        BKLogSegmentWriter perStreamLogWriter = writer.getCachedLogWriter();
        writer.closeAndComplete();
        BKLogWriteHandler blplm = ((BKDistributedLogManager) (dlm)).createWriteHandler(true);
        assertNotNull(zkc.exists(blplm.completedLedgerZNode(txid, txid - 1,
            perStreamLogWriter.getLogSegmentSequenceNumber()), false));
        Utils.ioResult(blplm.asyncClose());

        // Should be able to open the underline ledger using BK client
        long ledgerId = perStreamLogWriter.getLogSegmentId();
        BKNamespaceDriver driver = (BKNamespaceDriver) dlm.getNamespaceDriver();
        driver.getReaderBKC().get().openLedgerNoRecovery(ledgerId,
            BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
        // Delete the log and we shouldn't be able the open the ledger
        dlm.delete();
        try {
            driver.getReaderBKC().get().openLedgerNoRecovery(ledgerId,
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));
            fail("Should fail to open ledger after we delete the log");
        } catch (BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
            // ignore
        }
        // delete again should not throw any exception
        try {
            dlm.delete();
        } catch (IOException ioe) {
            fail("Delete log twice should not throw any exception");
        }
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testSyncLogWithLedgerMetadata() throws Exception {

        String application = "myapplication";
        String component = "mycomponent";
        String custom = "mycustommetadata";
        LedgerMetadata ledgerMetadata = new LedgerMetadata();
        ledgerMetadata.setApplication(application);
        ledgerMetadata.setComponent(component);
        ledgerMetadata.addCustomMetadata("custom", custom);

        BKDistributedLogManager dlm = createNewDLM(conf, "distrlog-writemetadata");

        BKSyncLogWriter sync = dlm.openLogWriter(ledgerMetadata);
        sync.write(DLMTestUtil.getLogRecordInstance(1));

        LedgerHandle lh = getLedgerHandle(sync.getCachedLogWriter());
        Map<String, byte[]> customMeta = lh.getCustomMetadata();
        assertEquals(application, new String(customMeta.get("application"), UTF_8));
        assertEquals(component, new String(customMeta.get("component"), UTF_8));
        assertEquals(custom, new String(customMeta.get("custom"), UTF_8));

        sync.closeAndComplete();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testAsyncLogWithLedgerMetadata() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setWriteLockEnabled(false);

        BKDistributedLogManager dlm = createNewDLM(confLocal, "distrlog-writemetadata-async");

        String application = "myapplication";
        String custom = "mycustommetadata";
        LedgerMetadata ledgerMetadata = new LedgerMetadata();
        ledgerMetadata.setApplication(application);
        ledgerMetadata.addCustomMetadata("custom", custom);

        AsyncLogWriter async = Utils.ioResult(dlm.openAsyncLogWriter(ledgerMetadata));
        Utils.ioResult(async.write(DLMTestUtil.getLogRecordInstance(2)));

        LedgerHandle lh = getLedgerHandle(((BKAsyncLogWriter) async).getCachedLogWriter());
        Map<String, byte[]> customMeta = lh.getCustomMetadata();
        assertEquals(application, new String(customMeta.get("application"), UTF_8));
        assertNull(customMeta.get("component"));
        assertEquals(custom, new String(customMeta.get("custom"), UTF_8));

        Utils.ioResult(async.asyncClose());
        dlm.close();
    }
}
