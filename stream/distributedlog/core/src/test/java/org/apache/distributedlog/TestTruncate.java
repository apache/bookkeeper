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
package org.apache.distributedlog;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.LogSegmentMetadata.TruncationStatus;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Cases for truncation.
 */
public class TestTruncate extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestTruncate.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration()
                    .setLockTimeout(10)
                    .setOutputBufferSize(0)
                    .setPeriodicFlushFrequencyMilliSeconds(10)
                    .setSchedulerShutdownTimeoutMs(0)
                    .setDLLedgerMetadataLayoutVersion(
                            LogSegmentMetadata.LogSegmentMetadataVersion.VERSION_V2_LEDGER_SEQNO.value);

    static void updateCompletionTime(ZooKeeperClient zkc,
                                     LogSegmentMetadata l, long completionTime) throws Exception {
        LogSegmentMetadata newSegment = l.mutator().setCompletionTime(completionTime).build();
        DLMTestUtil.updateSegmentMetadata(zkc, newSegment);
    }

    static void setTruncationStatus(ZooKeeperClient zkc,
                                    LogSegmentMetadata l,
                                    TruncationStatus status) throws Exception {
        LogSegmentMetadata newSegment =
                l.mutator().setTruncationStatus(status).build();
        DLMTestUtil.updateSegmentMetadata(zkc, newSegment);
    }

    @Test(timeout = 60000)
    public void testPurgeLogs() throws Exception {
        String name = "distrlog-purge-logs";
        URI uri = createDLMURI("/" + name);

        populateData(new HashMap<Long, DLSN>(), conf, name, 10, 10, false);

        DistributedLogManager distributedLogManager = createNewDLM(conf, name);

        List<LogSegmentMetadata> segments = distributedLogManager.getLogSegments();
        LOG.info("Segments before modifying completion time : {}", segments);

        ZooKeeperClient zkc = TestZooKeeperClientBuilder.newBuilder(conf)
                .uri(uri)
                .build();

        // Update completion time of first 5 segments
        long newTimeMs = System.currentTimeMillis() - 60 * 60 * 1000 * 2;
        for (int i = 0; i < 5; i++) {
            LogSegmentMetadata segment = segments.get(i);
            updateCompletionTime(zkc, segment, newTimeMs + i);
        }
        zkc.close();

        segments = distributedLogManager.getLogSegments();
        LOG.info("Segments after modifying completion time : {}", segments);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setRetentionPeriodHours(1);
        confLocal.setExplicitTruncationByApplication(false);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        long txid = 1 + 10 * 10;
        for (int j = 1; j <= 10; j++) {
            Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
        }

        // wait until truncation task to be completed.
        BKAsyncLogWriter bkLogWriter = (BKAsyncLogWriter) writer;
        CompletableFuture<List<LogSegmentMetadata>> truncationAttempt = bkLogWriter.getLastTruncationAttempt();
        while (truncationAttempt == null || !truncationAttempt.isDone()) {
            TimeUnit.MILLISECONDS.sleep(20);
            truncationAttempt = bkLogWriter.getLastTruncationAttempt();
        }

        assertEquals(6, distributedLogManager.getLogSegments().size());

        Utils.close(writer);
        dlm.close();

        distributedLogManager.close();
    }

    @Test(timeout = 60000)
    public void testTruncation() throws Exception {
        String name = "distrlog-truncation";

        long txid = 1;
        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        Pair<DistributedLogManager, AsyncLogWriter> pair =
                populateData(txid2DLSN, conf, name, 4, 10, true);

        Thread.sleep(1000);

        // delete invalid dlsn
        assertFalse(Utils.ioResult(pair.getRight().truncate(DLSN.InvalidDLSN)));
        verifyEntries(name, 1, 1, 5 * 10);

        for (int i = 1; i <= 4; i++) {
            int txn = (i - 1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long) txn);
            assertTrue(Utils.ioResult(pair.getRight().truncate(dlsn)));
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(Utils.ioResult(pair.getRight().truncate(dlsn)));
        verifyEntries(name, 1, 41, 10);

        Utils.close(pair.getRight());
        pair.getLeft().close();
    }

    @Test(timeout = 60000)
    public void testExplicitTruncation() throws Exception {
        String name = "distrlog-truncation-explicit";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setExplicitTruncationByApplication(true);

        Map<Long, DLSN> txid2DLSN = new HashMap<Long, DLSN>();
        Pair<DistributedLogManager, AsyncLogWriter> pair =
                populateData(txid2DLSN, confLocal, name, 4, 10, true);

        Thread.sleep(1000);

        for (int i = 1; i <= 4; i++) {
            int txn = (i - 1) * 10 + i;
            DLSN dlsn = txid2DLSN.get((long) txn);
            assertTrue(Utils.ioResult(pair.getRight().truncate(dlsn)));
            verifyEntries(name, 1, (i - 1) * 10 + 1, (5 - i + 1) * 10);
        }

        // Delete higher dlsn
        int txn = 43;
        DLSN dlsn = txid2DLSN.get((long) txn);
        assertTrue(Utils.ioResult(pair.getRight().truncate(dlsn)));
        verifyEntries(name, 1, 41, 10);

        Utils.close(pair.getRight());
        pair.getLeft().close();

        // Try force truncation
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocal, name);
        BKLogWriteHandler handler = dlm.createWriteHandler(true);
        Utils.ioResult(handler.purgeLogSegmentsOlderThanTxnId(Integer.MAX_VALUE));

        verifyEntries(name, 1, 41, 10);
    }

    @Test(timeout = 60000)
    public void testOnlyPurgeSegmentsBeforeNoneFullyTruncatedSegment() throws Exception {
        String name = "distrlog-only-purge-segments-before-none-fully-truncated-segment";
        URI uri = createDLMURI("/" + name);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setExplicitTruncationByApplication(true);

        // populate data
        populateData(new HashMap<Long, DLSN>(), confLocal, name, 4, 10, false);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        LOG.info("Segments before modifying segment status : {}", segments);

        ZooKeeperClient zkc = TestZooKeeperClientBuilder.newBuilder(conf)
                .uri(uri)
                .build();
        setTruncationStatus(zkc, segments.get(0), TruncationStatus.PARTIALLY_TRUNCATED);
        for (int i = 1; i < 4; i++) {
            LogSegmentMetadata segment = segments.get(i);
            setTruncationStatus(zkc, segment, TruncationStatus.TRUNCATED);
        }
        List<LogSegmentMetadata> segmentsAfterTruncated = dlm.getLogSegments();

        dlm.purgeLogsOlderThan(999999);
        List<LogSegmentMetadata> newSegments = dlm.getLogSegments();
        LOG.info("Segments after purge segments older than 999999 : {}", newSegments);
        assertArrayEquals(segmentsAfterTruncated.toArray(new LogSegmentMetadata[segmentsAfterTruncated.size()]),
                          newSegments.toArray(new LogSegmentMetadata[newSegments.size()]));

        dlm.close();

        // Update completion time of all 4 segments
        long newTimeMs = System.currentTimeMillis() - 60 * 60 * 1000 * 10;
        for (int i = 0; i < 4; i++) {
            LogSegmentMetadata segment = newSegments.get(i);
            updateCompletionTime(zkc, segment, newTimeMs + i);
        }

        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(confLocal);
        newConf.setRetentionPeriodHours(1);

        DistributedLogManager newDLM = createNewDLM(newConf, name);
        AsyncLogWriter newWriter = newDLM.startAsyncLogSegmentNonPartitioned();
        long txid = 1 + 4 * 10;
        for (int j = 1; j <= 10; j++) {
            Utils.ioResult(newWriter.write(DLMTestUtil.getLogRecordInstance(txid++)));
        }

        // to make sure the truncation task is executed
        DLSN lastDLSN = Utils.ioResult(newDLM.getLastDLSNAsync());
        LOG.info("Get last dlsn of stream {} : {}", name, lastDLSN);

        assertEquals(5, newDLM.getLogSegments().size());

        Utils.close(newWriter);
        newDLM.close();

        zkc.close();
    }

    @Test(timeout = 60000)
    public void testPartiallyTruncateTruncatedSegments() throws Exception {
        String name = "distrlog-partially-truncate-truncated-segments";
        URI uri = createDLMURI("/" + name);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setExplicitTruncationByApplication(true);

        // populate
        Map<Long, DLSN> dlsnMap = new HashMap<Long, DLSN>();
        populateData(dlsnMap, confLocal, name, 4, 10, false);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        LOG.info("Segments before modifying segment status : {}", segments);

        ZooKeeperClient zkc = TestZooKeeperClientBuilder.newBuilder(conf)
                .uri(uri)
                .build();
        for (int i = 0; i < 4; i++) {
            LogSegmentMetadata segment = segments.get(i);
            setTruncationStatus(zkc, segment, TruncationStatus.TRUNCATED);
        }

        List<LogSegmentMetadata> newSegments = dlm.getLogSegments();
        LOG.info("Segments after changing truncation status : {}", newSegments);

        dlm.close();

        DistributedLogManager newDLM = createNewDLM(confLocal, name);
        AsyncLogWriter newWriter = newDLM.startAsyncLogSegmentNonPartitioned();
        Utils.ioResult(newWriter.truncate(dlsnMap.get(15L)));

        List<LogSegmentMetadata> newSegments2 = newDLM.getLogSegments();
        assertArrayEquals(newSegments.toArray(new LogSegmentMetadata[4]),
                          newSegments2.toArray(new LogSegmentMetadata[4]));

        Utils.close(newWriter);
        newDLM.close();
        zkc.close();
    }

    private Pair<DistributedLogManager, AsyncLogWriter> populateData(
            Map<Long, DLSN> txid2DLSN, DistributedLogConfiguration confLocal,
            String name, int numLogSegments, int numEntriesPerLogSegment,
            boolean createInprogressLogSegment) throws Exception {
        long txid = 1;
        for (long i = 1; i <= numLogSegments; i++) {
            LOG.info("Writing Log Segment {}.", i);
            DistributedLogManager dlm = createNewDLM(confLocal, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= numEntriesPerLogSegment; j++) {
                long curTxId = txid++;
                DLSN dlsn = Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(curTxId)));
                txid2DLSN.put(curTxId, dlsn);
            }
            Utils.close(writer);
            dlm.close();
        }

        if (createInprogressLogSegment) {
            DistributedLogManager dlm = createNewDLM(confLocal, name);
            AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
            for (int j = 1; j <= 10; j++) {
                long curTxId = txid++;
                DLSN dlsn = Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(curTxId)));
                txid2DLSN.put(curTxId, dlsn);
            }
            return new ImmutablePair<DistributedLogManager, AsyncLogWriter>(dlm, writer);
        } else {
            return null;
        }
    }

    private void verifyEntries(String name, long readFromTxId, long startTxId, int numEntries) throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, name);
        LogReader reader = dlm.getInputStream(readFromTxId);

        long txid = startTxId;
        int numRead = 0;
        LogRecord r = reader.readNext(false);
        while (null != r) {
            DLMTestUtil.verifyLogRecord(r);
            assertEquals(txid++, r.getTransactionId());
            ++numRead;
            r = reader.readNext(false);
        }
        assertEquals(numEntries, numRead);
        reader.close();
        dlm.close();
    }

}
