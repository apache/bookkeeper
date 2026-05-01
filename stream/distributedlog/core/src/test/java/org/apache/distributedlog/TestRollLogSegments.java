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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.feature.CoreFeatureKeys;
import org.apache.distributedlog.impl.logsegment.BKLogSegmentEntryReader;
import org.apache.distributedlog.util.FailpointUtils;
import org.apache.distributedlog.util.Utils;
import org.junit.Test;

/**
 * Test Cases for RollLogSegments.
 */
@CustomLog
public class TestRollLogSegments extends TestDistributedLogBase {

    private static void ensureOnlyOneInprogressLogSegments(List<LogSegmentMetadata> segments) throws Exception {
        int numInprogress = 0;
        for (LogSegmentMetadata segment : segments) {
            if (segment.isInProgress()) {
                ++numInprogress;
            }
        }
        assertEquals(1, numInprogress);
    }

    @Test(timeout = 60000)
    public void testDisableRollingLogSegments() throws Exception {
        String name = "distrlog-disable-rolling-log-segments";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(40);

        int numEntries = 100;
        BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

        SettableFeature disableLogSegmentRolling =
                (SettableFeature) dlm.getFeatureProvider()
                        .getFeature(CoreFeatureKeys.DISABLE_LOGSEGMENT_ROLLING.name().toLowerCase());
        disableLogSegmentRolling.set(true);

        final CountDownLatch latch = new CountDownLatch(numEntries);

        // send requests in parallel
        for (int i = 1; i <= numEntries; i++) {
            final int entryId = i;
            writer.write(DLMTestUtil.getLogRecordInstance(entryId)).whenComplete(new FutureEventListener<DLSN>() {

                @Override
                public void onSuccess(DLSN value) {
                    log.info().attr("entryId", entryId).attr("dlsn", value).log("Completed entry");
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable cause) {
                    // nope
                }
            });
        }
        latch.await();

        // make sure all ensure blocks were executed
        writer.closeAndComplete();

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(1, segments.size());

        dlm.close();
    }

    @Test(timeout = 600000)
    public void testLastDLSNInRollingLogSegments() throws Exception {
        final Map<Long, DLSN> lastDLSNs = new HashMap<Long, DLSN>();
        String name = "distrlog-lastdlsn-in-rolling-log-segments";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(40);

        int numEntries = 100;

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        final CountDownLatch latch = new CountDownLatch(numEntries);

        // send requests in parallel to have outstanding requests
        for (int i = 1; i <= numEntries; i++) {
            final int entryId = i;
            CompletableFuture<DLSN> writeFuture =
                writer.write(DLMTestUtil.getLogRecordInstance(entryId))
                    .whenComplete(new FutureEventListener<DLSN>() {

                @Override
                public void onSuccess(DLSN value) {
                    log.info().attr("entryId", entryId).attr("dlsn", value).log("Completed entry");
                    synchronized (lastDLSNs) {
                        DLSN lastDLSN = lastDLSNs.get(value.getLogSegmentSequenceNo());
                        if (null == lastDLSN || lastDLSN.compareTo(value) < 0) {
                            lastDLSNs.put(value.getLogSegmentSequenceNo(), value);
                        }
                    }
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable cause) {

                }
            });
            if (i == 1) {
                // wait for first log segment created
                Utils.ioResult(writeFuture);
            }
        }
        latch.await();

        // make sure all ensure blocks were executed.
        writer.closeAndComplete();

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        log.info().attr("size", lastDLSNs.size()).attr("lastDLSNs", lastDLSNs).log("lastDLSNs after writes");
        log.info().attr("size", segments.size()).attr("segments", segments).log("segments after writes");
        assertTrue(segments.size() >= 2);
        assertTrue(lastDLSNs.size() >= 2);
        assertEquals(lastDLSNs.size(), segments.size());
        for (LogSegmentMetadata segment : segments) {
            DLSN dlsnInMetadata = segment.getLastDLSN();
            DLSN dlsnSeen = lastDLSNs.get(segment.getLogSegmentSequenceNumber());
            assertNotNull(dlsnInMetadata);
            assertNotNull(dlsnSeen);
            if (dlsnInMetadata.compareTo(dlsnSeen) != 0) {
                log.error().attr("dlsnInMetadata", dlsnInMetadata).attr("dlsnSeen", dlsnSeen)
                        .log("Last dlsn recorded in log segment differs from the one already seen");
            }
            assertEquals(0, dlsnInMetadata.compareTo(dlsnSeen));
        }

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testUnableToRollLogSegments() throws Exception {
        String name = "distrlog-unable-to-roll-log-segments";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(1);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        long txId = 1L;

        // Create Log Segments
        Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txId)));

        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate,
                FailpointUtils.FailPointActions.FailPointAction_Throw);

        try {
            // If we couldn't open new log segment, we should keep using the old one
            final int numRecords = 10;
            final CountDownLatch latch = new CountDownLatch(numRecords);
            for (int i = 0; i < numRecords; i++) {
                writer.write(DLMTestUtil.getLogRecordInstance(++txId)).whenComplete(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        log.info().attr("entry", value).log("Completed entry");
                        latch.countDown();
                    }
                    @Override
                    public void onFailure(Throwable cause) {
                        log.error().exception(cause).log("Failed to write entries");
                    }
                });
            }

            latch.await();

            writer.close();

            List<LogSegmentMetadata> segments = dlm.getLogSegments();
            log.info().attr("segments", segments).log("LogSegments");

            assertEquals(1, segments.size());

            long expectedTxID = 1L;
            LogReader reader = dlm.getInputStream(DLSN.InitialDLSN);
            LogRecordWithDLSN record = reader.readNext(false);
            while (null != record) {
                DLMTestUtil.verifyLogRecord(record);
                assertEquals(expectedTxID++, record.getTransactionId());
                assertEquals(record.getTransactionId() - 1, record.getSequenceId());

                record = reader.readNext(false);
            }

            assertEquals(12L, expectedTxID);

            reader.close();

            dlm.close();
        } finally {
            FailpointUtils.removeFailpoint(FailpointUtils.FailPointName.FP_StartLogSegmentBeforeLedgerCreate);
        }
    }

    @Test(timeout = 60000)
    public void testRollingLogSegments() throws Exception {
        log.info("start testRollingLogSegments");
        String name = "distrlog-rolling-logsegments-hightraffic";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentRollingIntervalMinutes(0);
        confLocal.setMaxLogSegmentBytes(1);
        confLocal.setLogSegmentRollingConcurrency(Integer.MAX_VALUE);

        int numLogSegments = 10;

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();

        final CountDownLatch latch = new CountDownLatch(numLogSegments);
        long startTime = System.currentTimeMillis();
        // send requests in parallel to have outstanding requests
        for (int i = 1; i <= numLogSegments; i++) {
            final int entryId = i;
            CompletableFuture<DLSN> writeFuture = writer.write(DLMTestUtil.getLogRecordInstance(entryId))
                    .whenComplete(new FutureEventListener<DLSN>() {
                @Override
                public void onSuccess(DLSN value) {
                    log.info().attr("entryId", entryId).attr("dlsn", value).log("Completed entry");
                    latch.countDown();
                }
                @Override
                public void onFailure(Throwable cause) {
                    log.error().exception(cause).log("Failed to write entries");
                }
            });
            if (i == 1) {
                // wait for first log segment created
                Utils.ioResult(writeFuture);
            }
        }
        latch.await();

        log.info().attr("elapsedMs", System.currentTimeMillis() - startTime).log("Took ms to complete all requests");

        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        log.info().attr("segments", segments).log("LogSegments");
        log.info().attr("size", segments.size()).log("LogSegments size");

        assertTrue(segments.size() >= 2);
        ensureOnlyOneInprogressLogSegments(segments);

        int numSegmentsAfterAsyncWrites = segments.size();

        // writer should work after rolling log segments
        // there would be (numLogSegments/2) segments based on current rolling policy
        for (int i = 1; i <= numLogSegments; i++) {
            DLSN newDLSN = Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(numLogSegments + i)));
            log.info().attr("entryId", numLogSegments + i).attr("dlsn", newDLSN).log("Completed entry");
        }

        segments = dlm.getLogSegments();
        log.info().attr("segments", segments).log("LogSegments");
        log.info().attr("size", segments.size()).log("LogSegments size");

        assertEquals(numSegmentsAfterAsyncWrites + numLogSegments / 2, segments.size());
        ensureOnlyOneInprogressLogSegments(segments);

        writer.close();
        dlm.close();
    }

    private void checkAndWaitWriterReaderPosition(BKLogSegmentWriter writer, long expectedWriterPosition,
                                                  BKAsyncLogReader reader, long expectedReaderPosition,
                                                  LedgerHandle inspector, long expectedLac) throws Exception {
        while (getLedgerHandle(writer).getLastAddConfirmed() < expectedWriterPosition) {
            Thread.sleep(1000);
        }
        assertEquals(expectedWriterPosition, getLedgerHandle(writer).getLastAddConfirmed());
        assertEquals(expectedLac, inspector.readLastConfirmed());
        EntryPosition readPosition = reader.getReadAheadReader().getNextEntryPosition();
        log.info().attr("readPosition", readPosition).log("ReadAhead moved read position");
        while (readPosition.getEntryId() < expectedReaderPosition) {
            Thread.sleep(1000);
            readPosition = reader.getReadAheadReader().getNextEntryPosition();
            log.info().attr("readPosition", readPosition).log("ReadAhead moved read position");
        }
        assertEquals(expectedReaderPosition, readPosition.getEntryId());
    }

    @Test(timeout = 60000)
    @SuppressWarnings("deprecation")
    public void testCaughtUpReaderOnLogSegmentRolling() throws Exception {
        String name = "distrlog-caughtup-reader-on-logsegment-rolling";

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(0);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(4 * 1024 * 1024);
        confLocal.setTraceReadAheadMetadataChanges(true);
        confLocal.setEnsembleSize(1);
        confLocal.setWriteQuorumSize(1);
        confLocal.setAckQuorumSize(1);
        confLocal.setReadLACLongPollTimeout(99999999);
        confLocal.setReaderIdleWarnThresholdMillis(2 * 99999999 + 1);
        confLocal.setBKClientReadTimeout(99999999 + 1);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKSyncLogWriter writer = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();

        // 1) writer added 5 entries.
        final int numEntries = 5;
        for (int i = 1; i <= numEntries; i++) {
            writer.write(DLMTestUtil.getLogRecordInstance(i));
            writer.flush();
            writer.commit();
        }

        BKDistributedLogManager readDLM = (BKDistributedLogManager) createNewDLM(confLocal, name);
        final BKAsyncLogReader reader = (BKAsyncLogReader) readDLM.getAsyncLogReader(DLSN.InitialDLSN);

        // 2) reader should be able to read 5 entries.
        for (long i = 1; i <= numEntries; i++) {
            LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(i, record.getTransactionId());
            assertEquals(record.getTransactionId() - 1, record.getSequenceId());
        }

        BKLogSegmentWriter perStreamWriter = writer.segmentWriter;
        BookKeeperClient bkc = DLMTestUtil.getBookKeeperClient(readDLM);
        LedgerHandle readLh = bkc.get().openLedgerNoRecovery(getLedgerHandle(perStreamWriter).getId(),
                BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes(UTF_8));

        // Writer moved to lac = 9, while reader knows lac = 8 and moving to wait on 9
        checkAndWaitWriterReaderPosition(perStreamWriter, 9, reader, 9, readLh, 8);

        // write 6th record
        writer.write(DLMTestUtil.getLogRecordInstance(numEntries + 1));
        writer.flush();
        // Writer moved to lac = 10, while reader knows lac = 9 and moving to wait on 10
        checkAndWaitWriterReaderPosition(perStreamWriter, 10, reader, 10, readLh, 9);

        // write records without commit to simulate similar failure cases
        writer.write(DLMTestUtil.getLogRecordInstance(numEntries + 2));
        writer.flush();
        // Writer moved to lac = 11, while reader knows lac = 10 and moving to wait on 11
        checkAndWaitWriterReaderPosition(perStreamWriter, 11, reader, 11, readLh, 10);

        while (true) {
            BKLogSegmentEntryReader entryReader =
                    (BKLogSegmentEntryReader) reader.getReadAheadReader().getCurrentSegmentReader().getEntryReader();
            if (null != entryReader && null != entryReader.getOutstandingLongPoll()) {
                break;
            }
            Thread.sleep(1000);
        }
        log.info("Waiting for long poll getting interrupted with metadata changed");

        // simulate a recovery without closing ledger causing recording wrong last dlsn
        BKLogWriteHandler writeHandler = writer.getCachedWriteHandler();
        writeHandler.completeAndCloseLogSegment(
                writeHandler.inprogressZNodeName(perStreamWriter.getLogSegmentId(),
                        perStreamWriter.getStartTxId(), perStreamWriter.getLogSegmentSequenceNumber()),
                perStreamWriter.getLogSegmentSequenceNumber(),
                perStreamWriter.getLogSegmentId(),
                perStreamWriter.getStartTxId(), perStreamWriter.getLastTxId(),
                perStreamWriter.getPositionWithinLogSegment() - 1,
                9,
                0);

        BKSyncLogWriter anotherWriter = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        anotherWriter.write(DLMTestUtil.getLogRecordInstance(numEntries + 3));
        anotherWriter.flush();
        anotherWriter.commit();
        anotherWriter.closeAndComplete();

        for (long i = numEntries + 1; i <= numEntries + 3; i++) {
            LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(i, record.getTransactionId());
        }

        Utils.close(reader);
        readDLM.close();
    }
}
