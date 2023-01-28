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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Ticker;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.exceptions.AlreadyTruncatedTransactionException;
import org.apache.distributedlog.exceptions.DLIllegalStateException;
import org.apache.distributedlog.impl.logsegment.BKLogSegmentEntryStore;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.distributedlog.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


/**
 * Test Case {@link ReadAheadEntryReader}.
 */
public class TestReadAheadEntryReader extends TestDistributedLogBase {

    private static final int MAX_CACHED_ENTRIES = 5;
    private static final int NUM_PREFETCH_ENTRIES = 10;

    @Rule
    public TestName runtime = new TestName();
    private DistributedLogConfiguration baseConf;
    private OrderedScheduler scheduler;
    private BookKeeperClient bkc;
    private ZooKeeperClient zkc;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        baseConf = new DistributedLogConfiguration();
        baseConf.addConfiguration(conf);
        baseConf.setOutputBufferSize(0);
        baseConf.setPeriodicFlushFrequencyMilliSeconds(0);
        baseConf.setImmediateFlushEnabled(false);
        baseConf.setReadAheadMaxRecords(MAX_CACHED_ENTRIES);
        baseConf.setNumPrefetchEntriesPerLogSegment(NUM_PREFETCH_ENTRIES);
        baseConf.setMaxPrefetchEntriesPerLogSegment(NUM_PREFETCH_ENTRIES);
        zkc = ZooKeeperClientBuilder.newBuilder()
                .name("test-zk")
                .zkServers(bkutil.getZkServers())
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .zkAclId(conf.getZkAclId())
                .build();
        bkc = BookKeeperClientBuilder.newBuilder()
                .name("test-bk")
                .dlConfig(conf)
                .ledgersPath("/ledgers")
                .zkServers(bkutil.getZkServers())
                .build();
        scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-read-ahead-entry-reader")
                .numThreads(1)
                .build();
    }

    @Override
    @After
    public void teardown() throws Exception {
        if (null != bkc) {
            bkc.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
        if (null != zkc) {
            zkc.close();
        }
        super.teardown();
    }

    private ReadAheadEntryReader createEntryReader(String streamName,
                                                   DLSN fromDLSN,
                                                   BKDistributedLogManager dlm,
                                                   DistributedLogConfiguration conf)
            throws Exception {
        BKLogReadHandler readHandler = dlm.createReadHandler(
                Optional.<String>empty(),
                true);
        LogSegmentEntryStore entryStore = new BKLogSegmentEntryStore(
                conf,
                ConfUtils.getConstDynConf(conf),
                zkc,
                bkc,
                scheduler,
                null,
                NullStatsLogger.INSTANCE,
                AsyncFailureInjector.NULL);
        return new ReadAheadEntryReader(
                streamName,
                fromDLSN,
                conf,
                readHandler,
                entryStore,
                scheduler,
                Ticker.systemTicker(),
                new AlertStatsLogger(NullStatsLogger.INSTANCE, "test-alert"));
    }

    private void ensureOrderSchedulerEmpty(String streamName) throws Exception {
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        scheduler.executeOrdered(streamName, () -> {
            FutureUtils.complete(promise, null);
        });
        Utils.ioResult(promise);
    }

    void generateCompletedLogSegments(DistributedLogManager dlm,
                                      long numCompletedSegments,
                                      long segmentSize) throws Exception {
        generateCompletedLogSegments(dlm, numCompletedSegments, segmentSize, 1L);
    }

    void generateCompletedLogSegments(DistributedLogManager dlm,
                                      long numCompletedSegments,
                                      long segmentSize,
                                      long startTxId) throws Exception {

        long txid = startTxId;
        for (long i = 0; i < numCompletedSegments; i++) {
            AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
            for (long j = 1; j <= segmentSize; j++) {
                Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(txid++)));
                LogRecord ctrlRecord = DLMTestUtil.getLogRecordInstance(txid);
                ctrlRecord.setControl();
                Utils.ioResult(writer.write(ctrlRecord));
            }
            Utils.close(writer);
        }
    }

    AsyncLogWriter createInprogressLogSegment(DistributedLogManager dlm,
                                              DistributedLogConfiguration conf,
                                              long segmentSize) throws Exception {
        AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
        for (long i = 1L; i <= segmentSize; i++) {
            Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(i)));
            LogRecord ctrlRecord = DLMTestUtil.getLogRecordInstance(i);
            ctrlRecord.setControl();
            Utils.ioResult(writer.write(ctrlRecord));
        }
        return writer;
    }

    void expectAlreadyTruncatedTransactionException(ReadAheadEntryReader reader,
                                                    String errMsg)
            throws Exception {
        try {
            reader.checkLastException();
            fail(errMsg);
        } catch (AlreadyTruncatedTransactionException atte) {
            // expected
        }
    }

    void expectIllegalStateException(ReadAheadEntryReader reader,
                                     String errMsg)
            throws Exception {
        try {
            reader.checkLastException();
            fail(errMsg);
        } catch (DLIllegalStateException le) {
            // expected
        }
    }

    void expectNoException(ReadAheadEntryReader reader) throws Exception {
        reader.checkLastException();
    }

    //
    // Test Positioning
    //

    @Test(timeout = 60000)
    public void testStartWithEmptySegmentList() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(baseConf, streamName);
        ReadAheadEntryReader readAheadEntryReader =
                createEntryReader(streamName, DLSN.InitialDLSN, dlm, baseConf);

        readAheadEntryReader.start(Lists.<LogSegmentMetadata>newArrayList());

        ensureOrderSchedulerEmpty(streamName);
        assertFalse("ReadAhead should not be initialized with empty segment list",
                readAheadEntryReader.isInitialized());
        assertTrue("ReadAhead should be empty when it isn't initialized",
                readAheadEntryReader.isCacheEmpty());
        assertFalse("ReadAhead should not be marked as caught up when it isn't initialized",
                readAheadEntryReader.isReadAheadCaughtUp());

        // generate list of log segments
        generateCompletedLogSegments(dlm, 1, MAX_CACHED_ENTRIES / 2 + 1);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be only one",
                1, segments.size());

        // notify the readahead reader with new segment lsit
        readAheadEntryReader.onSegmentsUpdated(segments);

        // check the reader state after initialization
        ensureOrderSchedulerEmpty(streamName);
        assertTrue("ReadAhead should be initialized with non-empty segment list",
                readAheadEntryReader.isInitialized());
        assertNotNull("current segment reader should be initialized",
                readAheadEntryReader.getCurrentSegmentReader());
        assertEquals("current segment sequence number should be " + segments.get(0).getLogSegmentSequenceNumber(),
                segments.get(0).getLogSegmentSequenceNumber(), readAheadEntryReader.getCurrentSegmentSequenceNumber());
        assertNull("there should be no next segment reader",
                readAheadEntryReader.getNextSegmentReader());
        assertTrue("there should be no remaining segment readers",
                readAheadEntryReader.getSegmentReaders().isEmpty());

        Utils.close(readAheadEntryReader);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testInitializeMultipleClosedLogSegments0() throws Exception {
        // 5 completed log segments, start from the begin
        testInitializeMultipleClosedLogSegments(5, DLSN.InitialDLSN, 0);
    }

    @Test(timeout = 60000)
    public void testInitializeMultipleClosedLogSegments1() throws Exception {
        // 5 completed log segments, start from the 4th segment and it should skip first 3 log segments
        testInitializeMultipleClosedLogSegments(5, new DLSN(4L, 0L, 0L), 3);
    }

    private void testInitializeMultipleClosedLogSegments(
            int numLogSegments,
            DLSN fromDLSN,
            int expectedCurrentSegmentIdx
    ) throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(baseConf, streamName);

        // generate list of log segments
        generateCompletedLogSegments(dlm, 1, MAX_CACHED_ENTRIES / 2 + 1, 1L);
        generateCompletedLogSegments(dlm, numLogSegments - 1, 1, MAX_CACHED_ENTRIES + 2);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        assertEquals(segments.size() + " log segments found, expected to be " + numLogSegments,
                numLogSegments, segments.size());

        ReadAheadEntryReader readAheadEntryReader =
                createEntryReader(streamName, fromDLSN, dlm, baseConf);
        readAheadEntryReader.start(segments);

        ensureOrderSchedulerEmpty(streamName);
        assertTrue("ReadAhead should be initialized with non-empty segment list",
                readAheadEntryReader.isInitialized());
        assertNotNull("current segment reader should be initialized",
                readAheadEntryReader.getCurrentSegmentReader());
        assertTrue("current segment reader should be open and started",
                readAheadEntryReader.getCurrentSegmentReader().isReaderOpen()
                        && readAheadEntryReader.getCurrentSegmentReader().isReaderStarted());
        assertEquals("current segment reader should read " + segments.get(expectedCurrentSegmentIdx),
                segments.get(expectedCurrentSegmentIdx),
                readAheadEntryReader.getCurrentSegmentReader().getSegment());
        assertEquals("current segment sequence number should be "
                + segments.get(expectedCurrentSegmentIdx).getLogSegmentSequenceNumber(),
                segments.get(expectedCurrentSegmentIdx).getLogSegmentSequenceNumber(),
                readAheadEntryReader.getCurrentSegmentSequenceNumber());
        assertNull("next segment reader should not be initialized since it is a closed log segment",
                readAheadEntryReader.getNextSegmentReader());
        assertEquals("there should be " + (numLogSegments - (expectedCurrentSegmentIdx + 1))
                + " remaining segment readers",
                numLogSegments - (expectedCurrentSegmentIdx + 1),
                readAheadEntryReader.getSegmentReaders().size());
        int segmentIdx = expectedCurrentSegmentIdx + 1;
        for (ReadAheadEntryReader.SegmentReader reader : readAheadEntryReader.getSegmentReaders()) {
            LogSegmentMetadata expectedSegment = segments.get(segmentIdx);
            assertEquals("Segment should " + expectedSegment,
                    expectedSegment, reader.getSegment());
            assertTrue("Segment reader for " + expectedSegment + " should be open",
                    reader.isReaderOpen());
            assertFalse("Segment reader for " + expectedSegment + " should not be started",
                    reader.isReaderStarted());
            ++segmentIdx;
        }

        Utils.close(readAheadEntryReader);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testPositioningAtInvalidLogSegment() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(baseConf, streamName);

        // generate list of log segments
        generateCompletedLogSegments(dlm, 3, 3);
        AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
        Utils.ioResult(writer.truncate(new DLSN(2L, 1L, 0L)));

        List<LogSegmentMetadata> segments = dlm.getLogSegments();

        // positioning on a truncated log segment (segment 1)
        ReadAheadEntryReader readAheadEntryReader =
                createEntryReader(streamName, DLSN.InitialDLSN, dlm, baseConf);
        readAheadEntryReader.start(segments);
        // ensure initialization to complete
        ensureOrderSchedulerEmpty(streamName);
        expectNoException(readAheadEntryReader);
        Entry.Reader entryReader =
                readAheadEntryReader.getNextReadAheadEntry(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(2L, entryReader.getLSSN());
        assertEquals(1L, entryReader.getEntryId());
        entryReader.release();
        Utils.close(readAheadEntryReader);

        // positioning on a partially truncated log segment (segment 2) before min active dlsn
        readAheadEntryReader = createEntryReader(streamName, new DLSN(2L, 0L, 0L), dlm, baseConf);
        readAheadEntryReader.start(segments);
        // ensure initialization to complete
        ensureOrderSchedulerEmpty(streamName);
        expectNoException(readAheadEntryReader);
        entryReader =
                readAheadEntryReader.getNextReadAheadEntry(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(2L, entryReader.getLSSN());
        assertEquals(1L, entryReader.getEntryId());
        entryReader.release();
        Utils.close(readAheadEntryReader);

        // positioning on a partially truncated log segment (segment 2) after min active dlsn
        readAheadEntryReader = createEntryReader(streamName, new DLSN(2L, 2L, 0L), dlm, baseConf);
        readAheadEntryReader.start(segments);
        // ensure initialization to complete
        ensureOrderSchedulerEmpty(streamName);
        expectNoException(readAheadEntryReader);
        entryReader =
                readAheadEntryReader.getNextReadAheadEntry(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(2L, entryReader.getLSSN());
        assertEquals(2L, entryReader.getEntryId());
        entryReader.release();
        Utils.close(readAheadEntryReader);

        Utils.close(writer);
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testPositioningIgnoreTruncationStatus() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(baseConf);
        confLocal.setIgnoreTruncationStatus(true);

        String streamName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(confLocal, streamName);

        // generate list of log segments
        generateCompletedLogSegments(dlm, 3, 2);
        AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
        Utils.ioResult(writer.truncate(new DLSN(2L, 1L, 0L)));

        List<LogSegmentMetadata> segments = dlm.getLogSegments();

        // positioning on a truncated log segment (segment 1)
        ReadAheadEntryReader readAheadEntryReader =
                createEntryReader(streamName, DLSN.InitialDLSN, dlm, confLocal);
        readAheadEntryReader.start(segments);
        // ensure initialization to complete
        ensureOrderSchedulerEmpty(streamName);
        expectNoException(readAheadEntryReader);
        Utils.close(readAheadEntryReader);

        // positioning on a partially truncated log segment (segment 2) before min active dlsn
        readAheadEntryReader = createEntryReader(streamName, new DLSN(2L, 0L, 0L), dlm, confLocal);
        readAheadEntryReader.start(segments);
        // ensure initialization to complete
        ensureOrderSchedulerEmpty(streamName);
        expectNoException(readAheadEntryReader);
        Utils.close(readAheadEntryReader);

        // positioning on a partially truncated log segment (segment 2) after min active dlsn
        readAheadEntryReader = createEntryReader(streamName, new DLSN(2L, 1L, 0L), dlm, confLocal);
        readAheadEntryReader.start(segments);
        // ensure initialization to complete
        ensureOrderSchedulerEmpty(streamName);
        expectNoException(readAheadEntryReader);
        Utils.close(readAheadEntryReader);

        Utils.close(writer);
        dlm.close();
    }

    //
    // Test Reinitialization
    //

    @Test(timeout = 60000)
    public void testLogSegmentSequenceNumberGap() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(baseConf, streamName);

        // generate list of log segments
        generateCompletedLogSegments(dlm, 3, 2);
        List<LogSegmentMetadata> segments = dlm.getLogSegments();

        ReadAheadEntryReader readAheadEntryReader =
                createEntryReader(streamName, DLSN.InitialDLSN, dlm, baseConf);
        readAheadEntryReader.start(segments.subList(0, 1));
        int expectedCurrentSegmentIdx = 0;
        ensureOrderSchedulerEmpty(streamName);
        assertTrue("ReadAhead should be initialized with non-empty segment list",
                readAheadEntryReader.isInitialized());
        assertNotNull("current segment reader should be initialized",
                readAheadEntryReader.getCurrentSegmentReader());
        assertTrue("current segment reader should be open and started",
                readAheadEntryReader.getCurrentSegmentReader().isReaderOpen()
                        && readAheadEntryReader.getCurrentSegmentReader().isReaderStarted());
        assertEquals("current segment reader should read " + segments.get(expectedCurrentSegmentIdx),
                segments.get(expectedCurrentSegmentIdx),
                readAheadEntryReader.getCurrentSegmentReader().getSegment());
        assertEquals("current segment sequence number should be "
                + segments.get(expectedCurrentSegmentIdx).getLogSegmentSequenceNumber(),
                segments.get(expectedCurrentSegmentIdx).getLogSegmentSequenceNumber(),
                readAheadEntryReader.getCurrentSegmentSequenceNumber());
        assertNull("next segment reader should not be initialized since it is a closed log segment",
                readAheadEntryReader.getNextSegmentReader());

        readAheadEntryReader.onSegmentsUpdated(segments.subList(2, 3));
        ensureOrderSchedulerEmpty(streamName);
        expectIllegalStateException(readAheadEntryReader,
                "inconsistent log segment found");

        Utils.close(readAheadEntryReader);
        dlm.close();
    }

}
