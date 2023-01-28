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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.logsegment.LogSegmentFilter;
import org.apache.distributedlog.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test {@link BKLogReadHandler}.
 */
public class TestBKLogReadHandler extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestBKLogReadHandler.class);

    @Rule
    public TestName runtime = new TestName();

    private void prepareLogSegmentsNonPartitioned(String name,
                                                  int numSegments, int numEntriesPerSegment) throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, name);
        long txid = 1;
        for (int sid = 0; sid < numSegments; ++sid) {
            LogWriter out = dlm.startLogSegmentNonPartitioned();
            for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
                LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
                out.write(record);
                ++txid;
            }
            out.close();
        }
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNWithOpenLedger() throws Exception {
        String dlName = runtime.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(false);
        confLocal.setOutputBufferSize(0);

        int numEntriesPerSegment = 10;
        DistributedLogManager dlm1 = createNewDLM(confLocal, dlName);
        long txid = 1;

        ArrayList<CompletableFuture<DLSN>> futures = new ArrayList<CompletableFuture<DLSN>>(numEntriesPerSegment);
        AsyncLogWriter out = dlm1.startAsyncLogSegmentNonPartitioned();
        for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
            futures.add(out.write(DLMTestUtil.getLogRecordInstance(txid)));
            ++txid;
        }
        Utils.ioResult(FutureUtils.collect(futures));
        // commit
        LogRecord controlRecord = new LogRecord(txid, DistributedLogConstants.CONTROL_RECORD_CONTENT);
        controlRecord.setControl();
        Utils.ioResult(out.write(controlRecord));

        DLSN last = dlm1.getLastDLSN();
        assertEquals(new DLSN(1, 9, 0), last);
        DLSN first = Utils.ioResult(dlm1.getFirstDLSNAsync());
        assertEquals(new DLSN(1, 0, 0), first);
        Utils.close(out);
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNNoLogSegments() throws Exception {
        String dlName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = dlm.createReadHandler();
        CompletableFuture<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        try {
            Utils.ioResult(futureRecord);
            fail("should have thrown exception");
        } catch (LogNotFoundException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNWithLogSegments() throws Exception {
        String dlName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(conf, dlName);
        DLMTestUtil.generateCompletedLogSegments(dlm, conf, 3, 3);
        BKLogReadHandler readHandler = dlm.createReadHandler();
        CompletableFuture<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        try {
            LogRecordWithDLSN record = Utils.ioResult(futureRecord);
            assertEquals(new DLSN(1, 0, 0), record.getDlsn());
        } catch (Exception ex) {
            fail("should not have thrown exception: " + ex);
        }
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNAfterCleanTruncation() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 3, 10);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler =
            ((BKDistributedLogManager) dlm).createReadHandler();
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();
        CompletableFuture<Boolean> futureSuccess = writer.truncate(new DLSN(2, 0, 0));
        Boolean success = Utils.ioResult(futureSuccess);
        assertTrue(success);
        CompletableFuture<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        LogRecordWithDLSN record = Utils.ioResult(futureRecord);
        assertEquals(new DLSN(2, 0, 0), record.getDlsn());
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNAfterPartialTruncation() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 3, 10);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler =
            ((BKDistributedLogManager) dlm).createReadHandler();
        AsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned();

        // Only truncates at ledger boundary.
        CompletableFuture<Boolean> futureSuccess = writer.truncate(new DLSN(2, 5, 0));
        Boolean success = Utils.ioResult(futureSuccess);
        assertTrue(success);
        CompletableFuture<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        LogRecordWithDLSN record = Utils.ioResult(futureRecord);
        assertEquals(new DLSN(2, 0, 0), record.getDlsn());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountEmptyLedger() throws Exception {
        String dlName = runtime.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(DLSN.InitialDLSN);
        try {
            Utils.ioResult(count);
            fail("log is empty, should have returned log empty ex");
        } catch (LogNotFoundException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountTotalCount() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(DLSN.InitialDLSN);
        assertEquals(33, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountAtLedgerBoundary() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(2, 0, 0));
        assertEquals(30, Utils.ioResult(count).longValue());
        count = readHandler.asyncGetLogRecordCount(new DLSN(3, 0, 0));
        assertEquals(27, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountPastEnd() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(12, 0, 0));
        assertEquals(0, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountLastRecord() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(11, 2, 0));
        assertEquals(1, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountInteriorRecords() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 5, 10);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(3, 5, 0));
        assertEquals(25, Utils.ioResult(count).longValue());
        count = readHandler.asyncGetLogRecordCount(new DLSN(2, 5, 0));
        assertEquals(35, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithControlRecords() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, runtime.getMethodName());
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 5, 5, txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 0, 10, txid);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(15, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithAllControlRecords() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, runtime.getMethodName());
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 5, 0, txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 10, 0, txid);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(0, Utils.ioResult(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithSingleInProgressLedger() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);

        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        int txid = 1;

        Utils.ioResult(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Utils.ioResult(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Utils.ioResult(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));

        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        List<LogSegmentMetadata> ledgerList = Utils.ioResult(
                readHandler.readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                )
        ).getValue();
        assertEquals(1, ledgerList.size());
        assertTrue(ledgerList.get(0).isInProgress());

        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(2, Utils.ioResult(count).longValue());

        Utils.close(out);
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithCompletedAndInprogressLedgers() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);

        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, txid);
        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        Utils.ioResult(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Utils.ioResult(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Utils.ioResult(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));

        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        List<LogSegmentMetadata> ledgerList = Utils.ioResult(
                readHandler.readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null)
        ).getValue();
        assertEquals(2, ledgerList.size());
        assertFalse(ledgerList.get(0).isInProgress());
        assertTrue(ledgerList.get(1).isInProgress());

        CompletableFuture<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(7, Utils.ioResult(count).longValue());

        Utils.close(out);
    }

    @Test(timeout = 60000)
    public void testLockStreamWithMissingLog() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        try {
            Utils.ioResult(readHandler.lockStream());
            fail("Should fail lock stream if log not found");
        } catch (LogNotFoundException ex) {
        }

        BKLogReadHandler subscriberReadHandler = bkdlm.createReadHandler(Optional.of("test-subscriber"));
        try {
            Utils.ioResult(subscriberReadHandler.lockStream());
            fail("Subscriber should fail lock stream if log not found");
        } catch (LogNotFoundException ex) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testLockStreamDifferentSubscribers() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, 1);
        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        Utils.ioResult(readHandler.lockStream());

        // two subscribers could lock stream in parallel
        BKDistributedLogManager bkdlm10 = createNewDLM(conf, streamName);
        BKLogReadHandler s10Handler =
                bkdlm10.createReadHandler(Optional.of("s1"));
        Utils.ioResult(s10Handler.lockStream());
        BKDistributedLogManager bkdlm20 = createNewDLM(conf, streamName);
        BKLogReadHandler s20Handler =
                bkdlm20.createReadHandler(Optional.of("s2"));
        Utils.ioResult(s20Handler.lockStream());

        readHandler.asyncClose();
        bkdlm.close();
        s10Handler.asyncClose();
        bkdlm10.close();
        s20Handler.asyncClose();
        bkdlm20.close();
    }

    @Test(timeout = 60000)
    public void testLockStreamSameSubscriber() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = createNewDLM(conf, streamName);
        DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, 1);
        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        Utils.ioResult(readHandler.lockStream());

        // same subscrbiers couldn't lock stream in parallel
        BKDistributedLogManager bkdlm10 = createNewDLM(conf, streamName);
        BKLogReadHandler s10Handler =
                bkdlm10.createReadHandler(Optional.of("s1"));
        Utils.ioResult(s10Handler.lockStream());

        BKDistributedLogManager bkdlm11 = createNewDLM(conf, streamName);
        BKLogReadHandler s11Handler =
                bkdlm11.createReadHandler(Optional.of("s1"));
        try {
            Utils.ioResult(s11Handler.lockStream(), 10000, TimeUnit.MILLISECONDS);
            fail("Should fail lock stream using same subscriber id");
        } catch (OwnershipAcquireFailedException oafe) {
            // expected
        } catch (TimeoutException te) {
            // expected.
        }

        readHandler.asyncClose();
        bkdlm.close();
        s10Handler.asyncClose();
        bkdlm10.close();
        s11Handler.asyncClose();
        bkdlm11.close();
    }
}
