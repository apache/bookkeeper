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

import com.google.common.base.Optional;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.logsegment.LogSegmentFilter;
import org.apache.distributedlog.util.FutureUtils;
import org.apache.distributedlog.util.Utils;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Await;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.twitter.util.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Test {@link BKLogReadHandler}
 */
public class TestBKLogReadHandler extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestBKLogReadHandler.class);

    @Rule
    public TestName runtime = new TestName();

    private void prepareLogSegmentsNonPartitioned(String name, int numSegments, int numEntriesPerSegment) throws Exception {
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

        ArrayList<Future<DLSN>> futures = new ArrayList<Future<DLSN>>(numEntriesPerSegment);
        AsyncLogWriter out = dlm1.startAsyncLogSegmentNonPartitioned();
        for (int eid = 0; eid < numEntriesPerSegment; ++eid) {
            futures.add(out.write(DLMTestUtil.getLogRecordInstance(txid)));
            ++txid;
        }
        FutureUtils.result(Future.collect(futures));
        // commit
        LogRecord controlRecord = new LogRecord(txid, DistributedLogConstants.CONTROL_RECORD_CONTENT);
        controlRecord.setControl();
        FutureUtils.result(out.write(controlRecord));

        DLSN last = dlm1.getLastDLSN();
        assertEquals(new DLSN(1,9,0), last);
        DLSN first = Await.result(dlm1.getFirstDLSNAsync());
        assertEquals(new DLSN(1,0,0), first);
        Utils.close(out);
    }

    @Test(timeout = 60000)
    public void testGetFirstDLSNNoLogSegments() throws Exception {
        String dlName = runtime.getMethodName();
        BKDistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = dlm.createReadHandler();
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        try {
            Await.result(futureRecord);
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
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        try {
            LogRecordWithDLSN record = Await.result(futureRecord);
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
        Future<Boolean> futureSuccess = writer.truncate(new DLSN(2, 0, 0));
        Boolean success = Await.result(futureSuccess);
        assertTrue(success);
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        LogRecordWithDLSN record = Await.result(futureRecord);
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
        Future<Boolean> futureSuccess = writer.truncate(new DLSN(2, 5, 0));
        Boolean success = Await.result(futureSuccess);
        assertTrue(success);
        Future<LogRecordWithDLSN> futureRecord = readHandler.asyncGetFirstLogRecord();
        LogRecordWithDLSN record = Await.result(futureRecord);
        assertEquals(new DLSN(2, 0, 0), record.getDlsn());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountEmptyLedger() throws Exception {
        String dlName = runtime.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(DLSN.InitialDLSN);
        try {
            Await.result(count);
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
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(DLSN.InitialDLSN);
        assertEquals(33, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountAtLedgerBoundary() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(2, 0, 0));
        assertEquals(30, Await.result(count).longValue());
        count = readHandler.asyncGetLogRecordCount(new DLSN(3, 0, 0));
        assertEquals(27, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountPastEnd() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(12, 0, 0));
        assertEquals(0, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountLastRecord() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 11, 3);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(11, 2, 0));
        assertEquals(1, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountInteriorRecords() throws Exception {
        String dlName = runtime.getMethodName();
        prepareLogSegmentsNonPartitioned(dlName, 5, 10);
        DistributedLogManager dlm = createNewDLM(conf, dlName);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(3, 5, 0));
        assertEquals(25, Await.result(count).longValue());
        count = readHandler.asyncGetLogRecordCount(new DLSN(2, 5, 0));
        assertEquals(35, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithControlRecords() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, runtime.getMethodName());
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 5, 5, txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 0, 10, txid);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(15, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithAllControlRecords() throws Exception {
        DistributedLogManager dlm = createNewDLM(conf, runtime.getMethodName());
        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 5, 0, txid);
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(dlm, 10, 0, txid);
        BKLogReadHandler readHandler = ((BKDistributedLogManager) dlm).createReadHandler();
        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(0, Await.result(count).longValue());
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithSingleInProgressLedger() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);

        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        int txid = 1;

        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));

        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        List<LogSegmentMetadata> ledgerList = FutureUtils.result(
                readHandler.readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null
                )
        ).getValue();
        assertEquals(1, ledgerList.size());
        assertTrue(ledgerList.get(0).isInProgress());

        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(2, Await.result(count).longValue());

        Utils.close(out);
    }

    @Test(timeout = 60000)
    public void testGetLogRecordCountWithCompletedAndInprogressLedgers() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);

        long txid = 1;
        txid += DLMTestUtil.generateLogSegmentNonPartitioned(bkdlm, 0, 5, txid);
        AsyncLogWriter out = bkdlm.startAsyncLogSegmentNonPartitioned();
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));
        Await.result(out.write(DLMTestUtil.getLargeLogRecordInstance(txid++, false)));

        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        List<LogSegmentMetadata> ledgerList = FutureUtils.result(
                readHandler.readLogSegmentsFromStore(
                        LogSegmentMetadata.COMPARATOR,
                        LogSegmentFilter.DEFAULT_FILTER,
                        null)
        ).getValue();
        assertEquals(2, ledgerList.size());
        assertFalse(ledgerList.get(0).isInProgress());
        assertTrue(ledgerList.get(1).isInProgress());

        Future<Long> count = null;
        count = readHandler.asyncGetLogRecordCount(new DLSN(1, 0, 0));
        assertEquals(7, Await.result(count).longValue());

        Utils.close(out);
    }

    @Test(timeout = 60000)
    public void testLockStreamWithMissingLog() throws Exception {
        String streamName = runtime.getMethodName();
        BKDistributedLogManager bkdlm = (BKDistributedLogManager) createNewDLM(conf, streamName);
        BKLogReadHandler readHandler = bkdlm.createReadHandler();
        try {
            Await.result(readHandler.lockStream());
            fail("Should fail lock stream if log not found");
        } catch (LogNotFoundException ex) {
        }

        BKLogReadHandler subscriberReadHandler = bkdlm.createReadHandler(Optional.of("test-subscriber"));
        try {
            Await.result(subscriberReadHandler.lockStream());
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
        Await.result(readHandler.lockStream());

        // two subscribers could lock stream in parallel
        BKDistributedLogManager bkdlm10 = createNewDLM(conf, streamName);
        BKLogReadHandler s10Handler =
                bkdlm10.createReadHandler(Optional.of("s1"));
        Await.result(s10Handler.lockStream());
        BKDistributedLogManager bkdlm20 = createNewDLM(conf, streamName);
        BKLogReadHandler s20Handler =
                bkdlm20.createReadHandler(Optional.of("s2"));
        Await.result(s20Handler.lockStream());

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
        Await.result(readHandler.lockStream());

        // same subscrbiers couldn't lock stream in parallel
        BKDistributedLogManager bkdlm10 = createNewDLM(conf, streamName);
        BKLogReadHandler s10Handler =
                bkdlm10.createReadHandler(Optional.of("s1"));
        Await.result(s10Handler.lockStream());

        BKDistributedLogManager bkdlm11 = createNewDLM(conf, streamName);
        BKLogReadHandler s11Handler =
                bkdlm11.createReadHandler(Optional.of("s1"));
        try {
            Await.result(s11Handler.lockStream(), Duration.apply(10000, TimeUnit.MILLISECONDS));
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
