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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Test Sync Log Reader.
 */
public class TestBKSyncLogReader extends TestDistributedLogBase {

    private static final Logger logger = LoggerFactory.getLogger(TestBKSyncLogReader.class);

    @Rule
    public TestName testName = new TestName();

    @Test(timeout = 60000)
    public void testCreateReaderBeyondLastTransactionId() throws Exception {
        String name = testName.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i < 10; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        LogReader reader = dlm.getInputStream(20L);
        assertNull(reader.readNext(false));

        // write another 20 records
        out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 10; i < 30; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        for (int i = 0; i < 10; i++) {
            LogRecord record = waitForNextRecord(reader);
            assertEquals(20L + i, record.getTransactionId());
        }
        assertNull(reader.readNext(false));
    }

    @Test(timeout = 60000)
    public void testDeletingLogWhileReading() throws Exception {
        String name = testName.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1; i < 10; i++) {
            LogRecord op = DLMTestUtil.getLogRecordInstance(i);
            out.write(op);
        }
        out.closeAndComplete();

        LogReader reader = dlm.getInputStream(1L);
        for (int i = 1; i < 10; i++) {
            LogRecord record = waitForNextRecord(reader);
            assertEquals((long) i, record.getTransactionId());
        }

        DistributedLogManager deleteDLM = createNewDLM(conf, name);
        deleteDLM.delete();

        LogRecord record;
        try {
            record = reader.readNext(false);
            while (null == record) {
                record = reader.readNext(false);
            }
            fail("Should fail reading next with LogNotFound");
        } catch (LogNotFoundException lnfe) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testReadingFromEmptyLog() throws Exception {
        String name = testName.getMethodName();
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        // write a record but not commit
        LogRecord op = DLMTestUtil.getLogRecordInstance(1L);
        out.write(op);

        LogReader reader = dlm.getInputStream(1L);
        assertNull(reader.readNext(true));
        assertNull(reader.readNext(false));

        op = DLMTestUtil.getLogRecordInstance(2L);
        out.write(op);

        // reader is able to read first record
        LogRecord record = waitForNextRecord(reader);
        assertNotNull(record);
        assertEquals(1L, record.getTransactionId());
        DLMTestUtil.verifyLogRecord(record);

        assertNull(reader.readNext(true));

        out.close();
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testReadRecordsAfterReadAheadCaughtUp() throws Exception {
        String name = testName.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1L; i <= 10L; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(i);
            out.write(record);
        }
        out.flush();
        out.commit();

        logger.info("Write first 10 records");

        // all 10 records are added to the stream
        // then open a reader to read
        BKSyncLogReader reader = (BKSyncLogReader) dlm.getInputStream(1L);

        // wait until readahead caught up
        while (!reader.getReadAheadReader().isReadAheadCaughtUp()) {
            TimeUnit.MILLISECONDS.sleep(20);
        }

        logger.info("ReadAhead is caught up with first 10 records");

        for (long i = 11L; i <= 20L; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(i);
            out.write(record);
        }
        out.flush();
        out.commit();

        logger.info("Write another 10 records");

        // resume reading from sync reader util it consumes 20 records
        long expectedTxId = 1L;
        for (int i = 0; i < 20; i++) {
            LogRecord record = reader.readNext(false);
            while (null == record) {
                record = reader.readNext(false);
            }
            assertEquals(expectedTxId, record.getTransactionId());
            DLMTestUtil.verifyLogRecord(record);
            ++expectedTxId;
        }
        // after read 20 records, it should return null
        assertNull(reader.readNext(false));

        out.close();
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testReadRecordsWhenReadAheadCatchingUp() throws Exception {
        String name = testName.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);
        confLocal.setReadAheadMaxRecords(1);
        confLocal.setReadAheadBatchSize(1);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1L; i <= 10L; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(i);
            out.write(record);
        }
        out.flush();
        out.commit();

        logger.info("Write first 10 records");

        // open a reader to read
        BKSyncLogReader reader = (BKSyncLogReader) dlm.getInputStream(1L);
        // resume reading from sync reader. so it should be able to read all 10 records
        // and return null to claim it as caughtup
        LogRecord record = reader.readNext(false);
        int numReads = 0;
        long expectedTxId = 1L;
        while (null != record) {
            ++numReads;
            assertEquals(expectedTxId, record.getTransactionId());
            DLMTestUtil.verifyLogRecord(record);
            ++expectedTxId;
            record = reader.readNext(false);
        }
        assertEquals(10, numReads);

        out.close();
        reader.close();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testReadRecordsWhenReadAheadCatchingUp2() throws Exception {
        String name = testName.getMethodName();

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setPeriodicFlushFrequencyMilliSeconds(Integer.MAX_VALUE);

        DistributedLogManager dlm = createNewDLM(confLocal, name);
        final BKSyncLogWriter out = (BKSyncLogWriter) dlm.startLogSegmentNonPartitioned();
        for (long i = 1L; i <= 10L; i++) {
            LogRecord record = DLMTestUtil.getLogRecordInstance(i);
            out.write(record);
        }
        out.flush();
        out.commit();
        final AtomicLong nextTxId = new AtomicLong(11L);

        logger.info("Write first 10 records");

        ScheduledExecutorService executorService =
                Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long txid = nextTxId.getAndIncrement();
                LogRecord record = DLMTestUtil.getLogRecordInstance(txid);
                try {
                    out.write(record);
                } catch (IOException e) {
                    // ignore the ioe
                }
            }
        }, 0, 400, TimeUnit.MILLISECONDS);

        // open a reader to read
        BKSyncLogReader reader = (BKSyncLogReader) dlm.getInputStream(1L);
        // resume reading from sync reader. so it should be able to read all 10 records
        // and return null to claim it as caughtup
        LogRecord record = reader.readNext(false);
        int numReads = 0;
        long expectedTxId = 1L;
        while (null != record) {
            ++numReads;
            assertEquals(expectedTxId, record.getTransactionId());
            DLMTestUtil.verifyLogRecord(record);
            ++expectedTxId;
            record = reader.readNext(false);
        }
        assertTrue(numReads >= 10);

        executorService.shutdown();
        out.close();
        reader.close();
        dlm.close();
    }
}
