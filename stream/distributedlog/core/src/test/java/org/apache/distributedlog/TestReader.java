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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A Reader wraps reading next logic for testing.
 */
public class TestReader implements FutureEventListener<LogRecordWithDLSN> {

    static final Logger LOG = LoggerFactory.getLogger(TestReader.class);

    final String readerName;
    final DistributedLogManager dlm;
    AsyncLogReader reader;
    final DLSN startDLSN;
    DLSN nextDLSN;
    final boolean simulateErrors;
    int delayMs;
    final ScheduledExecutorService executorService;

    // Latches

    // Latch on notifying reader is ready to read
    final CountDownLatch readyLatch;
    // Latch no notifying reads are completed or errors are encountered
    final CountDownLatch completionLatch;
    // Latch no notifying reads are done.
    final CountDownLatch countLatch;

    // States
    final AtomicBoolean errorsFound;
    final AtomicInteger readCount;
    final AtomicInteger positionReaderCount;

    public TestReader(String name,
                      DistributedLogManager dlm,
                      DLSN startDLSN,
                      boolean simulateErrors,
                      int delayMs,
                      CountDownLatch readyLatch,
                      CountDownLatch countLatch,
                      CountDownLatch completionLatch) {
        this.readerName = name;
        this.dlm = dlm;
        this.startDLSN = startDLSN;
        this.simulateErrors = simulateErrors;
        this.delayMs = delayMs;
        this.readyLatch = readyLatch;
        this.countLatch = countLatch;
        this.completionLatch = completionLatch;
        // States
        this.errorsFound = new AtomicBoolean(false);
        this.readCount = new AtomicInteger(0);
        this.positionReaderCount = new AtomicInteger(0);
        // Executors
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public AtomicInteger getNumReaderPositions() {
        return this.positionReaderCount;
    }

    public AtomicInteger getNumReads() {
        return this.readCount;
    }

    public boolean areErrorsFound() {
        return errorsFound.get();
    }

    private int nextDelayMs() {
        int newDelayMs = Math.min(delayMs * 2, 500);
        if (0 == delayMs) {
            newDelayMs = 10;
        }
        delayMs = newDelayMs;
        return delayMs;
    }

    private void positionReader(final DLSN dlsn) {
        positionReaderCount.incrementAndGet();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncLogReader reader = dlm.getAsyncLogReader(dlsn);
                    if (simulateErrors) {
                        ((BKAsyncLogReader) reader).simulateErrors();
                    }
                    nextDLSN = dlsn;
                    LOG.info("Positioned reader {} at {}", readerName, dlsn);
                    if (null != TestReader.this.reader) {
                        Utils.close(TestReader.this.reader);
                    }
                    TestReader.this.reader = reader;
                    readNext();
                    readyLatch.countDown();
                } catch (IOException exc) {
                    int nextMs = nextDelayMs();
                    LOG.info("Encountered exception {} on opening reader {} at {}, retrying in {} ms",
                        exc, readerName, dlsn, nextMs);
                    positionReader(dlsn);
                }
            }
        };
        executorService.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
    }

    private void readNext() {
        CompletableFuture<LogRecordWithDLSN> record = reader.readNext();
        record.whenComplete(this);
    }

    @Override
    public void onSuccess(LogRecordWithDLSN value) {
        try {
            assertTrue(value.getDlsn().compareTo(nextDLSN) >= 0);
            LOG.info("Received record {} from log {} for reader {}",
                value.getDlsn(), dlm.getStreamName(), readerName);
            assertFalse(value.isControl());
            assertEquals(0, value.getDlsn().getSlotId());
            DLMTestUtil.verifyLargeLogRecord(value);
        } catch (Exception exc) {
            LOG.error("Exception encountered when verifying received log record {} for reader {} :",
                value.getDlsn(), readerName, exc);
            errorsFound.set(true);
            completionLatch.countDown();
            return;
        }
        readCount.incrementAndGet();
        countLatch.countDown();
        if (countLatch.getCount() <= 0) {
            LOG.info("Reader {} is completed", readerName);
            closeReader();
            completionLatch.countDown();
        } else {
            LOG.info("Reader {} : read count becomes {}, latch = {}",
                readerName, readCount.get(), countLatch.getCount());
            nextDLSN = value.getDlsn().getNextDLSN();
            readNext();
        }
    }

    @Override
    public void onFailure(Throwable cause) {
        LOG.error("{} encountered exception on reading next record : ", readerName, cause);
        closeReader();
        nextDelayMs();
        positionReader(nextDLSN);
    }

    private void closeReader() {
        if (null != reader) {
            reader.asyncClose().whenComplete((value, cause) -> {
                LOG.warn("Exception on closing reader {} : ", readerName, cause);
            });
        }
    }

    public void start() {
        positionReader(startDLSN);
    }

    public void stop() {
        closeReader();
        executorService.shutdown();
    }

}
