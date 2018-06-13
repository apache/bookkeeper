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

package org.apache.bookkeeper.tests.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.github.dockerjava.api.DockerClient;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.tests.integration.utils.BookKeeperClusterUtils;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

@Slf4j
@RunWith(Arquillian.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestDlogSmoke {

    @Rule
    public final TestName runtime = new TestName();

    @ArquillianResource
    DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Test
    public void test000_Setup() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
        BookKeeperClusterUtils.createDlogNamespaceIfNeeded(docker, currentVersion, "/distributedlog");
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, currentVersion));
    }

    @Test
    public void test999_Teardown() {
        Assert.assertTrue(BookKeeperClusterUtils.stopAllBookies(docker));
    }

    private URI getDlogUri() {
        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        return URI.create("distributedlog://" + zookeeper + "/distributedlog");
    }

    private static DistributedLogConfiguration newImmediateFlushConf() {
        return new DistributedLogConfiguration()
            .setImmediateFlushEnabled(true)
            .setOutputBufferSize(0)
            .setPeriodicFlushFrequencyMilliSeconds(0);
    }

    private static DistributedLogConfiguration newPeriodicFlushConf(int bufferSize, int periodicFlushMs) {
        return new DistributedLogConfiguration()
            .setImmediateFlushEnabled(false)
            .setOutputBufferSize(bufferSize)
            .setPeriodicFlushFrequencyMilliSeconds(periodicFlushMs);
    }

    @Test
    public void test001_ReadAfterWriteImmediateFlush() throws Exception {
        DistributedLogConfiguration conf = newImmediateFlushConf();
        testReadAfterWrite(conf);
    }

    @Test
    public void test002_ReadAfterWritePeriodicFlushBuffer0() throws Exception {
        DistributedLogConfiguration conf = newPeriodicFlushConf(0, 2);
        testReadAfterWrite(conf);
    }

    @Test
    public void test003_ReadAfterWritePeriodicFlushBuffer512() throws Exception {
        DistributedLogConfiguration conf = newPeriodicFlushConf(0, 2);
        testReadAfterWrite(conf);
    }

    private void testReadAfterWrite(DistributedLogConfiguration conf) throws Exception {
        URI dlogUri = getDlogUri();
        @Cleanup Namespace namespace = NamespaceBuilder.newBuilder()
            .conf(conf)
            .uri(dlogUri)
            .build();

        String streamName = runtime.getMethodName();

        @Cleanup DistributedLogManager writeDlm = namespace.openLog(streamName);
        @Cleanup DistributedLogManager readDlm = namespace.openLog(streamName);

        int numRecords = 100;

        AsyncLogWriter writer = FutureUtils.result(writeDlm.openAsyncLogWriter());
        log.info("Successfully open writer to write to stream {}", writer.getStreamName());
        writeRecords(writer, numRecords);

        AsyncLogReader reader = FutureUtils.result(readDlm.openAsyncLogReader(DLSN.InitialDLSN));
        log.info("Successfully open reader to read from stream {}", reader.getStreamName());
        readRecordsAndClose(reader, numRecords);
    }

    @Test
    public void test004_ReadAfterControlWrite() throws Exception {

        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setOutputBufferSize(1024 * 1024)           // 1MB
            .setImmediateFlushEnabled(false)            // disable immediate flush
            .setPeriodicFlushFrequencyMilliSeconds(0);  // disable periodic flush

        URI dlogUri = getDlogUri();
        @Cleanup Namespace namespace = NamespaceBuilder.newBuilder()
            .conf(conf)
            .uri(dlogUri)
            .build();

        String streamName = runtime.getMethodName();

        @Cleanup DistributedLogManager writeDlm = namespace.openLog(streamName);
        @Cleanup DistributedLogManager readDlm = namespace.openLog(streamName);

        int numRecords = 100;

        AsyncLogWriter writer = FutureUtils.result(writeDlm.openAsyncLogWriter());
        log.info("Successfully open writer to write to stream {}", writer.getStreamName());
        List<CompletableFuture<DLSN>> writeFutures = Lists.newArrayList();
        try {
            for (int i = 0; i < numRecords; i++) {
                LogRecord record = new LogRecord(
                    i,
                    String.format("record-%03d", i).getBytes(UTF_8));
                writeFutures.add(writer.write(record));
                log.info("Write record {}", i);
            }

            LogRecord controlRecord = new LogRecord(
                numRecords,
                String.format("control-record-%03d", numRecords).getBytes(UTF_8));
            controlRecord.setControl();
            log.info("Writing a control record to trigger flushing records written before.");
            DLSN controlDlsn = FutureUtils.result(writer.write(controlRecord));
            log.info("Control record is written at {}", controlDlsn);

            List<DLSN> dlsns = FutureUtils.result(
                FutureUtils.collect(writeFutures));

            // verify the dlsn
            DLSN prevDLSN = DLSN.InvalidDLSN;
            for (DLSN dlsn : dlsns) {
                if (prevDLSN.getLogSegmentSequenceNo() == dlsn.getLogSegmentSequenceNo()) {
                    assertEquals(prevDLSN.getEntryId(), dlsn.getEntryId());
                    assertEquals(prevDLSN.getSlotId() + 1, dlsn.getSlotId());
                } else {
                    assertEquals(
                        prevDLSN.getLogSegmentSequenceNo() + 1,
                        dlsn.getLogSegmentSequenceNo());
                    assertEquals(0L, dlsn.getEntryId());
                    assertEquals(0L, dlsn.getSlotId());
                }
                prevDLSN = dlsn;
            }

        } finally {
            FutureUtils.result(writer.asyncClose());
        }

        AsyncLogReader reader = FutureUtils.result(readDlm.openAsyncLogReader(DLSN.InitialDLSN));
        log.info("Successfully open reader to read from stream {}", reader.getStreamName());
        readRecordsAndClose(reader, numRecords);
    }

    private static void readRecordsAndClose(AsyncLogReader reader, int numRecords) throws Exception {
        try {
            for (long i = 0; i < numRecords; i++) {
                LogRecordWithDLSN record = FutureUtils.result(reader.readNext());
                log.info("Read record {} : {}", i, new String(record.getPayload(), UTF_8));

                assertEquals(i, record.getSequenceId());
                assertEquals(i, record.getTransactionId());
                assertEquals(
                    String.format("record-%03d", i),
                    new String(record.getPayload(), UTF_8));
            }
        } finally {
            FutureUtils.result(reader.asyncClose());
        }
    }

    @Test
    public void test005_WriteAndTailingReadImmediateFlush() throws Exception {
        DistributedLogConfiguration conf = newImmediateFlushConf();
        testWriteAndTailingRead(conf);
    }

    @Test
    public void test006_WriteAndTailingReadPeriodicFlushBuffer0() throws Exception {
        DistributedLogConfiguration conf = newPeriodicFlushConf(0, 2);
        testWriteAndTailingRead(conf);
    }

    @Test
    public void test007_WriteAndTailingReadPeriodicFlushBuffer512() throws Exception {
        DistributedLogConfiguration conf = newPeriodicFlushConf(0, 2);
        testWriteAndTailingRead(conf);
    }

    private void testWriteAndTailingRead(DistributedLogConfiguration conf) throws Exception {
        URI dlogUri = getDlogUri();
        @Cleanup Namespace namespace = NamespaceBuilder.newBuilder()
            .conf(conf)
            .uri(dlogUri)
            .build();

        final String streamName = runtime.getMethodName();
        final int numRecords = 100;


        @Cleanup("shutdown")
        ExecutorService writeExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("write-executor").build());
        final CountDownLatch writeLatch = new CountDownLatch(1);
        @Cleanup("shutdown")
        ExecutorService readExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("read-executor").build());
        final CountDownLatch readLatch = new CountDownLatch(1);

        readExecutor.submit(() -> {
            try {
                readRecords(namespace, streamName, numRecords);
                readLatch.countDown();
            } catch (Exception e) {
                log.error("Failed to read {} records", e);
            }
        });

        writeExecutor.submit(() -> {
            try {
                writeRecords(namespace, streamName, numRecords);
                writeLatch.countDown();
            } catch (Exception e) {
                log.error("Failed to write {} records", e);
            }

        });

        writeLatch.await();
        readLatch.await();
    }

    private static void writeRecords(Namespace namespace, String streamName, int numRecords) throws Exception {

        @Cleanup DistributedLogManager writeDlm = namespace.openLog(streamName);
        AsyncLogWriter writer = FutureUtils.result(writeDlm.openAsyncLogWriter());
        log.info("Successfully open writer to write to stream {}", writer.getStreamName());
        writeRecords(writer, numRecords);
    }

    private static void writeRecords(AsyncLogWriter writer, int numRecords) throws Exception {
        try {
            for (int i = 0; i < numRecords; i++) {
                LogRecord record = new LogRecord(
                    i,
                    String.format("record-%03d", i).getBytes(UTF_8));
                FutureUtils.result(writer.write(record));
                log.info("Write record {}", i);
            }
        } finally {
            FutureUtils.result(writer.asyncClose());
        }
    }

    private static void readRecords(Namespace namespace, String streamName, int numRecords) throws Exception {
        @Cleanup DistributedLogManager readDlm = namespace.openLog(streamName);

        AsyncLogReader reader = null;
        boolean created = false;
        while (!created) {
            try {
                reader = FutureUtils.result(readDlm.openAsyncLogReader(DLSN.InitialDLSN));
                created = true;
            } catch (LogNotFoundException | LogEmptyException e) {
                log.info("Log stream {} is not found, backoff 200 ms", streamName);
                TimeUnit.MILLISECONDS.sleep(200);
            }
        }
        log.info("Successfully open reader to read from stream {}", reader.getStreamName());
        readRecordsAndClose(reader, numRecords);
    }

}
