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

import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.exceptions.LockCancelledException;
import org.apache.distributedlog.exceptions.LockingException;
import org.apache.distributedlog.exceptions.OwnershipAcquireFailedException;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.lock.LockClosedException;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * TestAsyncReaderLock.
 */
public class TestAsyncReaderLock extends TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestAsyncReaderLock.class);

    @Rule
    public TestName runtime = new TestName();

    void assertAcquiredFlagsSet(boolean[] acquiredFlags, int endIndex) {
        for (int i = 0; i < endIndex; i++) {
            assertTrue("reader " + i + " should have acquired lock", acquiredFlags[i]);
        }
        for (int i = endIndex; i < acquiredFlags.length; i++) {
            assertFalse("reader " + i + " should not have acquired lock", acquiredFlags[i]);
        }
    }

    @Test(timeout = 60000)
    public void testReaderLockIfLockPathDoesntExist() throws Exception {
        final String name = runtime.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.closeAndComplete();

        CompletableFuture<AsyncLogReader> futureReader1 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        BKAsyncLogReader reader1 = (BKAsyncLogReader) Utils.ioResult(futureReader1);
        LogRecordWithDLSN record = Utils.ioResult(reader1.readNext());
        assertEquals(1L, record.getTransactionId());
        assertEquals(0L, record.getSequenceId());
        DLMTestUtil.verifyLogRecord(record);

        String readLockPath = reader1.readHandler.getReadLockPath();
        Utils.close(reader1);

        // simulate a old stream created without readlock path
        NamespaceDriver driver = dlm.getNamespaceDriver();
        ((BKNamespaceDriver) driver).getWriterZKC().get().delete(readLockPath, -1);
        CompletableFuture<AsyncLogReader> futureReader2 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader2 = Utils.ioResult(futureReader2);
        record = Utils.ioResult(reader2.readNext());
        assertEquals(1L, record.getTransactionId());
        assertEquals(0L, record.getSequenceId());
        DLMTestUtil.verifyLogRecord(record);
    }

    @Test(timeout = 60000)
    public void testReaderLockCloseInAcquireCallback() throws Exception {
        final String name = runtime.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.closeAndComplete();

        final CountDownLatch latch = new CountDownLatch(1);

        CompletableFuture<AsyncLogReader> futureReader1 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        futureReader1
            .thenCompose(
                reader -> reader.asyncClose()
                    .thenApply(result -> {
                        latch.countDown();
                        return null;
                    }));

        latch.await();
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockBackgroundReaderLockAcquire() throws Exception {
        final String name = runtime.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.closeAndComplete();

        CompletableFuture<AsyncLogReader> futureReader1 = dlm.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Utils.ioResult(futureReader1);
        reader1.readNext();

        final CountDownLatch acquiredLatch = new CountDownLatch(1);
        final AtomicBoolean acquired = new AtomicBoolean(false);
        Thread acquireThread = new Thread(new Runnable() {
            @Override
            public void run() {
                CompletableFuture<AsyncLogReader> futureReader2 = null;
                DistributedLogManager dlm2 = null;
                try {
                    dlm2 = createNewDLM(conf, name);
                    futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
                    AsyncLogReader reader2 = Utils.ioResult(futureReader2);
                    acquired.set(true);
                    acquiredLatch.countDown();
                } catch (Exception ex) {
                    fail("shouldn't reach here");
                } finally {
                    try {
                        dlm2.close();
                    } catch (Exception ex) {
                        fail("shouldn't reach here");
                    }
                }
            }
        }, "acquire-thread");
        acquireThread.start();

        Thread.sleep(1000);
        assertEquals(false, acquired.get());
        Utils.close(reader1);

        acquiredLatch.await();
        assertEquals(true, acquired.get());
        dlm.close();
    }

    int countDefined(ArrayList<CompletableFuture<AsyncLogReader>> readers) {
        int done = 0;
        for (CompletableFuture<AsyncLogReader> futureReader : readers) {
            if (futureReader.isDone()) {
                done++;
            }
        }
        return done;
    }

    @Test(timeout = 60000)
    public void testReaderLockManyLocks() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        int count = 5;
        final CountDownLatch acquiredLatch = new CountDownLatch(count);
        final ArrayList<CompletableFuture<AsyncLogReader>> readers =
                new ArrayList<CompletableFuture<AsyncLogReader>>(count);
        for (int i = 0; i < count; i++) {
            readers.add(null);
        }
        final DistributedLogManager[] dlms = new DistributedLogManager[count];
        for (int i = 0; i < count; i++) {
            dlms[i] = createNewDLM(conf, name);
            readers.set(i, dlms[i].getAsyncLogReaderWithLock(DLSN.InitialDLSN));
            readers.get(i).whenComplete(new FutureEventListener<AsyncLogReader>() {
                @Override
                public void onSuccess(AsyncLogReader reader) {
                    acquiredLatch.countDown();
                    reader.asyncClose();
                }
                @Override
                public void onFailure(Throwable cause) {
                    fail("acquire shouldnt have failed");
                }
            });
        }

        acquiredLatch.await();
        for (int i = 0; i < count; i++) {
            dlms[i].close();
        }

        dlm.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockDlmClosed() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = createNewDLM(conf, name);
        CompletableFuture<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Utils.ioResult(futureReader1);

        BKDistributedLogManager dlm2 = (BKDistributedLogManager) createNewDLM(conf, name);
        CompletableFuture<AsyncLogReader> futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        dlm2.close();
        try {
            Utils.ioResult(futureReader2);
            fail("should have thrown exception!");
        } catch (CancellationException ce) {
        } catch (LockClosedException ex) {
        } catch (LockCancelledException ex) {
        }

        Utils.close(reader1);
        dlm0.close();
        dlm1.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockSessionExpires() throws Exception {
        String name = runtime.getMethodName();
        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);
        Namespace ns0 = NamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();
        DistributedLogManager dlm0 = ns0.openLog(name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        Namespace ns1 = NamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();
        DistributedLogManager dlm1 = ns1.openLog(name);
        CompletableFuture<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Utils.ioResult(futureReader1);
        ZooKeeperClientUtils.expireSession(((BKNamespaceDriver)
                ns1.getNamespaceDriver()).getWriterZKC(), zkServers, 1000);

        // The result of expireSession is somewhat non-deterministic with this lock.
        // It may fail with LockingException or it may successfully reacquire, so for
        // the moment rather than make it deterministic we accept either result.
        boolean success = false;
        try {
            Utils.ioResult(reader1.readNext());
            success = true;
        } catch (LockingException ex) {
        }
        if (success) {
            Utils.ioResult(reader1.readNext());
        }

        Utils.close(reader1);
        dlm0.close();
        ns0.close();
        dlm1.close();
        ns1.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockFutureCancelledWhileWaiting() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = createNewDLM(conf, name);
        CompletableFuture<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Utils.ioResult(futureReader1);

        DistributedLogManager dlm2 = createNewDLM(conf, name);
        CompletableFuture<AsyncLogReader> futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        try {
            futureReader2.cancel(true);
            Utils.ioResult(futureReader2);
            fail("Should fail getting log reader as it is cancelled");
        } catch (CancellationException ce) {
        } catch (LockClosedException ex) {
        } catch (LockCancelledException ex) {
        } catch (OwnershipAcquireFailedException oafe) {
        }

        futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        Utils.close(reader1);

        Utils.ioResult(futureReader2);

        dlm0.close();
        dlm1.close();
        dlm2.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockFutureCancelledWhileLocked() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = createNewDLM(conf, name);
        CompletableFuture<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        // Must not throw or cancel or do anything bad, future already completed.
        Utils.ioResult(futureReader1);
        futureReader1.cancel(true);
        AsyncLogReader reader1 = Utils.ioResult(futureReader1);
        Utils.ioResult(reader1.readNext());

        dlm0.close();
        dlm1.close();
    }

    @Test(timeout = 60000)
    public void testReaderLockSharedDlmDoesNotConflict() throws Exception {
        String name = runtime.getMethodName();
        DistributedLogManager dlm0 = createNewDLM(conf, name);
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm0.startAsyncLogSegmentNonPartitioned());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        writer.write(DLMTestUtil.getLogRecordInstance(2L));
        writer.closeAndComplete();

        DistributedLogManager dlm1 = createNewDLM(conf, name);
        CompletableFuture<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        CompletableFuture<AsyncLogReader> futureReader2 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        // Both use the same client id, so there's no lock conflict. Not necessarily ideal, but how the
        // system currently works.
        Utils.ioResult(futureReader1);
        Utils.ioResult(futureReader2);

        dlm0.close();
        dlm1.close();
    }

    static class ReadRecordsListener implements FutureEventListener<AsyncLogReader> {

        final AtomicReference<DLSN> currentDLSN;
        final String name;
        final ExecutorService executorService;

        final CountDownLatch latch = new CountDownLatch(1);
        boolean failed = false;

        public ReadRecordsListener(AtomicReference<DLSN> currentDLSN,
                                   String name,
                                   ExecutorService executorService) {
            this.currentDLSN = currentDLSN;
            this.name = name;
            this.executorService = executorService;
        }
        public CountDownLatch getLatch() {
            return latch;
        }
        public boolean failed() {
            return failed;
        }
        public boolean done() {
            return latch.getCount() == 0;
        }

        @Override
        public void onSuccess(final AsyncLogReader reader) {
            LOG.info("Reader {} is ready to read entries", name);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    readEntries(reader);
                }
            });
        }

        private void readEntries(AsyncLogReader reader) {
            try {
                for (int i = 0; i < 300; i++) {
                    LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
                    currentDLSN.set(record.getDlsn());
                }
            } catch (Exception ex) {
                failed = true;
            } finally {
                latch.countDown();
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            LOG.error("{} failed to open reader", name, cause);
            failed = true;
            latch.countDown();
        }
    }

    @Test(timeout = 60000)
    public void testReaderLockMultiReadersScenario() throws Exception {
        final String name = runtime.getMethodName();
        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);

        DistributedLogConfiguration localConf = new DistributedLogConfiguration();
        localConf.addConfiguration(conf);
        localConf.setImmediateFlushEnabled(false);
        localConf.setPeriodicFlushFrequencyMilliSeconds(60 * 1000);
        localConf.setOutputBufferSize(0);
        // Otherwise, we won't be able to run scheduled threads for readahead when we're in a callback.
        localConf.setNumWorkerThreads(2);
        localConf.setLockTimeout(Long.MAX_VALUE);

        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(localConf).uri(uri).clientId("main").build();

        DistributedLogManager dlm0 = namespace.openLog(name);
        DLMTestUtil.generateCompletedLogSegments(dlm0, localConf, 9, 100);
        dlm0.close();

        int recordCount = 0;
        AtomicReference<DLSN> currentDLSN = new AtomicReference<DLSN>(DLSN.InitialDLSN);

        String clientId1 = "reader1";
        Namespace namespace1 = NamespaceBuilder.newBuilder()
                .conf(localConf).uri(uri).clientId(clientId1).build();
        DistributedLogManager dlm1 = namespace1.openLog(name);
        String clientId2 = "reader2";
        Namespace namespace2 = NamespaceBuilder.newBuilder()
                .conf(localConf).uri(uri).clientId(clientId2).build();
        DistributedLogManager dlm2 = namespace2.openLog(name);
        String clientId3 = "reader3";
        Namespace namespace3 = NamespaceBuilder.newBuilder()
                .conf(localConf).uri(uri).clientId(clientId3).build();
        DistributedLogManager dlm3 = namespace3.openLog(name);

        LOG.info("{} is opening reader on stream {}", clientId1, name);
        CompletableFuture<AsyncLogReader> futureReader1 = dlm1.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        AsyncLogReader reader1 = Utils.ioResult(futureReader1);
        LOG.info("{} opened reader on stream {}", clientId1, name);

        LOG.info("{} is opening reader on stream {}", clientId2, name);
        CompletableFuture<AsyncLogReader> futureReader2 = dlm2.getAsyncLogReaderWithLock(DLSN.InitialDLSN);
        LOG.info("{} is opening reader on stream {}", clientId3, name);
        CompletableFuture<AsyncLogReader> futureReader3 = dlm3.getAsyncLogReaderWithLock(DLSN.InitialDLSN);

        ExecutorService executorService = Executors.newCachedThreadPool();

        ReadRecordsListener listener2 =
                new ReadRecordsListener(currentDLSN, clientId2, executorService);
        ReadRecordsListener listener3 =
                new ReadRecordsListener(currentDLSN, clientId3, executorService);
        futureReader2.whenComplete(listener2);
        futureReader3.whenComplete(listener3);

        // Get reader1 and start reading.
        for ( ; recordCount < 200; recordCount++) {
            LogRecordWithDLSN record = Utils.ioResult(reader1.readNext());
            currentDLSN.set(record.getDlsn());
        }

        // Take a break, reader2 decides to stop waiting and cancels.
        Thread.sleep(1000);
        assertFalse(listener2.done());
        futureReader2.cancel(true);
        listener2.getLatch().await();
        assertTrue(listener2.done());
        assertTrue(listener2.failed());

        // Reader1 starts reading again.
        for (; recordCount < 300; recordCount++) {
            LogRecordWithDLSN record = Utils.ioResult(reader1.readNext());
            currentDLSN.set(record.getDlsn());
        }

        // Reader1 is done, someone else can take over. Since reader2 was
        // aborted, reader3 should take its place.
        assertFalse(listener3.done());
        Utils.close(reader1);
        listener3.getLatch().await();
        assertTrue(listener3.done());
        assertFalse(listener3.failed());

        assertEquals(new DLSN(3, 99, 0), currentDLSN.get());

        try {
            Utils.ioResult(futureReader2);
        } catch (Exception ex) {
            // Can't get this one to close it--the dlm will take care of it.
        }

        Utils.close(Utils.ioResult(futureReader3));

        dlm1.close();
        namespace1.close();
        dlm2.close();
        namespace2.close();
        dlm3.close();
        namespace3.close();

        executorService.shutdown();
    }

    @Test(timeout = 60000)
    public void testAsyncReadWithSubscriberId() throws Exception {
        String name = "distrlog-asyncread-with-sbuscriber-id";
        String subscriberId = "asyncreader";
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setImmediateFlushEnabled(true);

        DistributedLogManager dlm = createNewDLM(confLocal, name);

        DLSN readDLSN = DLSN.InitialDLSN;

        int txid = 1;
        for (long i = 0; i < 3; i++) {
            BKAsyncLogWriter writer = (BKAsyncLogWriter) dlm.startAsyncLogSegmentNonPartitioned();
            for (long j = 1; j <= 10; j++) {
                DLSN dlsn = Utils.ioResult(writer.write(DLMTestUtil.getEmptyLogRecordInstance(txid++)));
                if (i == 1 && j == 1L) {
                    readDLSN = dlsn;
                }
            }
            writer.closeAndComplete();
        }

        BKAsyncLogReader reader0 = (BKAsyncLogReader) Utils.ioResult(dlm.getAsyncLogReaderWithLock(subscriberId));
        assertEquals(DLSN.NonInclusiveLowerBound, reader0.getStartDLSN());
        long numTxns = 0;
        LogRecordWithDLSN record = Utils.ioResult(reader0.readNext());
        while (null != record) {
            DLMTestUtil.verifyEmptyLogRecord(record);
            ++numTxns;
            assertEquals(numTxns, record.getTransactionId());
            assertEquals(record.getTransactionId() - 1, record.getSequenceId());

            if (txid - 1 == numTxns) {
                break;
            }
            record = Utils.ioResult(reader0.readNext());
        }
        assertEquals(txid - 1, numTxns);
        Utils.close(reader0);

        SubscriptionsStore subscriptionsStore = dlm.getSubscriptionsStore();
        Utils.ioResult(subscriptionsStore.advanceCommitPosition(subscriberId, readDLSN));
        BKAsyncLogReader reader1 = (BKAsyncLogReader) Utils.ioResult(dlm.getAsyncLogReaderWithLock(subscriberId));
        assertEquals(readDLSN, reader1.getStartDLSN());
        numTxns = 0;
        long startTxID =  10L;
        record = Utils.ioResult(reader1.readNext());
        while (null != record) {
            DLMTestUtil.verifyEmptyLogRecord(record);
            ++numTxns;
            ++startTxID;
            assertEquals(startTxID, record.getTransactionId());
            assertEquals(record.getTransactionId() - 1L, record.getSequenceId());

            if (startTxID == txid - 1) {
                break;
            }
            record = Utils.ioResult(reader1.readNext());
        }
        assertEquals(txid - 1, startTxID);
        assertEquals(20, numTxns);
        Utils.close(reader1);

        dlm.close();
    }
}
