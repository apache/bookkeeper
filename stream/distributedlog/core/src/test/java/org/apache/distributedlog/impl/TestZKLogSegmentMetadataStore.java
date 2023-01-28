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
package org.apache.distributedlog.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClientUtils;
import org.apache.distributedlog.callback.LogSegmentNamesListener;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test ZK based log segment metadata store.
 */
public class TestZKLogSegmentMetadataStore extends TestDistributedLogBase {

    private static final Logger logger = LoggerFactory.getLogger(TestZKLogSegmentMetadataStore.class);

    private static final  int zkSessionTimeoutMs = 2000;

    private LogSegmentMetadata createLogSegment(
            long logSegmentSequenceNumber) {
        return createLogSegment(logSegmentSequenceNumber, 99L);
    }

    private LogSegmentMetadata createLogSegment(
        long logSegmentSequenceNumber,
        long lastEntryId) {
        return DLMTestUtil.completedLogSegment(
                "/" + runtime.getMethodName(),
                logSegmentSequenceNumber,
                logSegmentSequenceNumber,
                1L,
                100,
                logSegmentSequenceNumber,
                lastEntryId,
                0L,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    @Rule
    public TestName runtime = new TestName();
    protected final DistributedLogConfiguration baseConf =
            new DistributedLogConfiguration();
    protected ZooKeeperClient zkc;
    protected ZKLogSegmentMetadataStore lsmStore;
    protected OrderedScheduler scheduler;
    protected URI uri;
    protected String rootZkPath;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .uri(createDLMURI("/"))
                .sessionTimeoutMs(zkSessionTimeoutMs)
                .build();
        scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-zk-logsegment-metadata-store")
                .numThreads(1)
                .build();
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        conf.addConfiguration(baseConf);
        this.uri = createDLMURI("/" + runtime.getMethodName());
        lsmStore = new ZKLogSegmentMetadataStore(conf, zkc, scheduler);
        zkc.get().create(
                "/" + runtime.getMethodName(),
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        this.rootZkPath = "/" + runtime.getMethodName();
    }

    @After
    public void teardown() throws Exception {
        if (null != zkc) {
            zkc.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    @Test(timeout = 60000)
    public void testCreateLogSegment() throws Exception {
        LogSegmentMetadata segment = createLogSegment(1L);
        Transaction<Object> createTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn, segment, null);
        Utils.ioResult(createTxn.execute());
        // the log segment should be created
        assertNotNull("LogSegment " + segment + " should be created",
                zkc.get().exists(segment.getZkPath(), false));
        LogSegmentMetadata segment2 = createLogSegment(1L);
        Transaction<Object> createTxn2 = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn2, segment2, null);
        try {
            Utils.ioResult(createTxn2.execute());
            fail("Should fail if log segment exists");
        } catch (Throwable t) {
            // expected
            assertTrue("Should throw NodeExistsException if log segment exists",
                    t instanceof ZKException);
            ZKException zke = (ZKException) t;
            assertEquals("Should throw NodeExistsException if log segment exists",
                    KeeperException.Code.NODEEXISTS, zke.getKeeperExceptionCode());
        }
    }

    @Test(timeout = 60000)
    public void testDeleteLogSegment() throws Exception {
        LogSegmentMetadata segment = createLogSegment(1L);
        Transaction<Object> createTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn, segment, null);
        Utils.ioResult(createTxn.execute());
        // the log segment should be created
        assertNotNull("LogSegment " + segment + " should be created",
                zkc.get().exists(segment.getZkPath(), false));
        Transaction<Object> deleteTxn = lsmStore.transaction();
        lsmStore.deleteLogSegment(deleteTxn, segment, null);
        Utils.ioResult(deleteTxn.execute());
        assertNull("LogSegment " + segment + " should be deleted",
                zkc.get().exists(segment.getZkPath(), false));
    }

    @Test(timeout = 60000)
    public void testDeleteNonExistentLogSegment() throws Exception {
        LogSegmentMetadata segment = createLogSegment(1L);
        Transaction<Object> deleteTxn = lsmStore.transaction();
        lsmStore.deleteLogSegment(deleteTxn, segment, null);
        try {
            Utils.ioResult(deleteTxn.execute());
            fail("Should fail deletion if log segment doesn't exist");
        } catch (Throwable t) {
            assertTrue("Should throw NoNodeException if log segment doesn't exist",
                    t instanceof ZKException);
            ZKException zke = (ZKException) t;
            assertEquals("Should throw NoNodeException if log segment doesn't exist",
                    KeeperException.Code.NONODE, zke.getKeeperExceptionCode());
        }
    }

    @Test(timeout = 60000)
    public void testUpdateNonExistentLogSegment() throws Exception {
        LogSegmentMetadata segment = createLogSegment(1L);
        Transaction<Object> updateTxn = lsmStore.transaction();
        lsmStore.updateLogSegment(updateTxn, segment);
        try {
            Utils.ioResult(updateTxn.execute());
            fail("Should fail update if log segment doesn't exist");
        } catch (Throwable t) {
            assertTrue("Should throw NoNodeException if log segment doesn't exist",
                    t instanceof ZKException);
            ZKException zke = (ZKException) t;
            assertEquals("Should throw NoNodeException if log segment doesn't exist",
                    KeeperException.Code.NONODE, zke.getKeeperExceptionCode());
        }
    }

    @Test(timeout = 60000)
    public void testUpdateLogSegment() throws Exception {
        LogSegmentMetadata segment = createLogSegment(1L, 99L);
        Transaction<Object> createTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn, segment, null);
        Utils.ioResult(createTxn.execute());
        // the log segment should be created
        assertNotNull("LogSegment " + segment + " should be created",
                zkc.get().exists(segment.getZkPath(), false));
        LogSegmentMetadata modifiedSegment = createLogSegment(1L, 999L);
        Transaction<Object> updateTxn = lsmStore.transaction();
        lsmStore.updateLogSegment(updateTxn, modifiedSegment);
        Utils.ioResult(updateTxn.execute());
        // the log segment should be updated
        LogSegmentMetadata readSegment =
                Utils.ioResult(LogSegmentMetadata.read(zkc, segment.getZkPath(), true));
        assertEquals("Last entry id should be changed from 99L to 999L",
                999L, readSegment.getLastEntryId());
    }

    @Test(timeout = 60000)
    public void testCreateDeleteLogSegmentSuccess() throws Exception {
        LogSegmentMetadata segment1 = createLogSegment(1L);
        LogSegmentMetadata segment2 = createLogSegment(2L);
        // create log segment 1
        Transaction<Object> createTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn, segment1, null);
        Utils.ioResult(createTxn.execute());
        // the log segment should be created
        assertNotNull("LogSegment " + segment1 + " should be created",
                zkc.get().exists(segment1.getZkPath(), false));
        // delete log segment 1 and create log segment 2
        Transaction<Object> createDeleteTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createDeleteTxn, segment2, null);
        lsmStore.deleteLogSegment(createDeleteTxn, segment1, null);
        Utils.ioResult(createDeleteTxn.execute());
        // segment 1 should be deleted, segment 2 should be created
        assertNull("LogSegment " + segment1 + " should be deleted",
                zkc.get().exists(segment1.getZkPath(), false));
        assertNotNull("LogSegment " + segment2 + " should be created",
                zkc.get().exists(segment2.getZkPath(), false));
    }

    @Test(timeout = 60000)
    public void testCreateDeleteLogSegmentFailure() throws Exception {
        LogSegmentMetadata segment1 = createLogSegment(1L);
        LogSegmentMetadata segment2 = createLogSegment(2L);
        LogSegmentMetadata segment3 = createLogSegment(3L);
        // create log segment 1
        Transaction<Object> createTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn, segment1, null);
        Utils.ioResult(createTxn.execute());
        // the log segment should be created
        assertNotNull("LogSegment " + segment1 + " should be created",
                zkc.get().exists(segment1.getZkPath(), false));
        // delete log segment 1 and delete log segment 2
        Transaction<Object> createDeleteTxn = lsmStore.transaction();
        lsmStore.deleteLogSegment(createDeleteTxn, segment1, null);
        lsmStore.deleteLogSegment(createDeleteTxn, segment2, null);
        lsmStore.createLogSegment(createDeleteTxn, segment3, null);
        try {
            Utils.ioResult(createDeleteTxn.execute());
            fail("Should fail transaction if one operation failed");
        } catch (Throwable t) {
            assertTrue("Transaction is aborted",
                    t instanceof ZKException);
            ZKException zke = (ZKException) t;
            assertEquals("Transaction is aborted",
                    KeeperException.Code.NONODE, zke.getKeeperExceptionCode());
        }
        // segment 1 should not be deleted
        assertNotNull("LogSegment " + segment1 + " should not be deleted",
                zkc.get().exists(segment1.getZkPath(), false));
        // segment 3 should not be created
        assertNull("LogSegment " + segment3 + " should be created",
                zkc.get().exists(segment3.getZkPath(), false));
    }

    @Test(timeout = 60000)
    public void testGetLogSegment() throws Exception {
        LogSegmentMetadata segment = createLogSegment(1L, 99L);
        Transaction<Object> createTxn = lsmStore.transaction();
        lsmStore.createLogSegment(createTxn, segment, null);
        Utils.ioResult(createTxn.execute());
        // the log segment should be created
        assertNotNull("LogSegment " + segment + " should be created",
                zkc.get().exists(segment.getZkPath(), false));
        LogSegmentMetadata readSegment =
                Utils.ioResult(lsmStore.getLogSegment(segment.getZkPath()));
        assertEquals("Log segment should match",
                segment, readSegment);
    }

    @Test(timeout = 60000)
    public void testGetLogSegmentNames() throws Exception {
        Transaction<Object> createTxn = lsmStore.transaction();
        List<LogSegmentMetadata> createdSegments = Lists.newArrayListWithExpectedSize(10);
        for (int i = 0; i < 10; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            createdSegments.add(segment);
            lsmStore.createLogSegment(createTxn, segment, null);
        }
        Utils.ioResult(createTxn.execute());
        String rootPath = "/" + runtime.getMethodName();
        List<String> children = zkc.get().getChildren(rootPath, false);
        Collections.sort(children);
        assertEquals("Should find 10 log segments",
                10, children.size());
        List<String> logSegmentNames =
                Utils.ioResult(lsmStore.getLogSegmentNames(rootPath, null)).getValue();
        Collections.sort(logSegmentNames);
        assertEquals("Should find 10 log segments",
                10, logSegmentNames.size());
        assertEquals(children, logSegmentNames);
        List<CompletableFuture<LogSegmentMetadata>> getFutures = Lists.newArrayListWithExpectedSize(10);
        for (int i = 0; i < 10; i++) {
            getFutures.add(lsmStore.getLogSegment(rootPath + "/" + logSegmentNames.get(i)));
        }
        List<LogSegmentMetadata> segments =
                Utils.ioResult(FutureUtils.collect(getFutures));
        for (int i = 0; i < 10; i++) {
            assertEquals(createdSegments.get(i), segments.get(i));
        }
    }

    @Test(timeout = 60000)
    public void testRegisterListenerAfterLSMStoreClosed() throws Exception {
        lsmStore.close();
        LogSegmentMetadata segment = createLogSegment(1L);
        lsmStore.getLogSegmentNames(segment.getZkPath(), new LogSegmentNamesListener() {
            @Override
            public void onSegmentsUpdated(Versioned<List<String>> segments) {
                // no-op;
            }
            @Override
            public void onLogStreamDeleted() {
                // no-op;
            }
        });
        assertTrue("No listener is registered",
                lsmStore.listeners.isEmpty());
    }

    @Test(timeout = 60000)
    public void testLogSegmentNamesListener() throws Exception {
        int numSegments = 3;
        Transaction<Object> createTxn = lsmStore.transaction();
        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.createLogSegment(createTxn, segment, null);
        }
        Utils.ioResult(createTxn.execute());
        String rootPath = "/" + runtime.getMethodName();
        List<String> children = zkc.get().getChildren(rootPath, false);
        Collections.sort(children);

        final AtomicInteger numNotifications = new AtomicInteger(0);
        final List<List<String>> segmentLists = Lists.newArrayListWithExpectedSize(2);
        LogSegmentNamesListener listener = new LogSegmentNamesListener() {
            @Override
            public void onSegmentsUpdated(Versioned<List<String>> segments) {
                logger.info("Received segments : {}", segments);
                segmentLists.add(segments.getValue());
                numNotifications.incrementAndGet();
            }
            @Override
            public void onLogStreamDeleted() {
                // no-op;
            }
        };
        lsmStore.getLogSegmentNames(rootPath, listener);
        assertEquals(1, lsmStore.listeners.size());
        assertTrue("Should contain listener", lsmStore.listeners.containsKey(rootPath));
        assertTrue("Should contain listener", lsmStore.listeners.get(rootPath).containsKey(listener));
        while (numNotifications.get() < 1) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive one segment list update",
                1, numNotifications.get());
        List<String> firstSegmentList = segmentLists.get(0);
        Collections.sort(firstSegmentList);
        assertEquals("List of segments should be same",
                children, firstSegmentList);

        logger.info("Create another {} segments.", numSegments);

        // create another log segment, it should trigger segment list updated
        Transaction<Object> anotherCreateTxn = lsmStore.transaction();
        for (int i = numSegments; i < 2 * numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.createLogSegment(anotherCreateTxn, segment, null);
        }
        Utils.ioResult(anotherCreateTxn.execute());
        List<String> newChildren = zkc.get().getChildren(rootPath, false);
        Collections.sort(newChildren);
        logger.info("All log segments become {}", newChildren);
        while (numNotifications.get() < 2) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive second segment list update",
                2, numNotifications.get());
        List<String> secondSegmentList = segmentLists.get(1);
        Collections.sort(secondSegmentList);
        assertEquals("List of segments should be updated",
                2 * numSegments, secondSegmentList.size());
        assertEquals("List of segments should be updated",
                newChildren, secondSegmentList);
    }

    @Test(timeout = 60000)
    public void testLogSegmentNamesListenerOnDeletion() throws Exception {
        int numSegments = 3;
        Transaction<Object> createTxn = lsmStore.transaction();
        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.createLogSegment(createTxn, segment, null);
        }
        Utils.ioResult(createTxn.execute());
        String rootPath = "/" + runtime.getMethodName();
        List<String> children = zkc.get().getChildren(rootPath, false);
        Collections.sort(children);

        final AtomicInteger numNotifications = new AtomicInteger(0);
        final List<List<String>> segmentLists = Lists.newArrayListWithExpectedSize(2);
        LogSegmentNamesListener listener = new LogSegmentNamesListener() {
            @Override
            public void onSegmentsUpdated(Versioned<List<String>> segments) {
                logger.info("Received segments : {}", segments);
                segmentLists.add(segments.getValue());
                numNotifications.incrementAndGet();
            }

            @Override
            public void onLogStreamDeleted() {
                // no-op;
            }
        };
        lsmStore.getLogSegmentNames(rootPath, listener);
        assertEquals(1, lsmStore.listeners.size());
        assertTrue("Should contain listener", lsmStore.listeners.containsKey(rootPath));
        assertTrue("Should contain listener", lsmStore.listeners.get(rootPath).containsKey(listener));
        while (numNotifications.get() < 1) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive one segment list update",
                1, numNotifications.get());
        List<String> firstSegmentList = segmentLists.get(0);
        Collections.sort(firstSegmentList);
        assertEquals("List of segments should be same",
                children, firstSegmentList);

        // delete all log segments, it should trigger segment list updated
        Transaction<Object> deleteTxn = lsmStore.transaction();
        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.deleteLogSegment(deleteTxn, segment, null);
        }
        Utils.ioResult(deleteTxn.execute());
        List<String> newChildren = zkc.get().getChildren(rootPath, false);
        Collections.sort(newChildren);
        while (numNotifications.get() < 2) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive second segment list update",
                2, numNotifications.get());
        List<String> secondSegmentList = segmentLists.get(1);
        Collections.sort(secondSegmentList);
        assertEquals("List of segments should be updated",
                0, secondSegmentList.size());
        assertEquals("List of segments should be updated",
                newChildren, secondSegmentList);

        // delete the root path
        zkc.get().delete(rootPath, -1);
        while (!lsmStore.listeners.isEmpty()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertTrue("listener should be removed after root path is deleted",
                lsmStore.listeners.isEmpty());
    }

    @Test(timeout = 60000)
    public void testLogSegmentNamesListenerOnSessionExpired() throws Exception {
        int numSegments = 3;
        Transaction<Object> createTxn = lsmStore.transaction();
        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.createLogSegment(createTxn, segment, null);
        }
        Utils.ioResult(createTxn.execute());
        String rootPath = "/" + runtime.getMethodName();
        List<String> children = zkc.get().getChildren(rootPath, false);
        Collections.sort(children);

        final AtomicInteger numNotifications = new AtomicInteger(0);
        final List<List<String>> segmentLists = Lists.newArrayListWithExpectedSize(2);
        LogSegmentNamesListener listener = new LogSegmentNamesListener() {
            @Override
            public void onSegmentsUpdated(Versioned<List<String>> segments) {
                logger.info("Received segments : {}", segments);
                segmentLists.add(segments.getValue());
                numNotifications.incrementAndGet();
            }

            @Override
            public void onLogStreamDeleted() {
                // no-op;
            }
        };
        lsmStore.getLogSegmentNames(rootPath, listener);
        assertEquals(1, lsmStore.listeners.size());
        assertTrue("Should contain listener", lsmStore.listeners.containsKey(rootPath));
        assertTrue("Should contain listener", lsmStore.listeners.get(rootPath).containsKey(listener));
        while (numNotifications.get() < 1) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive one segment list update",
                1, numNotifications.get());
        List<String> firstSegmentList = segmentLists.get(0);
        Collections.sort(firstSegmentList);
        assertEquals("List of segments should be same",
                children, firstSegmentList);

        ZooKeeperClientUtils.expireSession(zkc,
                BKNamespaceDriver.getZKServersFromDLUri(uri), conf.getZKSessionTimeoutMilliseconds());

        logger.info("Create another {} segments.", numSegments);

        // create another log segment, it should trigger segment list updated
        Transaction<Object> anotherCreateTxn = lsmStore.transaction();
        for (int i = numSegments; i < 2 * numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.createLogSegment(anotherCreateTxn, segment, null);
        }
        Utils.ioResult(anotherCreateTxn.execute());
        List<String> newChildren = zkc.get().getChildren(rootPath, false);
        Collections.sort(newChildren);
        logger.info("All log segments become {}", newChildren);
        while (numNotifications.get() < 2) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive third segment list update",
                2, numNotifications.get());
        List<String> thirdSegmentList = segmentLists.get(1);
        Collections.sort(thirdSegmentList);
        assertEquals("List of segments should be updated",
                2 * numSegments, thirdSegmentList.size());
        assertEquals("List of segments should be updated",
                newChildren, thirdSegmentList);
    }

    @Test(timeout = 60000)
    public void testLogSegmentNamesListenerOnDeletingLogStream() throws Exception {
        int numSegments = 3;
        Transaction<Object> createTxn = lsmStore.transaction();
        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.createLogSegment(createTxn, segment, null);
        }
        Utils.ioResult(createTxn.execute());
        String rootPath = "/" + runtime.getMethodName();
        List<String> children = zkc.get().getChildren(rootPath, false);
        Collections.sort(children);

        final AtomicInteger numNotifications = new AtomicInteger(0);
        final List<List<String>> segmentLists = Lists.newArrayListWithExpectedSize(2);
        final CountDownLatch deleteLatch = new CountDownLatch(1);
        LogSegmentNamesListener listener = new LogSegmentNamesListener() {
            @Override
            public void onSegmentsUpdated(Versioned<List<String>> segments) {
                logger.info("Received segments : {}", segments);
                segmentLists.add(segments.getValue());
                numNotifications.incrementAndGet();
            }

            @Override
            public void onLogStreamDeleted() {
                deleteLatch.countDown();
            }
        };
        lsmStore.getLogSegmentNames(rootPath, listener);
        assertEquals(1, lsmStore.listeners.size());
        assertTrue("Should contain listener", lsmStore.listeners.containsKey(rootPath));
        assertTrue("Should contain listener", lsmStore.listeners.get(rootPath).containsKey(listener));
        while (numNotifications.get() < 1) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive one segment list update",
                1, numNotifications.get());
        List<String> firstSegmentList = segmentLists.get(0);
        Collections.sort(firstSegmentList);
        assertEquals("List of segments should be same",
                children, firstSegmentList);

        // delete all log segments, it should trigger segment list updated
        Transaction<Object> deleteTxn = lsmStore.transaction();
        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = createLogSegment(i);
            lsmStore.deleteLogSegment(deleteTxn, segment, null);
        }
        Utils.ioResult(deleteTxn.execute());
        List<String> newChildren = zkc.get().getChildren(rootPath, false);
        Collections.sort(newChildren);
        while (numNotifications.get() < 2) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals("Should receive second segment list update",
                2, numNotifications.get());
        List<String> secondSegmentList = segmentLists.get(1);
        Collections.sort(secondSegmentList);
        assertEquals("List of segments should be updated",
                0, secondSegmentList.size());
        assertEquals("List of segments should be updated",
                newChildren, secondSegmentList);

        // delete the root path
        zkc.get().delete(rootPath, -1);
        while (!lsmStore.listeners.isEmpty()) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertTrue("listener should be removed after root path is deleted",
                lsmStore.listeners.isEmpty());
        deleteLatch.await();
    }

    @Test(timeout = 60000)
    public void testStoreMaxLogSegmentSequenceNumber() throws Exception {
        Transaction<Object> updateTxn = lsmStore.transaction();
        Versioned<Long> value = new Versioned<Long>(999L, new LongVersion(0));
        final CompletableFuture<Version> result = new CompletableFuture<Version>();
        LogMetadata metadata = mock(LogMetadata.class);
        when(metadata.getLogSegmentsPath()).thenReturn(rootZkPath);
        lsmStore.storeMaxLogSegmentSequenceNumber(updateTxn, metadata, value,
                new Transaction.OpListener<Version>() {
            @Override
            public void onCommit(Version r) {
                result.complete(r);
            }

            @Override
            public void onAbort(Throwable t) {
                result.completeExceptionally(t);
            }
        });
        Utils.ioResult(updateTxn.execute());
        assertEquals(1L, ((LongVersion) Utils.ioResult(result)).getLongVersion());
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(rootZkPath, false, stat);
        assertEquals(999L, DLUtils.deserializeLogSegmentSequenceNumber(data));
        assertEquals(1, stat.getVersion());
    }

    @Test(timeout = 60000)
    public void testStoreMaxLogSegmentSequenceNumberBadVersion() throws Exception {
        Transaction<Object> updateTxn = lsmStore.transaction();
        Versioned<Long> value = new Versioned<Long>(999L, new LongVersion(10));
        final CompletableFuture<Version> result = new CompletableFuture<Version>();
        LogMetadata metadata = mock(LogMetadata.class);
        when(metadata.getLogSegmentsPath()).thenReturn(rootZkPath);
        lsmStore.storeMaxLogSegmentSequenceNumber(updateTxn, metadata, value,
                new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version r) {
                        result.complete(r);
                    }

                    @Override
                    public void onAbort(Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
        try {
            Utils.ioResult(updateTxn.execute());
            fail("Should fail on storing log segment sequence number if providing bad version");
        } catch (ZKException zke) {
            assertEquals(KeeperException.Code.BADVERSION, zke.getKeeperExceptionCode());
        }
        try {
            Utils.ioResult(result);
            fail("Should fail on storing log segment sequence number if providing bad version");
        } catch (ZKException ze) {
            assertEquals(KeeperException.Code.BADVERSION, ze.getKeeperExceptionCode());
        }
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(rootZkPath, false, stat);
        assertEquals(0, stat.getVersion());
        assertEquals(0, data.length);
    }

    @Test(timeout = 60000)
    public void testStoreMaxLogSegmentSequenceNumberOnNonExistentPath() throws Exception {
        Transaction<Object> updateTxn = lsmStore.transaction();
        Versioned<Long> value = new Versioned<Long>(999L, new LongVersion(10));
        final CompletableFuture<Version> result = new CompletableFuture<Version>();
        String nonExistentPath = rootZkPath + "/non-existent";
        LogMetadata metadata = mock(LogMetadata.class);
        when(metadata.getLogSegmentsPath()).thenReturn(nonExistentPath);
        lsmStore.storeMaxLogSegmentSequenceNumber(updateTxn, metadata, value,
                new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version r) {
                        result.complete(r);
                    }

                    @Override
                    public void onAbort(Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
        try {
            Utils.ioResult(updateTxn.execute());
            fail("Should fail on storing log segment sequence number if path doesn't exist");
        } catch (ZKException zke) {
            assertEquals(KeeperException.Code.NONODE, zke.getKeeperExceptionCode());
        }
        try {
            Utils.ioResult(result);
            fail("Should fail on storing log segment sequence number if path doesn't exist");
        } catch (ZKException ke) {
            assertEquals(KeeperException.Code.NONODE, ke.getKeeperExceptionCode());
        }
    }

    @Test(timeout = 60000)
    public void testStoreMaxTxnId() throws Exception {
        Transaction<Object> updateTxn = lsmStore.transaction();
        Versioned<Long> value = new Versioned<Long>(999L, new LongVersion(0));
        final CompletableFuture<Version> result = new CompletableFuture<Version>();
        LogMetadataForWriter metadata = mock(LogMetadataForWriter.class);
        when(metadata.getMaxTxIdPath()).thenReturn(rootZkPath);
        lsmStore.storeMaxTxnId(updateTxn, metadata, value,
                new Transaction.OpListener<Version>() {
            @Override
            public void onCommit(Version r) {
                result.complete(r);
            }

            @Override
            public void onAbort(Throwable t) {
                result.completeExceptionally(t);
            }
        });
        Utils.ioResult(updateTxn.execute());
        assertEquals(1L, ((LongVersion) Utils.ioResult(result)).getLongVersion());
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(rootZkPath, false, stat);
        assertEquals(999L, DLUtils.deserializeTransactionId(data));
        assertEquals(1, stat.getVersion());
    }

    @Test(timeout = 60000)
    public void testStoreMaxTxnIdBadVersion() throws Exception {
        Transaction<Object> updateTxn = lsmStore.transaction();
        Versioned<Long> value = new Versioned<Long>(999L, new LongVersion(10));
        final CompletableFuture<Version> result = new CompletableFuture<Version>();
        LogMetadataForWriter metadata = mock(LogMetadataForWriter.class);
        when(metadata.getMaxTxIdPath()).thenReturn(rootZkPath);
        lsmStore.storeMaxTxnId(updateTxn, metadata, value,
                new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version r) {
                        result.complete(r);
                    }

                    @Override
                    public void onAbort(Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
        try {
            Utils.ioResult(updateTxn.execute());
            fail("Should fail on storing log record transaction id if providing bad version");
        } catch (ZKException zke) {
            assertEquals(KeeperException.Code.BADVERSION, zke.getKeeperExceptionCode());
        }
        try {
            Utils.ioResult(result);
            fail("Should fail on storing log record transaction id if providing bad version");
        } catch (ZKException ze) {
            assertEquals(KeeperException.Code.BADVERSION, ze.getKeeperExceptionCode());
        }
        Stat stat = new Stat();
        byte[] data = zkc.get().getData(rootZkPath, false, stat);
        assertEquals(0, stat.getVersion());
        assertEquals(0, data.length);
    }

    @Test(timeout = 60000)
    public void testStoreMaxTxnIdOnNonExistentPath() throws Exception {
        Transaction<Object> updateTxn = lsmStore.transaction();
        Versioned<Long> value = new Versioned<Long>(999L, new LongVersion(10));
        final CompletableFuture<Version> result = new CompletableFuture<Version>();
        String nonExistentPath = rootZkPath + "/non-existent";
        LogMetadataForWriter metadata = mock(LogMetadataForWriter.class);
        when(metadata.getMaxTxIdPath()).thenReturn(nonExistentPath);
        lsmStore.storeMaxTxnId(updateTxn, metadata, value,
                new Transaction.OpListener<Version>() {
                    @Override
                    public void onCommit(Version r) {
                        result.complete(r);
                    }

                    @Override
                    public void onAbort(Throwable t) {
                        result.completeExceptionally(t);
                    }
                });
        try {
            Utils.ioResult(updateTxn.execute());
            fail("Should fail on storing log record transaction id if path doesn't exist");
        } catch (ZKException zke) {
            assertEquals(KeeperException.Code.NONODE, zke.getKeeperExceptionCode());
        }
        try {
            Utils.ioResult(result);
            fail("Should fail on storing log record transaction id if path doesn't exist");
        } catch (ZKException ze) {
            assertEquals(KeeperException.Code.NONODE, ze.getKeeperExceptionCode());
        }
    }

}
