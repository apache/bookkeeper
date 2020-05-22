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
package org.apache.distributedlog.impl.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.distributedlog.DistributedLogConstants.EMPTY_BYTES;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.checkLogMetadataPaths;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.getLog;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.getLogSegments;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.getMissingPaths;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.intToBytes;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.pathExists;
import static org.apache.distributedlog.metadata.LogMetadata.ALLOCATION_PATH;
import static org.apache.distributedlog.metadata.LogMetadata.LAYOUT_VERSION;
import static org.apache.distributedlog.metadata.LogMetadata.LOCK_PATH;
import static org.apache.distributedlog.metadata.LogMetadata.LOGSEGMENTS_PATH;
import static org.apache.distributedlog.metadata.LogMetadata.MAX_TXID_PATH;
import static org.apache.distributedlog.metadata.LogMetadata.READ_LOCK_PATH;
import static org.apache.distributedlog.metadata.LogMetadata.VERSION_PATH;
import static org.apache.distributedlog.metadata.LogMetadata.getLogRootPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.LockingException;
import org.apache.distributedlog.exceptions.LogExistsException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link ZKLogStreamMetadataStore}.
 */
public class TestZKLogStreamMetadataStore extends ZooKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestZKLogStreamMetadataStore.class);

    private static final  int sessionTimeoutMs = 30000;

    @Rule
    public TestName testName = new TestName();

    private ZooKeeperClient zkc;
    private URI uri;
    private OrderedScheduler scheduler;
    private ZKLogStreamMetadataStore metadataStore;

    private static void createLog(ZooKeeperClient zk, URI uri, String logName, String logIdentifier)
            throws Exception {
        final String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        final String logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        final String maxTxIdPath = logRootPath + MAX_TXID_PATH;
        final String lockPath = logRootPath + LOCK_PATH;
        final String readLockPath = logRootPath + READ_LOCK_PATH;
        final String versionPath = logRootPath + VERSION_PATH;
        final String allocationPath = logRootPath + ALLOCATION_PATH;

        Utils.zkCreateFullPathOptimistic(zk, logRootPath, new byte[0],
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        Transaction txn = zk.get().transaction();
        txn.create(logSegmentsPath, DLUtils.serializeLogSegmentSequenceNumber(
                        DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(maxTxIdPath, DLUtils.serializeTransactionId(0L),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(lockPath, EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(readLockPath, EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(versionPath, intToBytes(LAYOUT_VERSION),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(allocationPath, EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.commit();
    }

    private static void createLog(ZooKeeperClient zk,
                                  URI uri,
                                  String logName,
                                  String logIdentifier,
                                  int numSegments)
            throws Exception {
        final String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        final String logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        final String maxTxIdPath = logRootPath + MAX_TXID_PATH;
        final String lockPath = logRootPath + LOCK_PATH;
        final String readLockPath = logRootPath + READ_LOCK_PATH;
        final String versionPath = logRootPath + VERSION_PATH;
        final String allocationPath = logRootPath + ALLOCATION_PATH;

        Utils.zkCreateFullPathOptimistic(zk, logRootPath, new byte[0],
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        Transaction txn = zk.get().transaction();
        txn.create(logSegmentsPath, DLUtils.serializeLogSegmentSequenceNumber(
                        DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(maxTxIdPath, DLUtils.serializeTransactionId(0L),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(lockPath, EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(readLockPath, EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(versionPath, intToBytes(LAYOUT_VERSION),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(allocationPath, EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);

        for (int i = 0; i < numSegments; i++) {
            LogSegmentMetadata segment = DLMTestUtil.completedLogSegment(
                logSegmentsPath,
                i + 1L,
                1L + i * 1000L,
                (i + 1) * 1000L,
                1000,
                i + 1L,
                999L,
                0L);
            txn.create(
                segment.getZkPath(),
                segment.getFinalisedData().getBytes(UTF_8),
                zk.getDefaultACL(),
                CreateMode.PERSISTENT);
        }

        txn.commit();
    }

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder.newBuilder()
                .name("zkc")
                .uri(DLMTestUtil.createDLMURI(zkPort, "/"))
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
        uri = DLMTestUtil.createDLMURI(zkPort, "");
        try {
            ZkUtils.createFullPathOptimistic(
                    zkc.get(),
                    uri.getPath(),
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            logger.debug("The namespace uri already exists.");
        }
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();
        metadataStore = new ZKLogStreamMetadataStore(
            "test-logstream-metadata-store",
            new DistributedLogConfiguration(),
            zkc,
            scheduler,
            NullStatsLogger.INSTANCE);
    }

    @After
    public void teardown() throws Exception {
        if (null != metadataStore) {
            metadataStore.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
        zkc.close();
    }

    @Test(timeout = 60000)
    public void testCheckLogMetadataPathsWithAllocator() throws Exception {
        String logRootPath = "/" + testName.getMethodName();
        List<Versioned<byte[]>> metadatas =
                Utils.ioResult(checkLogMetadataPaths(
                        zkc.get(), logRootPath, true));
        assertEquals("Should have 8 paths",
                8, metadatas.size());
        for (Versioned<byte[]> path : metadatas.subList(2, metadatas.size())) {
            assertNull(path.getValue());
            assertNull(path.getVersion());
        }
    }

    @Test(timeout = 60000)
    public void testCheckLogMetadataPathsWithoutAllocator() throws Exception {
        String logRootPath = "/" + testName.getMethodName();
        List<Versioned<byte[]>> metadatas =
                Utils.ioResult(checkLogMetadataPaths(
                        zkc.get(), logRootPath, false));
        assertEquals("Should have 7 paths",
                7, metadatas.size());
        for (Versioned<byte[]> path : metadatas.subList(2, metadatas.size())) {
            assertNull(path.getValue());
            assertNull(path.getVersion());
        }
    }

    private void testCreateLogMetadataWithMissingPaths(URI uri,
                                                       String logName,
                                                       String logIdentifier,
                                                       List<String> pathsToDelete,
                                                       boolean ownAllocator,
                                                       boolean createLogFirst)
            throws Exception {
        if (createLogFirst) {
            createLog(zkc, uri, logName, logIdentifier);
        }
        // delete a path
        for (String path : pathsToDelete) {
            zkc.get().delete(path, -1);
        }

        LogMetadataForWriter logMetadata =
                Utils.ioResult(getLog(uri, logName, logIdentifier, zkc, ownAllocator, true));

        final String logRootPath = getLogRootPath(uri, logName, logIdentifier);

        List<Versioned<byte[]>> metadatas =
                Utils.ioResult(checkLogMetadataPaths(zkc.get(), logRootPath, ownAllocator));

        if (ownAllocator) {
            assertEquals("Should have 8 paths : ownAllocator = " + ownAllocator,
                    8, metadatas.size());
        } else {
            assertEquals("Should have 7 paths : ownAllocator = " + ownAllocator,
                    7, metadatas.size());
        }

        for (Versioned<byte[]> metadata : metadatas) {
            assertTrue(pathExists(metadata));
            assertTrue(((LongVersion) metadata.getVersion()).getLongVersion() >= 0L);
        }

        Versioned<byte[]> logSegmentsData = logMetadata.getMaxLSSNData();

        assertEquals(DistributedLogConstants.UNASSIGNED_LOGSEGMENT_SEQNO,
                DLUtils.deserializeLogSegmentSequenceNumber(logSegmentsData.getValue()));

        Versioned<byte[]> maxTxIdData = logMetadata.getMaxTxIdData();

        assertEquals(0L, DLUtils.deserializeTransactionId(maxTxIdData.getValue()));

        if (ownAllocator) {
            Versioned<byte[]> allocationData = logMetadata.getAllocationData();
            assertEquals(0, allocationData.getValue().length);
        }
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingLogSegmentsPath() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + LOGSEGMENTS_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingMaxTxIdPath() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + MAX_TXID_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingLockPath() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + LOCK_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingReadLockPath() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + READ_LOCK_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingVersionPath() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + VERSION_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, false, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingAllocatorPath() throws Exception {
        URI uri = DLMTestUtil.createDLMURI(zkPort, "");
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + ALLOCATION_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataMissingAllPath() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        List<String> pathsToDelete = Lists.newArrayList(
                logRootPath + LOGSEGMENTS_PATH,
                logRootPath + MAX_TXID_PATH,
                logRootPath + LOCK_PATH,
                logRootPath + READ_LOCK_PATH,
                logRootPath + VERSION_PATH,
                logRootPath + ALLOCATION_PATH);
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadataOnExistedLog() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        List<String> pathsToDelete = Lists.newArrayList();
        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, true);
    }

    @Test(timeout = 60000)
    public void testCreateLogMetadata() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        List<String> pathsToDelete = Lists.newArrayList();

        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, false);
    }

    @Test(timeout = 60000, expected = LogNotFoundException.class)
    public void testCreateLogMetadataWithCreateIfNotExistsSetToFalse() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        Utils.ioResult(getLog(uri, logName, logIdentifier, zkc, true, false));
    }

    @SuppressWarnings("deprecation")
    @Test(timeout = 60000)
    public void testCreateLogMetadataWithCustomMetadata() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        List<String> pathsToDelete = Lists.newArrayList();

        DLMetadata.create(new BKDLConfig(zkServers, "/ledgers")).update(uri);

        Namespace namespace = NamespaceBuilder.newBuilder()
            .conf(new DistributedLogConfiguration())
            .uri(uri)
            .build();

        org.apache.distributedlog.api.MetadataAccessor accessor =
            namespace.getNamespaceDriver().getMetadataAccessor(logName);
        accessor.createOrUpdateMetadata(logName.getBytes("UTF-8"));
        accessor.close();

        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, false);
    }

    @Test(timeout = 60000, expected = LogNotFoundException.class)
    public void testGetLogSegmentsLogNotFound() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";

        String logSegmentsPath = LogMetadata.getLogSegmentsPath(uri, logName, logIdentifier);
        FutureUtils.result(getLogSegments(zkc, logSegmentsPath));
    }

    @Test(timeout = 60000)
    public void testGetLogSegmentsZKExceptions() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";

        ZooKeeper mockZk = mock(ZooKeeper.class);
        ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
        when(mockZkc.get()).thenReturn(mockZk);
        doAnswer(invocationOnMock -> {
            String path = (String) invocationOnMock.getArguments()[0];
            Children2Callback callback = (Children2Callback) invocationOnMock.getArguments()[2];
            callback.processResult(Code.BADVERSION.intValue(), path, null, null, null);
            return null;
        }).when(mockZk).getChildren(anyString(), anyBoolean(), any(Children2Callback.class), any());

        String logSegmentsPath = LogMetadata.getLogSegmentsPath(uri, logName, logIdentifier);
        try {
            FutureUtils.result(getLogSegments(mockZkc, logSegmentsPath));
            fail("Should fail to get log segments when encountering zk exceptions");
        } catch (ZKException zke) {
            assertEquals(Code.BADVERSION, zke.getKeeperExceptionCode());
        }
    }

    @Test(timeout = 60000)
    public void testGetLogSegments() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";

        // create log
        createLog(
            zkc,
            uri,
            logName,
            logIdentifier,
            5);

        List<LogSegmentMetadata> segments = FutureUtils.result(
            getLogSegments(zkc, LogMetadata.getLogSegmentsPath(uri, logName, logIdentifier)));
        assertEquals(5, segments.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(1L + i, segments.get(i).getLogSegmentSequenceNumber());
        }
    }

    @Test(timeout = 60000)
    public void testGetMissingPathsRecursive() throws Exception {
        List<String> missingPaths = FutureUtils.result(
            getMissingPaths(zkc, uri, "path_missing/to/log"));

        assertEquals(
            Lists.newArrayList(
                uri.getPath() + "/path_missing/to/log",
                uri.getPath() + "/path_missing/to",
                uri.getPath() + "/path_missing"
            ),
            missingPaths);
    }

    @Test(timeout = 60000)
    public void testGetMissingPathsRecursive2() throws Exception {
        String path = uri.getPath() + "/path_missing2/to/log";
        ZkUtils.createFullPathOptimistic(
            zkc.get(), path, EMPTY_BYTES, zkc.getDefaultACL(), CreateMode.PERSISTENT);

        List<String> missingPaths = FutureUtils.result(
            getMissingPaths(zkc, uri, "path_missing2/to/log"));

        assertEquals(
            Collections.emptyList(),
            missingPaths);
    }

    @Test(timeout = 60000)
    public void testGetMissingPathsFailure() throws Exception {
        ZooKeeper mockZk = mock(ZooKeeper.class);
        ZooKeeperClient mockZkc = mock(ZooKeeperClient.class);
        when(mockZkc.get()).thenReturn(mockZk);
        doAnswer(invocationOnMock -> {
            String path = (String) invocationOnMock.getArguments()[0];
            StatCallback callback = (StatCallback) invocationOnMock.getArguments()[2];
            callback.processResult(Code.BADVERSION.intValue(), path, null, null);
            return null;
        }).when(mockZk).exists(anyString(), anyBoolean(), any(StatCallback.class), any());

        try {
            FutureUtils.result(getMissingPaths(mockZkc, uri, "path_failure/to/log_failure"));
            fail("Should fail on getting missing paths on zookeeper exceptions.");
        } catch (ZKException zke) {
            assertEquals(Code.BADVERSION, zke.getKeeperExceptionCode());
        }
    }

    @Test(timeout = 60000)
    public void testRenameLog() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        int numSegments = 5;

        createLog(
            zkc,
            uri,
            logName,
            logIdentifier,
            numSegments);

        String newLogName = "path_rename/to/new/" + logName;
        FutureUtils.result(metadataStore.renameLog(uri, logName, newLogName));
    }

    @Test(timeout = 60000, expected = LogExistsException.class)
    public void testRenameLogExists() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        int numSegments = 5;
        createLog(
            zkc,
            uri,
            logName,
            logIdentifier,
            numSegments);

        String newLogName = "path_rename_exists/to/new/" + logName;
        createLog(
            zkc,
            uri,
            newLogName,
            logIdentifier,
            3);

        FutureUtils.result(metadataStore.renameLog(uri, logName, newLogName));
    }

    @Test(timeout = 60000, expected = LockingException.class)
    public void testRenameLockedLog() throws Exception {
        String logName = testName.getMethodName();
        String logIdentifier = "<default>";
        int numSegments = 5;
        createLog(
            zkc,
            uri,
            logName,
            logIdentifier,
            numSegments);

        // create a lock
        String logRootPath = getLogRootPath(uri, logName, logIdentifier);
        String lockPath = logRootPath + LOCK_PATH;
        zkc.get().create(lockPath + "/test", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        String newLogName = "path_rename_locked/to/new/" + logName;
        FutureUtils.result(metadataStore.renameLog(uri, logName, newLogName));
    }

}
