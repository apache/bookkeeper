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

import com.google.common.collect.Lists;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.MetadataAccessor;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.metadata.DLMetadata;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClusterTestCase;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.Utils;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

import static org.apache.distributedlog.metadata.LogMetadata.*;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.*;
import static org.junit.Assert.*;

/**
 * Test {@link ZKLogStreamMetadataStore}
 */
public class TestZKLogStreamMetadataStore extends ZooKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestZKLogStreamMetadataStore.class);

    private final static int sessionTimeoutMs = 30000;

    @Rule
    public TestName testName = new TestName();

    private ZooKeeperClient zkc;
    private URI uri;

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
        txn.create(lockPath, DistributedLogConstants.EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(readLockPath, DistributedLogConstants.EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(versionPath, intToBytes(LAYOUT_VERSION),
                zk.getDefaultACL(), CreateMode.PERSISTENT);
        txn.create(allocationPath, DistributedLogConstants.EMPTY_BYTES,
                zk.getDefaultACL(), CreateMode.PERSISTENT);
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
    }

    @After
    public void teardown() throws Exception {
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
            assertTrue(((ZkVersion) metadata.getVersion()).getZnodeVersion() >= 0);
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

        MetadataAccessor accessor = namespace.getNamespaceDriver().getMetadataAccessor(logName);
        accessor.createOrUpdateMetadata(logName.getBytes("UTF-8"));
        accessor.close();

        testCreateLogMetadataWithMissingPaths(uri, logName, logIdentifier, pathsToDelete, true, false);
    }

}
