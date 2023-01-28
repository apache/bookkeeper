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
package org.apache.distributedlog.impl.metadata;

import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.intToBytes;
import static org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore.processLogMetadatas;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.List;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.metadata.LogMetadata;
import org.apache.distributedlog.metadata.LogMetadataForWriter;
import org.apache.distributedlog.util.DLUtils;
import org.junit.Test;

/**
 * TestZKLogStreamMetadataStoreUtils.
 */
public class TestZKLogStreamMetadataStoreUtils {

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingMaxTxnId() throws Exception {
        String rootPath = "/test-missing-max-txn-id";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingVersion() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1)),
                new Versioned<byte[]>(null, null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasWrongVersion() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1)),
                new Versioned<byte[]>(intToBytes(9999), null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingLockPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1)),
                new Versioned<byte[]>(intToBytes(LogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(null, null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingReadLockPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1)),
                new Versioned<byte[]>(intToBytes(LogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(null, null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingLogSegmentsPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1)),
                new Versioned<byte[]>(intToBytes(LogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(null, null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000, expected = UnexpectedException.class)
    public void testProcessLogMetadatasMissingAllocatorPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1)),
                new Versioned<byte[]>(intToBytes(LogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(DLUtils.serializeLogSegmentSequenceNumber(1L), new LongVersion(1)),
                new Versioned<byte[]>(null, null));
        processLogMetadatas(uri, logName, logIdentifier, metadatas, true);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000)
    public void testProcessLogMetadatasNoAllocatorPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        Versioned<byte[]> maxTxnIdData =
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1));
        Versioned<byte[]> logSegmentsData =
                new Versioned<byte[]>(DLUtils.serializeLogSegmentSequenceNumber(1L), new LongVersion(1));
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                maxTxnIdData,
                new Versioned<byte[]>(intToBytes(LogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                logSegmentsData);
        LogMetadataForWriter metadata =
                processLogMetadatas(uri, logName, logIdentifier, metadatas, false);
        assertTrue(maxTxnIdData == metadata.getMaxTxIdData());
        assertTrue(logSegmentsData == metadata.getMaxLSSNData());
        assertNull(metadata.getAllocationData().getValue());
        assertNull(metadata.getAllocationData().getVersion());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 60000)
    public void testProcessLogMetadatasAllocatorPath() throws Exception {
        String rootPath = "/test-missing-version";
        URI uri = DLMTestUtil.createDLMURI(2181, rootPath);
        String logName = "test-log";
        String logIdentifier = "<default>";
        Versioned<byte[]> maxTxnIdData =
                new Versioned<byte[]>(DLUtils.serializeTransactionId(1L), new LongVersion(1));
        Versioned<byte[]> logSegmentsData =
                new Versioned<byte[]>(DLUtils.serializeLogSegmentSequenceNumber(1L), new LongVersion(1));
        Versioned<byte[]> allocationData =
                new Versioned<byte[]>(DLUtils.logSegmentId2Bytes(1L), new LongVersion(1));
        List<Versioned<byte[]>> metadatas = Lists.newArrayList(
                new Versioned<byte[]>(null, null),
                new Versioned<byte[]>(null, null),
                maxTxnIdData,
                new Versioned<byte[]>(intToBytes(LogMetadata.LAYOUT_VERSION), null),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                new Versioned<byte[]>(new byte[0], new LongVersion(1)),
                logSegmentsData,
                allocationData);
        LogMetadataForWriter metadata =
                processLogMetadatas(uri, logName, logIdentifier, metadatas, true);
        assertTrue(maxTxnIdData == metadata.getMaxTxIdData());
        assertTrue(logSegmentsData == metadata.getMaxLSSNData());
        assertTrue(allocationData == metadata.getAllocationData());
    }

}
