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
package org.apache.distributedlog.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.testing.annotations.FlakyTest;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.metadata.DryrunLogSegmentMetadataStoreUpdater;
import org.apache.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestDistributedLogAdmin.
 */
public class TestDistributedLogAdmin extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogAdmin.class);

    private ZooKeeperClient zooKeeperClient;

    @Before
    public void setup() throws Exception {
        zooKeeperClient = TestZooKeeperClientBuilder
            .newBuilder()
            .uri(createDLMURI("/"))
            .build();
        conf.setTraceReadAheadMetadataChanges(true);
    }

    @After
    public void teardown() throws Exception {
        zooKeeperClient.close();
    }

    @FlakyTest("https://issues.apache.org/jira/browse/DL-44")
    @Tag("flaky")
    @Test
    @SuppressWarnings("deprecation")
    public void testChangeSequenceNumber() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setLogSegmentSequenceNumberValidationEnabled(false);
        confLocal.setLogSegmentCacheEnabled(false);

        DistributedLogConfiguration readConf = new DistributedLogConfiguration();
        readConf.addConfiguration(conf);
        readConf.setLogSegmentCacheEnabled(false);
        readConf.setLogSegmentSequenceNumberValidationEnabled(true);

        URI uri = createDLMURI("/change-sequence-number");
        zooKeeperClient.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(confLocal)
                .uri(uri)
                .build();
        Namespace readNamespace = NamespaceBuilder.newBuilder()
                .conf(readConf)
                .uri(uri)
                .build();

        String streamName = "change-sequence-number";

        // create completed log segments
        DistributedLogManager dlm = namespace.openLog(streamName);
        DLMTestUtil.generateCompletedLogSegments(dlm, confLocal, 4, 10);
        DLMTestUtil.injectLogSegmentWithGivenLogSegmentSeqNo(dlm, confLocal, 5, 41, false, 10, true);
        dlm.close();

        // create a reader
        DistributedLogManager readDLM = readNamespace.openLog(streamName);
        AsyncLogReader reader = readDLM.getAsyncLogReader(DLSN.InitialDLSN);

        // read the records
        long expectedTxId = 1L;
        DLSN lastDLSN = DLSN.InitialDLSN;
        for (int i = 0; i < 4 * 10; i++) {
            LogRecordWithDLSN record = Utils.ioResult(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
            lastDLSN = record.getDlsn();
        }

        LOG.info("Injecting bad log segment '3'");

        dlm = namespace.openLog(streamName);
        DLMTestUtil.injectLogSegmentWithGivenLogSegmentSeqNo(dlm, confLocal, 3L, 5 * 10 + 1, true, 10, false);

        LOG.info("Injected bad log segment '3'");

        // there isn't records should be read
        CompletableFuture<LogRecordWithDLSN> readFuture = reader.readNext();
        try {
            LogRecordWithDLSN record = Utils.ioResult(readFuture);
            fail("Should fail reading next record "
                    + record
                    + " when there is a corrupted log segment");
        } catch (UnexpectedException ue) {
            // expected
        }

        LOG.info("Dryrun fix inprogress segment that has lower sequence number");

        // Dryrun
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(namespace,
                new DryrunLogSegmentMetadataStoreUpdater(confLocal,
                        getLogSegmentMetadataStore(namespace)), streamName, false, false);

        try {
            reader = readDLM.getAsyncLogReader(lastDLSN);
            Utils.ioResult(reader.readNext());
            fail("Should fail reading next when there is a corrupted log segment");
        } catch (UnexpectedException ue) {
            // expected
        }

        LOG.info("Actual run fix inprogress segment that has lower sequence number");

        // Actual run
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(namespace,
                LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal,
                        getLogSegmentMetadataStore(namespace)), streamName, false, false);

        // be able to read more after fix
        reader = readDLM.getAsyncLogReader(lastDLSN);
        // skip the first record
        Utils.ioResult(reader.readNext());
        readFuture = reader.readNext();

        expectedTxId = 51L;
        LogRecord record = Utils.ioResult(readFuture);
        assertNotNull(record);
        DLMTestUtil.verifyLogRecord(record);
        assertEquals(expectedTxId, record.getTransactionId());
        expectedTxId++;

        for (int i = 1; i < 10; i++) {
            record = Utils.ioResult(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
        }

        Utils.close(reader);
        readDLM.close();

        dlm.close();
        namespace.close();
        readNamespace.close();
    }
}
