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
package com.twitter.distributedlog.admin;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.TestZooKeeperClientBuilder;
import com.twitter.distributedlog.annotations.DistributedLogAnnotations;
import com.twitter.distributedlog.exceptions.UnexpectedException;
import com.twitter.distributedlog.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.DLMTestUtil;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.TestDistributedLogBase;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.metadata.DryrunLogSegmentMetadataStoreUpdater;
import com.twitter.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import static org.junit.Assert.*;

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

    /**
     * {@link https://issues.apache.org/jira/browse/DL-44}
     */
    @DistributedLogAnnotations.FlakyTest
    @Ignore
    @Test(timeout = 60000)
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
        com.twitter.distributedlog.DistributedLogManagerFactory factory =
                new com.twitter.distributedlog.DistributedLogManagerFactory(confLocal, uri);
        com.twitter.distributedlog.DistributedLogManagerFactory readFactory =
                new com.twitter.distributedlog.DistributedLogManagerFactory(readConf, uri);

        String streamName = "change-sequence-number";

        // create completed log segments
        DistributedLogManager dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.generateCompletedLogSegments(dlm, confLocal, 4, 10);
        DLMTestUtil.injectLogSegmentWithGivenLogSegmentSeqNo(dlm, confLocal, 5, 41, false, 10, true);
        dlm.close();

        // create a reader
        DistributedLogManager readDLM = readFactory.createDistributedLogManagerWithSharedClients(streamName);
        AsyncLogReader reader = readDLM.getAsyncLogReader(DLSN.InitialDLSN);

        // read the records
        long expectedTxId = 1L;
        DLSN lastDLSN = DLSN.InitialDLSN;
        for (int i = 0; i < 4 * 10; i++) {
            LogRecordWithDLSN record = Await.result(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
            lastDLSN = record.getDlsn();
        }

        LOG.info("Injecting bad log segment '3'");

        dlm = factory.createDistributedLogManagerWithSharedClients(streamName);
        DLMTestUtil.injectLogSegmentWithGivenLogSegmentSeqNo(dlm, confLocal, 3L, 5 * 10 + 1, true, 10, false);

        LOG.info("Injected bad log segment '3'");

        // there isn't records should be read
        Future<LogRecordWithDLSN> readFuture = reader.readNext();
        try {
            LogRecordWithDLSN record = Await.result(readFuture);
            fail("Should fail reading next record "
                    + record
                    + " when there is a corrupted log segment");
        } catch (UnexpectedException ue) {
            // expected
        }

        LOG.info("Dryrun fix inprogress segment that has lower sequence number");

        // Dryrun
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(factory,
                new DryrunLogSegmentMetadataStoreUpdater(confLocal, getLogSegmentMetadataStore(factory)), streamName, false, false);

        try {
            reader = readDLM.getAsyncLogReader(lastDLSN);
            Await.result(reader.readNext());
            fail("Should fail reading next when there is a corrupted log segment");
        } catch (UnexpectedException ue) {
            // expected
        }

        LOG.info("Actual run fix inprogress segment that has lower sequence number");

        // Actual run
        DistributedLogAdmin.fixInprogressSegmentWithLowerSequenceNumber(factory,
                LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal, getLogSegmentMetadataStore(factory)), streamName, false, false);

        // be able to read more after fix
        reader = readDLM.getAsyncLogReader(lastDLSN);
        // skip the first record
        Await.result(reader.readNext());
        readFuture = reader.readNext();

        expectedTxId = 51L;
        LogRecord record = Await.result(readFuture);
        assertNotNull(record);
        DLMTestUtil.verifyLogRecord(record);
        assertEquals(expectedTxId, record.getTransactionId());
        expectedTxId++;

        for (int i = 1; i < 10; i++) {
            record = Await.result(reader.readNext());
            assertNotNull(record);
            DLMTestUtil.verifyLogRecord(record);
            assertEquals(expectedTxId, record.getTransactionId());
            expectedTxId++;
        }

        Utils.close(reader);
        readDLM.close();

        dlm.close();
        factory.close();
        readFactory.close();
    }
}
