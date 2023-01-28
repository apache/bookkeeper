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

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.TestZooKeeperClientBuilder;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.common.util.SchedulerUtils;
import org.apache.distributedlog.metadata.DryrunLogSegmentMetadataStoreUpdater;
import org.apache.distributedlog.metadata.LogSegmentMetadataStoreUpdater;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 * TestDLCK.
 */
public class TestDLCK extends TestDistributedLogBase {

    static final Logger LOG = LoggerFactory.getLogger(TestDLCK.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setEnableLedgerAllocatorPool(true).setLedgerAllocatorPoolName("test");

    private ZooKeeperClient zkc;

    @Before
    public void setup() throws Exception {
        zkc = TestZooKeeperClientBuilder
            .newBuilder()
            .uri(createDLMURI("/"))
            .build();
    }

    @After
    public void teardown() throws Exception {
        zkc.close();
    }

    static Map<Long, LogSegmentMetadata> getLogSegments(DistributedLogManager dlm) throws Exception {
        Map<Long, LogSegmentMetadata> logSegmentMap =
                new HashMap<Long, LogSegmentMetadata>();
        List<LogSegmentMetadata> segments = dlm.getLogSegments();
        for (LogSegmentMetadata segment : segments) {
            logSegmentMap.put(segment.getLogSegmentSequenceNumber(), segment);
        }
        return logSegmentMap;
    }

    static void verifyLogSegment(Map<Long, LogSegmentMetadata> segments,
                                 DLSN lastDLSN, long logSegmentSequenceNumber,
                                 int recordCount, long lastTxId) {
        LogSegmentMetadata segment = segments.get(logSegmentSequenceNumber);
        assertNotNull(segment);
        assertEquals(lastDLSN, segment.getLastDLSN());
        assertEquals(recordCount, segment.getRecordCount());
        assertEquals(lastTxId, segment.getLastTxId());
    }

    @Test(timeout = 60000)
    @SuppressWarnings("deprecation")
    public void testCheckAndRepairDLNamespace() throws Exception {
        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.loadConf(conf);
        confLocal.setImmediateFlushEnabled(true);
        confLocal.setOutputBufferSize(0);
        confLocal.setLogSegmentSequenceNumberValidationEnabled(false);
        confLocal.setLogSegmentCacheEnabled(false);
        URI uri = createDLMURI("/check-and-repair-dl-namespace");
        zkc.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(confLocal)
                .uri(uri)
                .build();
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("dlck-tool")
                .numThreads(1)
                .build();
        ExecutorService executorService = Executors.newCachedThreadPool();

        String streamName = "check-and-repair-dl-namespace";

        // Create completed log segments
        DistributedLogManager dlm = namespace.openLog(streamName);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, confLocal, 1L, 1L, 10, false);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, confLocal, 2L, 11L, 10, true);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, confLocal, 3L, 21L, 10, false);
        DLMTestUtil.injectLogSegmentWithLastDLSN(dlm, confLocal, 4L, 31L, 10, true);

        // dryrun
        DistributedLogAdmin.checkAndRepairDLNamespace(
                uri,
                namespace,
                new DryrunLogSegmentMetadataStoreUpdater(confLocal, getLogSegmentMetadataStore(namespace)),
                scheduler,
                false,
                false);

        Map<Long, LogSegmentMetadata> segments = getLogSegments(dlm);
        LOG.info("segments after drynrun {}", segments);
        verifyLogSegment(segments, new DLSN(1L, 18L, 0L), 1L, 10, 10L);
        verifyLogSegment(segments, new DLSN(2L, 16L, 0L), 2L, 9, 19L);
        verifyLogSegment(segments, new DLSN(3L, 18L, 0L), 3L, 10, 30L);
        verifyLogSegment(segments, new DLSN(4L, 16L, 0L), 4L, 9, 39L);

        // check and repair
        DistributedLogAdmin.checkAndRepairDLNamespace(
                uri,
                namespace,
                LogSegmentMetadataStoreUpdater.createMetadataUpdater(confLocal, getLogSegmentMetadataStore(namespace)),
                scheduler,
                false,
                false);

        segments = getLogSegments(dlm);
        LOG.info("segments after repair {}", segments);
        verifyLogSegment(segments, new DLSN(1L, 18L, 0L), 1L, 10, 10L);
        verifyLogSegment(segments, new DLSN(2L, 18L, 0L), 2L, 10, 20L);
        verifyLogSegment(segments, new DLSN(3L, 18L, 0L), 3L, 10, 30L);
        verifyLogSegment(segments, new DLSN(4L, 18L, 0L), 4L, 10, 40L);

        dlm.close();
        SchedulerUtils.shutdownScheduler(executorService, 5, TimeUnit.MINUTES);
        namespace.close();
    }

}
