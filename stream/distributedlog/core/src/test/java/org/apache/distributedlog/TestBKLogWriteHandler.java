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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.bk.LedgerAllocator;
import org.apache.distributedlog.bk.LedgerAllocatorPool;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.util.FailpointUtils;
import org.apache.distributedlog.util.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;



/**
 * Test {@link BKLogWriteHandler}.
 */
public class TestBKLogWriteHandler extends TestDistributedLogBase {

    @Rule
    public TestName runtime = new TestName();

    /**
     * Testcase: when write handler encounters exceptions on starting log segment
     * it should abort the transaction and return the ledger to the pool.
     */
    @Test(timeout = 60000)
    public void testAbortTransactionOnStartLogSegment() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        ensureURICreated(zkc, uri);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);
        confLocal.setEnableLedgerAllocatorPool(true);
        confLocal.setLedgerAllocatorPoolCoreSize(1);
        confLocal.setLedgerAllocatorPoolName("test-allocator-pool");

        BKDistributedLogNamespace namespace = (BKDistributedLogNamespace)
                NamespaceBuilder.newBuilder()
                        .conf(confLocal)
                        .uri(uri)
                        .build();
        DistributedLogManager dlm = namespace.openLog("test-stream");
        FailpointUtils.setFailpoint(FailpointUtils.FailPointName.FP_StartLogSegmentOnAssignLogSegmentSequenceNumber,
                FailpointUtils.FailPointActions.FailPointAction_Throw);
        try {
            AsyncLogWriter writer =  Utils.ioResult(dlm.openAsyncLogWriter());
            Utils.ioResult(writer.write(DLMTestUtil.getLogRecordInstance(1L)));
            fail("Should fail opening the writer");
        } catch (IOException ioe) {
            // expected
        } finally {
            FailpointUtils.removeFailpoint(
                    FailpointUtils.FailPointName.FP_StartLogSegmentOnAssignLogSegmentSequenceNumber);
        }

        LedgerAllocator allocator = ((BKNamespaceDriver) namespace.getNamespaceDriver())
                .getLedgerAllocator();
        assertTrue(allocator instanceof LedgerAllocatorPool);
        LedgerAllocatorPool allocatorPool = (LedgerAllocatorPool) allocator;
        assertEquals(0, allocatorPool.obtainMapSize());

        AsyncLogWriter writer = Utils.ioResult(dlm.openAsyncLogWriter());
        writer.write(DLMTestUtil.getLogRecordInstance(1L));
        Utils.close(writer);
    }

    @Test
    public void testLedgerNumber() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        ensureURICreated(zkc, uri);

        DistributedLogConfiguration confLocal = new DistributedLogConfiguration();
        confLocal.addConfiguration(conf);
        confLocal.setOutputBufferSize(0);

        BKDistributedLogNamespace namespace = (BKDistributedLogNamespace)
            NamespaceBuilder.newBuilder()
                .conf(confLocal)
                .uri(uri)
                .build();
        DistributedLogManager dlm = namespace.openLog("test-log");
        BookKeeperAdmin admin = new BookKeeperAdmin(zkServers);

        Set<Long> s1 = getLedgers(admin);
        LOG.info("Ledgers after init: " + s1);

        LogWriter writer = dlm.openLogWriter();
        writer.write(new LogRecord(1, "test-data".getBytes(StandardCharsets.UTF_8)));
        writer.close();

        Set<Long> s2 = getLedgers(admin);
        LOG.info("Ledgers after write: " + s2);
        dlm.delete();
        assertEquals(1, s2.size() - s1.size()); // exact 1 ledger created only

        Set<Long> s3 = getLedgers(admin);
        LOG.info("Ledgers after delete: " + s3);

        assertEquals(s1.size(), s3.size());

    }

    // Get all ledgers from BK admin
    private Set<Long> getLedgers(BookKeeperAdmin bkAdmin) throws IOException {
        Set<Long> res = new HashSet<>();
        bkAdmin.listLedgers().forEach(res::add);
        return res;
    }

}
