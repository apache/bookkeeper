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
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
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

}
