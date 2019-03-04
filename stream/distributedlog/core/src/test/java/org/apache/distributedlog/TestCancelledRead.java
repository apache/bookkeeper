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
package org.apache.distributedlog;

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.api.LogWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Cases for RollLogSegments.
 */
public class TestCancelledRead extends TestDistributedLogBase {
    private static final Logger logger = LoggerFactory.getLogger(TestRollLogSegments.class);

    @Test(timeout = 600000)
    public void testWritingAndTailing() throws Exception {
        String name = "writing-and-tailing";
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setReadAheadWaitTime(5000)
            .setOutputBufferSize(0)
            .setCreateStreamIfNotExists(true)
            .setImmediateFlushEnabled(true)
            .setFailFastOnStreamNotReady(true)
            .setPeriodicFlushFrequencyMilliSeconds(0)
            .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE)
            .setEnableReadAhead(false)
            .setLogSegmentRollingIntervalMinutes(0);

        CompletableFuture<Void> f = new CompletableFuture<>();
        long entryId = 0;

        try (BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(conf, name);
             LogWriter writer = dlm.startLogSegmentNonPartitioned()) {
            entryId++;
            writer.write(DLMTestUtil.getLogRecordInstance(entryId, 100000));
        }

        try (BKDistributedLogManager dlmReader = (BKDistributedLogManager) createNewDLM(conf, name)) {
            BKAsyncLogReader reader = (BKAsyncLogReader) dlmReader.getAsyncLogReader(DLSN.InitialDLSN);

            assertNotNull(reader.readNext().get());

            conf.setMaxLogSegmentBytes(1000);
            try (BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(conf, name);
                 LogWriter writer = dlm.startLogSegmentNonPartitioned()) {
                for (int i = 0; i < 100; i++) {
                    entryId++;
                    writer.write(DLMTestUtil.getLogRecordInstance(entryId, 100));

                    assertNotNull(reader.readNext().get());
                }
            } finally {
                reader.asyncClose().get();
            }
        }
    }
}
