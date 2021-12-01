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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Cases for RollLogSegments.
 */
public class TestTxnId extends TestDistributedLogBase {
    private static final Logger logger = LoggerFactory.getLogger(TestRollLogSegments.class);

    @Test
    public void testRecoveryAfterBookieCrash() throws Exception {
        String name = "txnid-after-crash";
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setEnsembleSize(5)
            .setWriteQuorumSize(5)
            .setAckQuorumSize(5)
            .setLogSegmentRollingIntervalMinutes(0)
            .setLogSegmentRollingConcurrency(-1)
            .setMaxLogSegmentBytes(400000);

        bkutil.addBookie();
        bkutil.addBookie();

        try (BKDistributedLogManager dlm = createNewDLM(conf, name);
             BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned()) {
            writer.write(DLMTestUtil.getLogRecordInstance(1, 100000)).join();
            writer.write(DLMTestUtil.getLogRecordInstance(2, 100000)).join();

            bkutil.removeBookie();
            bkutil.removeBookie();

            try {
                writer.write(DLMTestUtil.getLogRecordInstance(3, 100000)).join();
                Assert.fail("Shouldn't have succeeded");
            } catch (Exception e) {
                // expected
            }

            writer.write(DLMTestUtil.getLogRecordInstance(4, 100000)).join();
            Assert.fail("Shouldn't be able to write");
        } catch (Exception e) {
            // expected
        }

        bkutil.addBookie();
        bkutil.addBookie();

        try (BKDistributedLogManager dlm = createNewDLM(conf, name);
             BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned()) {
            long firstTxid = dlm.getLastTxId() + 1;
            for (int i = 0; i < 20; i++) {
                logger.info("Writing entry {}", i);
                writer.write(DLMTestUtil.getLogRecordInstance(firstTxid + i, 100000)).join();
                Thread.sleep(100);
            }
        }
    }
}
