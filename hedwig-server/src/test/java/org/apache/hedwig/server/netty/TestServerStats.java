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
package org.apache.hedwig.server.netty;

import static org.junit.Assert.assertEquals;

import org.apache.hedwig.server.netty.ServerStats.OpStats;
import org.junit.Test;

/** Tests that Statistics updation in hedwig Server */
public class TestServerStats {

    /**
     * Tests that updatLatency should not fail with
     * ArrayIndexOutOfBoundException when latency time coming as negative.
     */
    @Test(timeout=60000)
    public void testUpdateLatencyShouldNotFailWithAIOBEWithNegativeLatency()
            throws Exception {
        OpStats opStat = new OpStats();
        opStat.updateLatency(-10);
        assertEquals("Should not update any latency metrics", 0,
                opStat.numSuccessOps);

    }
}
