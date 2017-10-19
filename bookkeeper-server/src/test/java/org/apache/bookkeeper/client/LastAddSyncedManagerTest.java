/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client;

import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Tests on LastAddSyncedManager
 */
public class LastAddSyncedManagerTest {

    @Test
    public void testCalculateLastAddSynced() throws Exception {
        assertCalculateLastAddSynced(3, 1, 5, Arrays.asList(1L, 2L, 3L), 3);
        assertCalculateLastAddSynced(3, 2, 5, Arrays.asList(1L, 2L, 3L), 2);
        assertCalculateLastAddSynced(3, 3, 5, Arrays.asList(1L, 2L, 3L), 1);
    }

    @Test
    public void testCalculateLastAddSyncedNoEnoughData() throws Exception {
        assertCalculateLastAddSynced(3, 3, 3, Arrays.asList(), -1);
        assertCalculateLastAddSynced(3, 3, 3, Arrays.asList(1L), -1);
        assertCalculateLastAddSynced(3, 3, 3, Arrays.asList(1L, 2L), -1);
    }

    private void assertCalculateLastAddSynced(int writeQuorumSize, int ackQuorumSize, int ensembleSize, List<Long> lastAddSynced, long expectedLastAddSynced) {
        LastAddSyncedManager lastAddSyncedManager = new LastAddSyncedManager(writeQuorumSize, ackQuorumSize);
        for (int i = 0; i < lastAddSynced.size(); i++) {
            lastAddSyncedManager.updateBookie(i, lastAddSynced.get(i));
        }
        assertEquals(expectedLastAddSynced, lastAddSyncedManager.calculateCurrentLastAddSynced());
    }
}
