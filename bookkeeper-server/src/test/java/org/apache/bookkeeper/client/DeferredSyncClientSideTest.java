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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.junit.Test;

/**
 * Client side tests on deferred sync write flag.
 */
public class DeferredSyncClientSideTest extends MockBookKeeperTestCase {

    static final byte[] PASSWORD = "password".getBytes();
    static final ByteBuf DATA = Unpooled.wrappedBuffer("foobar".getBytes());
    static final int NUM_ENTRIES = 100;

    @Test
    public void testAddEntryLastAddConfirmedDoesNotAdvance() throws Exception {
        try (WriteHandle wh = result(newCreateLedgerOp()
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withPassword(PASSWORD)
                .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                .execute())) {
            for (int i = 0; i < NUM_ENTRIES - 1; i++) {
                result(wh.appendAsync(DATA));
            }
            long lastEntryID = result(wh.appendAsync(DATA));
            assertEquals(NUM_ENTRIES - 1, lastEntryID);
            LedgerHandle lh = (LedgerHandle) wh;
            assertEquals(NUM_ENTRIES - 1, lh.getLastAddPushed());
            assertEquals(-1, lh.getLastAddConfirmed());
        }
    }

}