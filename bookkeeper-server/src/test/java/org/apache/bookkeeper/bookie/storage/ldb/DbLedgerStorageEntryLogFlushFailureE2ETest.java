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
package org.apache.bookkeeper.bookie.storage.ldb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.awaitility.Awaitility;
import org.junit.Test;

public class DbLedgerStorageEntryLogFlushFailureE2ETest extends BookKeeperClusterTestCase {
    private static final byte[] PASSWD = "passwd".getBytes(UTF_8);

    public DbLedgerStorageEntryLogFlushFailureE2ETest() {
        super(1);
        baseConf.setLedgerStorageClass(FailOnFlushDbLedgerStorage.class.getName());
        baseConf.setFlushInterval(60000);
        baseConf.setGcWaitTime(60000);
        baseConf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 1);
        baseConf.setProperty(DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS, 1000);
        baseClientConf.setAddEntryTimeout(5);
    }

    @Test
    public void testClientWriteFailsAndBookieShutsDownAfterEntryLogFlushFailure() throws Exception {
        BookieImpl bookie = (BookieImpl) serverByIndex(0).getBookie();
        LedgerHandle lh = bkc.createLedger(1, 1, 1, DigestType.CRC32, PASSWD);
        byte[] payload = new byte[100 * 1024];
        BKException clientFailure = null;

        FailOnFlushDbLedgerStorage.injectFailureOnNextFlush();
        try {
            for (int i = 0; i < 20; i++) {
                try {
                    lh.addEntry(payload);
                } catch (BKException e) {
                    clientFailure = e;
                    break;
                }
            }

            assertNotNull("Client should observe a write failure after the entry log flush failure", clientFailure);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertFalse("Bookie should be shut down after entry log flush failure", bookie.isRunning()));
        } finally {
            FailOnFlushDbLedgerStorage.resetFailure();
        }
    }
}
