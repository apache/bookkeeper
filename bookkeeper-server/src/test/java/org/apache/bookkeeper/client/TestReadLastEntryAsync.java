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

import static org.junit.Assert.assertEquals;

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
  * Test read next entry and the latest last add confirmed.
  */
public class TestReadLastEntryAsync extends BookKeeperClusterTestCase {

    final DigestType digestType;

    public TestReadLastEntryAsync() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testTryReadLastEntryAsyncOnEmptyLedger() throws Exception {
        final LedgerHandle lh = bkc.createLedger(3, 3, 1, digestType, "".getBytes());
        lh.close();

        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());

        final CountDownLatch latch1 = new CountDownLatch(1);
        readLh.asyncReadLastEntry(new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
                assertEquals(BKException.Code.NoSuchEntryException, rc);

                latch1.countDown();
            }
        }, null);

        latch1.await();
        lh.close();
        readLh.close();
    }
}
