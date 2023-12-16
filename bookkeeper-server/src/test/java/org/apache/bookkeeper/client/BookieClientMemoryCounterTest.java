/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.common.util.WritableListener;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;


@Slf4j
public class BookieClientMemoryCounterTest extends BookKeeperClusterTestCase {

    static final int MESSAGE_SIZE = 1024;
    static final long LOW_WATER_MARK = 10 * 1024;
    static final long HIGH_WATER_MARK = 20 * 1024;

    public BookieClientMemoryCounterTest() {
        super(1);
        baseClientConf.setWriteMemoryHighWaterMark(HIGH_WATER_MARK);
        baseClientConf.setWriteMemoryLowWaterMark(LOW_WATER_MARK);
    }

    @Test
    public void testPendingAddEntryMemory() throws Exception {
        // listen to the write state change events
        AtomicBoolean writeState = new AtomicBoolean(true);
        bkc.getWriteMemoryCounter().register(new WritableListener() {
            @Override
            public void onWriteStateChanged(boolean writable) {
                long usage = bkc.getWriteMemoryCounter().getSize();
                log.info("Write state changed to {}, current memory usage is {}", writeState, usage);
                // when the writable change to ture, the usage should under the LowWaterMark.
                // when the writable change to false, the usage should over than the HighWaterMark.
                if (writable) {
                    assertEquals(LOW_WATER_MARK - MESSAGE_SIZE, usage);
                } else {
                    assertEquals(HIGH_WATER_MARK + MESSAGE_SIZE, usage);
                }
                writeState.set(writable);
            }
        });

        LedgerHandle lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32, "".getBytes());
        byte[] msg = new byte[MESSAGE_SIZE];

        int testMessagesNum = 1000;

        // start a thread to send message
        AtomicInteger addCount = new AtomicInteger(testMessagesNum);
        new Thread(() -> {
            for (int i = 0; i < testMessagesNum; i++) {
                while (!writeState.get()) {
                    log.info("wait for the memory released");
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                lh.asyncAddEntry(msg, new AsyncCallback.AddCallback() {
                    @Override
                    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                        if (rc == BKException.Code.OK) {
                            log.info("Add complete with rc {}", rc);
                            addCount.getAndDecrement();
                        }
                    }
                }, null);
            }
        }).start();

        // while sending messages, we listen on the memory counter size. The size should never over than the
        // (highWaterMark + 1 message) bytes.
        while (addCount.get() != 0) {
            long size = bkc.getWriteMemoryCounter().getSize();
            assertTrue(size >= 0 && size < baseClientConf.getWriteMemoryHighWaterMark() + msg.length + 1);
            TimeUnit.MILLISECONDS.sleep(10);
        }

        lh.close();
    }
}
