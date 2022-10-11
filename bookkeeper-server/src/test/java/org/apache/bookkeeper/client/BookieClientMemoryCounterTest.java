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

import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.SlowBufferedChannel;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.common.util.WritableListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Slf4j
public class BookieClientMemoryCounterTest extends BookKeeperClusterTestCase {

    public BookieClientMemoryCounterTest() {
        super(1);
        baseClientConf.setAddEntryTimeout(10000);
        baseClientConf.setAddEntryQuorumTimeout(10000);
        baseClientConf.setWriteMemoryHighWaterMark(8 * 1024);
        baseClientConf.setWriteMemoryLowWaterMark(2 * 1024);
    }

    @Test
    public void testPendingAddEntryMemory() throws Exception {
        confByIndex(0).setMaxAddsInProgressLimit(30);
        ServerConfiguration conf = killBookie(0);
        BookieServer bks = startAndAddBookie(conf,
            bookieWithMockedJournal(conf, 0, 1, 0))
            .getServer();


        AtomicBoolean writeState = new AtomicBoolean(true);
        bkc.getWriteMemoryCounter().register(new WritableListener() {
            @Override
            public void onWriteStateChanged(boolean writable) {
                log.info("Write state changed to {}", writeState);
                writeState.set(writable);
            }
        });
        LedgerHandle lh = bkc.createLedger(1,1, BookKeeper.DigestType.CRC32, "".getBytes());
        CountDownLatch complete = new CountDownLatch(1);
        byte[] msg = new byte[1024];

        CountDownLatch latch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            while (!writeState.get()) {
                log.info("wait for the memory released");
                TimeUnit.SECONDS.sleep(1);
            }
            lh.asyncAddEntry(msg, new AsyncCallback.AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    log.info("Add complete with rc {}", rc);
                    latch.countDown();
                }
            }, null);
        }
        latch.await();
        lh.close();
    }

    private Bookie bookieWithMockedJournal(ServerConfiguration conf,
                                           long getDelay, long addDelay, long flushDelay) throws Exception {
        Bookie bookie = new TestBookieImpl(conf);
        if (getDelay <= 0 && addDelay <= 0 && flushDelay <= 0) {
            return bookie;
        }

        List<Journal> journals = getJournals(bookie);
        for (int i = 0; i < journals.size(); i++) {
            Journal mock = spy(journals.get(i));
            when(mock.getBufferedChannelBuilder()).thenReturn((FileChannel fc, int capacity) ->  {
                SlowBufferedChannel sbc = new SlowBufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);
                sbc.setAddDelay(addDelay);
                sbc.setGetDelay(getDelay);
                sbc.setFlushDelay(flushDelay);
                return sbc;
            });

            journals.set(i, mock);
        }
        return bookie;
    }

    @SuppressWarnings("unchecked")
    private List<Journal> getJournals(Bookie bookie) throws NoSuchFieldException, IllegalAccessException {
        Field f = BookieImpl.class.getDeclaredField("journals");
        f.setAccessible(true);

        return (List<Journal>) f.get(bookie);
    }

}
