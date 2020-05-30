/**
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test reading the last confirmed and entry.
 */
@RunWith(Parameterized.class)
public class TestReadLastConfirmedAndEntry extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestReadLastConfirmedAndEntry.class);

    final BookKeeper.DigestType digestType;

    public TestReadLastConfirmedAndEntry(Class<? extends LedgerStorage> storageClass) {
        super(3);
        this.digestType = BookKeeper.DigestType.CRC32;
        this.baseConf.setAllowEphemeralPorts(false);
        this.baseConf.setLedgerStorageClass(storageClass.getName());
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] {
            { InterleavedLedgerStorage.class },
            { SortedLedgerStorage.class },
            { DbLedgerStorage.class },
        });
    }

    static class FakeBookie extends Bookie {

        final long expectedEntryToFail;
        final boolean stallOrRespondNull;

        public FakeBookie(ServerConfiguration conf, long expectedEntryToFail, boolean stallOrRespondNull)
                throws InterruptedException, BookieException, KeeperException, IOException {
            super(conf);
            this.expectedEntryToFail = expectedEntryToFail;
            this.stallOrRespondNull = stallOrRespondNull;
        }

        @Override
        public ByteBuf readEntry(long ledgerId, long entryId)
                throws IOException, NoLedgerException {
            if (entryId == expectedEntryToFail) {
                if (stallOrRespondNull) {
                    try {
                        Thread.sleep(600000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // ignore
                    }
                } else {
                    throw new NoEntryException(ledgerId, entryId);
                }
            }
            return super.readEntry(ledgerId, entryId);
        }
    }

    @Test
    public void testAdvancedLacWithEmptyResponse() throws Exception {
        byte[] passwd = "advanced-lac-with-empty-response".getBytes(UTF_8);

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setAddEntryTimeout(9999999);
        newConf.setReadEntryTimeout(9999999);

        // stop existing bookies
        stopAllBookies();
        // add fake bookies
        long expectedEntryIdToFail = 2;
        for (int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = newServerConfiguration();
            Bookie b = new FakeBookie(conf, expectedEntryIdToFail, i != 0);
            bs.add(startBookie(conf, b));
            bsConfs.add(conf);
        }

        // create bookkeeper
        BookKeeper newBk = new BookKeeper(newConf);
        // create ledger to write some data
        LedgerHandle lh = newBk.createLedger(3, 3, 2, digestType, passwd);
        for (int i = 0; i <= expectedEntryIdToFail; i++) {
            lh.addEntry("test".getBytes(UTF_8));
        }

        // open ledger to tail reading
        LedgerHandle newLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, passwd);
        long lac = newLh.readLastConfirmed();
        assertEquals(expectedEntryIdToFail - 1, lac);
        Enumeration<LedgerEntry> entries = newLh.readEntries(0, lac);

        int numReads = 0;
        long expectedEntryId = 0L;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals(expectedEntryId++, entry.getEntryId());
            ++numReads;
        }
        assertEquals(lac + 1, numReads);

        final AtomicInteger rcHolder = new AtomicInteger(-12345);
        final AtomicLong lacHolder = new AtomicLong(lac);
        final AtomicReference<LedgerEntry> entryHolder = new AtomicReference<LedgerEntry>(null);
        final CountDownLatch latch = new CountDownLatch(1);

        newLh.asyncReadLastConfirmedAndEntry(newLh.getLastAddConfirmed() + 1, 99999, false,
                new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
                rcHolder.set(rc);
                lacHolder.set(lastConfirmed);
                entryHolder.set(entry);
                latch.countDown();
            }
        }, null);

        lh.addEntry("another test".getBytes(UTF_8));

        latch.await();
        assertEquals(expectedEntryIdToFail, lacHolder.get());
        assertNull(entryHolder.get());
        assertEquals(BKException.Code.OK, rcHolder.get());
    }

    static class SlowReadLacBookie extends Bookie {

        private final long lacToSlowRead;
        private final CountDownLatch readLatch;

        public SlowReadLacBookie(ServerConfiguration conf,
                                 long lacToSlowRead, CountDownLatch readLatch)
                throws IOException, KeeperException, InterruptedException, BookieException {
            super(conf);
            this.lacToSlowRead = lacToSlowRead;
            this.readLatch = readLatch;
        }

        @Override
        public long readLastAddConfirmed(long ledgerId) throws IOException {
            long lac = super.readLastAddConfirmed(ledgerId);
            logger.info("Last Add Confirmed for ledger {} is {}", ledgerId, lac);
            if (lacToSlowRead == lac) {
                logger.info("Suspend returning lac {} for ledger {}", lac, ledgerId);
                try {
                    readLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // no-op
                }
            }
            return super.readLastAddConfirmed(ledgerId);
        }
    }

    static class ReadLastConfirmedAndEntryResult implements AsyncCallback.ReadLastConfirmedAndEntryCallback {

        int rc = -1234;
        long lac = -1234L;
        LedgerEntry entry = null;
        final CountDownLatch doneLatch = new CountDownLatch(1);

        @Override
        public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
            this.rc = rc;
            this.lac = lastConfirmed;
            this.entry = entry;
            doneLatch.countDown();
        }

        void await() throws InterruptedException {
            doneLatch.await();
        }
    }

    @Test
    public void testRaceOnLastAddConfirmed() throws Exception {
        byte[] passwd = "race-on-last-add-confirmed".getBytes(UTF_8);

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setAddEntryTimeout(9999999);
        newConf.setReadEntryTimeout(9999999);

        final long lacToSlowRead = 0L;
        final CountDownLatch readLatch = new CountDownLatch(1);

        // stop first bookie
        ServerConfiguration bsConf = killBookie(0);
        // start it with a slow bookie
        Bookie b = new SlowReadLacBookie(bsConf, lacToSlowRead, readLatch);
        bs.add(startBookie(bsConf, b));
        bsConfs.add(bsConf);

        // create bookkeeper
        BookKeeper newBk = new BookKeeper(newConf);
        // create ledger
        LedgerHandle lh = newBk.createLedger(3, 3, 3, digestType, passwd);
        // 0) write entry 0
        lh.addEntry("entry-0".getBytes(UTF_8));

        // open ledger to read
        LedgerHandle readLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, passwd);

        // 1) wait entry 0 to be committed
        ReadLastConfirmedAndEntryResult readResult = new ReadLastConfirmedAndEntryResult();
        readLh.asyncReadLastConfirmedAndEntry(0L, 9999999, true, readResult, null);

        // 2) write entry 1 to commit entry 0 => lac = 0
        lh.addEntry("entry-1".getBytes(UTF_8));
        readResult.await();
        assertEquals(BKException.Code.OK, readResult.rc);
        assertEquals(0L, readResult.lac);
        assertEquals(0L, readResult.entry.getEntryId());
        assertEquals("entry-0", new String(readResult.entry.getEntry(), UTF_8));

        // 3) write entry 2 to commit entry 1 => lac = 1
        lh.addEntry("entry-2".getBytes(UTF_8));
        // 4) count down read latch to trigger previous readLacAndEntry request
        readLatch.countDown();
        // 5) due to piggyback, the lac is updated to lac = 1
        while (readLh.getLastAddConfirmed() < 1L) {
            Thread.sleep(100);
        }
        // 6) write entry 3 to commit entry 2 => lac = 2
        lh.addEntry("entry-3".getBytes(UTF_8));
        // 7) readLastConfirmedAndEntry for next entry (we are expecting to read entry 1)
        readResult = new ReadLastConfirmedAndEntryResult();
        readLh.asyncReadLastConfirmedAndEntry(1L, 9999999, true, readResult, null);
        readResult.await();
        assertEquals(BKException.Code.OK, readResult.rc);
        assertEquals(2L, readResult.lac);
        assertEquals(1L, readResult.entry.getEntryId());
        assertEquals("entry-1", new String(readResult.entry.getEntry(), UTF_8));

        lh.close();
        readLh.close();

        newBk.close();
    }
}
