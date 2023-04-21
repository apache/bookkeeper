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

import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookkeeper client with errors from Bookies.
 */
public class BookKeeperClientTestsWithBookieErrors extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperClientTestsWithBookieErrors.class);
    private static final int NUM_BOOKIES = 3;
    // The amount of sleeptime to sleep in injectSleepWhileRead fault injection
    private final long sleepTime;
    // Fault injection which would sleep for sleepTime before returning readEntry call
    private final Consumer<ByteBuf> injectSleepWhileRead;
    // Fault injection which would corrupt the entry data before returning readEntry call
    private final Consumer<ByteBuf> injectCorruptData;
    /*
     * The ordered list of injections for the Bookies (LedgerStorage). The first
     * bookie to get readEntry call will use the first faultInjection, and the
     * second bookie to get readentry call will use the second one and so on..
     *
     * It is assumed that there would be faultInjection for each Bookie. So if
     * there aren't NUM_BOOKIES num of faulInjections in this list then it will
     * fail with NullPointerException
     */
    private static List<Consumer<ByteBuf>> faultInjections = new ArrayList<Consumer<ByteBuf>>();
    /*
     * This map is used for storing LedgerStorage and the corresponding
     * faultInjection, according to the faultInjections list
     */
    private static HashMap<MockSortedLedgerStorage, Consumer<ByteBuf>> storageFaultInjectionsMap =
            new HashMap<MockSortedLedgerStorage, Consumer<ByteBuf>>();
    // Lock object for synchronizing injectCorruptData and faultInjections
    private static final Object lock = new Object();

    public BookKeeperClientTestsWithBookieErrors() {
        super(NUM_BOOKIES);
        baseConf.setLedgerStorageClass(MockSortedLedgerStorage.class.getName());

        // this fault injection will corrupt the entry data by modifying the last byte of the entry
        injectCorruptData = (byteBuf) -> {
            ByteBuffer buf = byteBuf.nioBuffer();
            int lastByteIndex = buf.limit() - 1;
            buf.put(lastByteIndex, (byte) (buf.get(lastByteIndex) - 1));
        };

        // this fault injection, will sleep for ReadEntryTimeout+2 secs before returning the readEntry call
        sleepTime = (baseClientConf.getReadEntryTimeout() + 2) * 1000;
        injectSleepWhileRead = (byteBuf) -> {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    @Before
    public void setUp() throws Exception {
        faultInjections.clear();
        storageFaultInjectionsMap.clear();
        super.setUp();
    }

    // Mock SortedLedgerStorage to simulate Fault Injection
    static class MockSortedLedgerStorage extends SortedLedgerStorage {
        public MockSortedLedgerStorage() {
            super();
        }

        @Override
        public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
            Consumer<ByteBuf> faultInjection;
            synchronized (lock) {
                faultInjection = storageFaultInjectionsMap.get(this);
                if (faultInjection == null) {
                    int readLedgerStorageIndex = storageFaultInjectionsMap.size();
                    faultInjection = faultInjections.get(readLedgerStorageIndex);
                    storageFaultInjectionsMap.put(this, faultInjection);
                }
            }
            ByteBuf byteBuf = super.getEntry(ledgerId, entryId);
            faultInjection.accept(byteBuf);
            return byteBuf;
        }
    }

    // In this testcase all the bookies will return corrupt entry
    @Test(timeout = 60000)
    public void testBookkeeperAllDigestErrors() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        // all the bookies need to return corrupt data
        faultInjections.add(injectCorruptData);
        faultInjections.add(injectCorruptData);
        faultInjections.add(injectCorruptData);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies will sleep (for ReadEntryTimeout+2 secs) before returning the data,
    // and the last one will return corrupt data
    @Test(timeout = 60000)
    public void testBKReadFirstTimeoutThenDigestError() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectCorruptData);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first one will return corrupt data and the last two bookies will
    // sleep (for ReadEntryTimeout+2 secs) before returning the data
    @Test(timeout = 60000)
    public void testBKReadFirstDigestErrorThenTimeout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectCorruptData);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies are killed before making the readentry call
    // and the last one will return corrupt data
    @Test(timeout = 60000)
    public void testBKReadFirstBookiesDownThenDigestError() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectCorruptData);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        wlh.addEntry("foobarfoo".getBytes());
        wlh.close();

        super.killBookie(0);
        super.killBookie(1);

        Thread.sleep(500);

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        try {
            rlh.readEntries(0, 0);
            fail("It is expected to fail with BKDigestMatchException");
        } catch (BKException.BKDigestMatchException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase all the bookies will sleep (for ReadEntryTimeout+2 secs) before returning the data
    @Test(timeout = 60000)
    public void testBKReadAllTimeouts() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        try {
            rlh.readEntries(4, 4);
            fail("It is expected to fail with BKTimeoutException");
        } catch (BKException.BKTimeoutException e) {
        }
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies will sleep (for ReadEntryTimeout+2 secs) before returning the data,
    // but the last one will return as expected
    @Test(timeout = 60000)
    public void testBKReadTwoBookiesTimeout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration().setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectSleepWhileRead);
        faultInjections.add(injectSleepWhileRead);
        faultInjections.add((byteBuf) -> {
        });

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        LedgerEntry entry = rlh.readEntries(4, 4).nextElement();
        Assert.assertTrue("The read Entry should match with what have been written",
                (new String(entry.getEntry())).equals("foobarfoo"));
        rlh.close();
        bkc.close();
    }

    // In this testcase first two bookies return the corrupt data,
    // but the last one will return as expected
    @Test(timeout = 60000)
    public void testBKReadTwoBookiesWithDigestError() throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        BookKeeper bkc = new BookKeeper(conf);

        byte[] passwd = "AAAAAAA".getBytes();

        faultInjections.add(injectCorruptData);
        faultInjections.add(injectCorruptData);
        faultInjections.add((byteBuf) -> {
        });

        LedgerHandle wlh = bkc.createLedger(3, 3, 2, DigestType.CRC32, passwd);
        long id = wlh.getId();
        for (int i = 0; i < 10; i++) {
            wlh.addEntry("foobarfoo".getBytes());
        }
        wlh.close();

        LedgerHandle rlh = bkc.openLedger(id, DigestType.CRC32, passwd);
        LedgerEntry entry = rlh.readEntries(4, 4).nextElement();
        Assert.assertTrue("The read Entry should match with what have been written",
                (new String(entry.getEntry())).equals("foobarfoo"));
        rlh.close();
        bkc.close();
    }
}
