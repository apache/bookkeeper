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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener}.
 */
public class TestReadEntryListener extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(TestReadEntryListener.class);

    final DigestType digestType;
    final byte[] passwd = "read-entry-listener".getBytes();

    public TestReadEntryListener() {
        super(6);
        this.digestType = DigestType.CRC32;
    }

    long getLedgerToRead(int ensemble, int writeQuorum, int ackQuorum, int numEntries)
            throws Exception {
        LedgerHandle lh = bkc.createLedger(ensemble, writeQuorum, ackQuorum, digestType, passwd);
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("" + i).getBytes());
        }
        lh.close();
        return lh.getId();
    }

    static class EntryWithRC {
        final LedgerEntry entry;
        final int rc;

        EntryWithRC(int rc, LedgerEntry entry) {
            this.rc = rc;
            this.entry = entry;
        }
    }

    static class LatchListener implements ReadEntryListener {

        final CountDownLatch l;
        final Map<Long, EntryWithRC> resultCodes;
        boolean inOrder = true;
        long nextEntryId;

        LatchListener(long startEntryId, int numEntries) {
            l = new CountDownLatch(numEntries);
            resultCodes = new HashMap<Long, EntryWithRC>();
            this.nextEntryId = startEntryId;
        }

        @Override
        public void onEntryComplete(int rc, LedgerHandle lh, LedgerEntry entry, Object ctx) {
            long entryId;
            if (BKException.Code.OK == rc) {
                if (nextEntryId != entry.getEntryId()) {
                    inOrder = false;
                }
                entryId = entry.getEntryId();
            } else {
                entryId = nextEntryId;
            }
            resultCodes.put(entryId, new EntryWithRC(rc, entry));
            ++nextEntryId;
            l.countDown();
        }

        void expectComplete() throws Exception {
            l.await();
        }

        boolean isInOrder() {
            return inOrder;
        }
    }

    ListenerBasedPendingReadOp createReadOp(LedgerHandle lh, long from, long to, ReadEntryListener listener) {
        return new ListenerBasedPendingReadOp(lh, bkc.getClientCtx(), from, to, listener, null, false);
    }

    void basicReadTest(boolean parallelRead) throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        for (int i = 0; i < numEntries; i++) {
            LatchListener listener = new LatchListener(i, 1);
            ListenerBasedPendingReadOp readOp = createReadOp(lh, i, i, listener);
            readOp.parallelRead(parallelRead).submit();
            listener.expectComplete();
            assertEquals(1, listener.resultCodes.size());
            EntryWithRC entry = listener.resultCodes.get((long) i);
            assertNotNull(entry);
            assertEquals(BKException.Code.OK, entry.rc);
            assertEquals(i, Integer.parseInt(new String(entry.entry.getEntry())));
            assertTrue(listener.isInOrder());
        }

        // read multiple entries
        LatchListener listener = new LatchListener(0L, numEntries);
        ListenerBasedPendingReadOp readOp = createReadOp(lh, 0, numEntries - 1, listener);
        readOp.parallelRead(parallelRead).submit();
        listener.expectComplete();
        assertEquals(numEntries, listener.resultCodes.size());
        for (int i = 0; i < numEntries; i++) {
            EntryWithRC entry = listener.resultCodes.get((long) i);
            assertNotNull(entry);
            assertEquals(BKException.Code.OK, entry.rc);
            assertEquals(i, Integer.parseInt(new String(entry.entry.getEntry())));
        }
        assertTrue(listener.isInOrder());

        lh.close();
    }

    @Test
    public void testBasicEnableParallelRead() throws Exception {
        basicReadTest(true);
    }

    @Test
    public void testBasicDisableParallelRead() throws Exception {
        basicReadTest(false);
    }

    private void readMissingEntriesTest(boolean parallelRead) throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 2, 2, numEntries);
        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        // read single entry
        LatchListener listener = new LatchListener(11L, 1);
        ListenerBasedPendingReadOp readOp = createReadOp(lh, 11, 11, listener);
        readOp.parallelRead(parallelRead).submit();
        listener.expectComplete();
        assertEquals(1, listener.resultCodes.size());
        EntryWithRC entry = listener.resultCodes.get(11L);
        assertNotNull(entry);
        assertEquals(BKException.Code.NoSuchEntryException, entry.rc);
        assertTrue(listener.isInOrder());

        // read multiple missing entries
        listener = new LatchListener(11L, 3);
        readOp = createReadOp(lh, 11, 13, listener);
        readOp.parallelRead(parallelRead).submit();
        listener.expectComplete();
        assertEquals(3, listener.resultCodes.size());
        assertTrue(listener.isInOrder());

        for (int i = 11; i <= 13; i++) {
            entry = listener.resultCodes.get((long) i);
            assertNotNull(entry);
            assertEquals(BKException.Code.NoSuchEntryException, entry.rc);
        }

        // read multiple entries with missing entries
        listener = new LatchListener(5L, 10);
        readOp = createReadOp(lh, 5L, 14L, listener);
        readOp.parallelRead(parallelRead).submit();
        listener.expectComplete();
        assertEquals(10, listener.resultCodes.size());
        assertTrue(listener.isInOrder());

        for (long i = 5L; i <= 14L; i++) {
            entry = listener.resultCodes.get(i);
            assertNotNull(entry);
            if (i < 10L) {
                assertEquals(BKException.Code.OK, entry.rc);
                assertEquals(i, Integer.parseInt(new String(entry.entry.getEntry())));
            } else {
                assertEquals(BKException.Code.NoSuchEntryException, entry.rc);
            }
        }

        lh.close();
    }

    @Test
    public void testReadMissingEntriesEnableParallelRead() throws Exception {
        readMissingEntriesTest(true);
    }

    @Test
    public void testReadMissingEntriesDisableParallelRead() throws Exception {
        readMissingEntriesTest(false);
    }

    private void readWithFailedBookiesTest(boolean parallelRead) throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 3, 3, numEntries);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble =
                lh.getLedgerMetadata().getEnsembleAt(5);
        // kill two bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));

        // read multiple entries
        LatchListener listener = new LatchListener(0L, numEntries);
        ListenerBasedPendingReadOp readOp = createReadOp(lh, 0, numEntries - 1, listener);
        readOp.parallelRead(parallelRead).submit();
        listener.expectComplete();
        assertEquals(numEntries, listener.resultCodes.size());
        for (int i = 0; i < numEntries; i++) {
            EntryWithRC entry = listener.resultCodes.get((long) i);
            assertNotNull(entry);
            assertEquals(BKException.Code.OK, entry.rc);
            assertEquals(i, Integer.parseInt(new String(entry.entry.getEntry())));
        }

        lh.close();
    }

    @Test
    public void testReadWithFailedBookiesEnableParallelRead() throws Exception {
        readWithFailedBookiesTest(true);
    }

    @Test
    public void testReadWithFailedBookiesDisableParallelRead() throws Exception {
        readWithFailedBookiesTest(false);
    }

    private void readFailureWithFailedBookiesTest(boolean parallelRead) throws Exception {
        int numEntries = 10;

        long id = getLedgerToRead(5, 3, 3, numEntries);

        LedgerHandle lh = bkc.openLedger(id, digestType, passwd);

        List<BookieId> ensemble =
            lh.getLedgerMetadata().getEnsembleAt(5);
        // kill bookies
        killBookie(ensemble.get(0));
        killBookie(ensemble.get(1));
        killBookie(ensemble.get(2));

        // read multiple entries
        LatchListener listener = new LatchListener(0L, numEntries);
        ListenerBasedPendingReadOp readOp = createReadOp(lh, 0, numEntries - 1, listener);
        readOp.parallelRead(parallelRead).submit();
        listener.expectComplete();
        assertEquals(numEntries, listener.resultCodes.size());
        for (int i = 0; i < numEntries; i++) {
            EntryWithRC entry = listener.resultCodes.get((long) i);
            assertNotNull(entry);
            if (i % 5 == 0) {
                assertEquals(BKException.Code.BookieHandleNotAvailableException, entry.rc);
            } else {
                assertEquals(BKException.Code.OK, entry.rc);
                assertEquals(i, Integer.parseInt(new String(entry.entry.getEntry())));
            }
        }

        lh.close();
    }

    @Test
    public void testReadFailureWithFailedBookiesEnableParallelRead() throws Exception {
        readFailureWithFailedBookiesTest(true);
    }

    @Test
    public void testReadFailureWithFailedBookiesDisableParallelRead() throws Exception {
        readFailureWithFailedBookiesTest(false);
    }
}
