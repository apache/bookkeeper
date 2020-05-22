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
package org.apache.bookkeeper.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-thread read test.
 */
public class MultipleThreadReadTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(MultipleThreadReadTest.class);

    BookKeeper.DigestType digestType;
    byte [] ledgerPassword = "aaa".getBytes();
    private int entriesPerLedger = 100;
    final SyncObj mainSyncObj = new SyncObj();

    class SyncObj {
        volatile int counter;
        boolean failed;
        public SyncObj() {
            counter = 0;
            failed = false;
        }
    }

    BookKeeperTestClient readBkc;

    public MultipleThreadReadTest() {
        super(6);
        this.digestType = BookKeeper.DigestType.CRC32;
        baseClientConf.setAddEntryTimeout(20);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        readBkc = new BookKeeperTestClient(baseClientConf);
    }

    private Thread getWriterThread(final int tNo, final LedgerHandle lh, final AtomicBoolean resultHolder) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                final SyncObj tSync = new SyncObj();
                for (int j = 0; j < entriesPerLedger; j++) {
                    final byte[] entry = ("Entry-" + tNo + "-" + j).getBytes();
                    lh.asyncAddEntry(entry, new AsyncCallback.AddCallback() {
                        @Override
                        public void addComplete(int rc, LedgerHandle ledgerHandle, long eid, Object o) {
                            SyncObj syncObj = (SyncObj) o;
                            synchronized (syncObj) {
                                if (rc != BKException.Code.OK) {
                                    LOG.error("Add entry {} failed : rc = {}", new String(entry, UTF_8), rc);
                                    syncObj.failed = true;
                                    syncObj.notify();
                                } else {
                                    syncObj.counter++;
                                    syncObj.notify();
                                }
                            }
                        }
                    }, tSync);
                }
                synchronized (tSync) {
                    while (!tSync.failed && tSync.counter < entriesPerLedger) {
                        try {
                            tSync.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    resultHolder.set(!tSync.failed);
                }
                // close this handle
                try {
                    lh.close();
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted on closing ledger handle {} : ", lh.getId(), ie);
                    Thread.currentThread().interrupt();
                } catch (BKException bke) {
                    LOG.error("Error on closing ledger handle {} : ", lh.getId(), bke);
                }
            }
        }, "WriteThread(Lid=" + lh.getId() + ")");
        t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                synchronized (mainSyncObj) {
                    mainSyncObj.failed = true;
                }
            }
        });
        return t;
    }

    private Thread getReaderThread(final int tNo, final LedgerHandle lh, final int ledgerNumber,
                                   final AtomicBoolean resultHolder) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                //LedgerHandle lh = clientList.get(0).openLedger(ledgerIds.get(tNo % numLedgers),
                //    digestType, ledgerPassword);
                long startEntryId = 0;
                long endEntryId;
                long eid = 0;
                while (startEntryId <= entriesPerLedger - 1) {
                    endEntryId = Math.min(startEntryId + 10 - 1, entriesPerLedger - 1);
                    final long numEntries = (endEntryId - startEntryId) + 1;
                    boolean success = true;
                    try {
                        Enumeration<LedgerEntry> list = lh.readEntries(startEntryId, endEntryId);
                        for (int j = 0; j < numEntries; j++) {
                            LedgerEntry e;
                            try {
                                e = list.nextElement();
                            } catch (NoSuchElementException exception) {
                                success = false;
                                break;
                            }
                            long curEid = eid++;
                            if (e.getEntryId() != curEid) {
                                LOG.error("Expected entry id {} for ledger {} but {} found.",
                                        curEid, lh.getId(), e.getEntryId());
                                success = false;
                                break;
                            }
                            byte[] data = e.getEntry();
                            if (!Arrays.equals(("Entry-" + ledgerNumber + "-" + e.getEntryId()).getBytes(), data)) {
                                LOG.error("Expected entry data 'Entry-{}-{}' but {} found for ledger {}.",
                                          ledgerNumber, e.getEntryId(), new String(data, UTF_8), lh.getId());
                                success = false;
                                break;
                            }
                        }
                        if (success) {
                            success = !list.hasMoreElements();
                            if (!success) {
                                LOG.error("Found more entries returned on reading ({}-{}) from ledger {}.",
                                        startEntryId, endEntryId, lh.getId());
                            }
                        }
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted on reading entries ({} - {}) from ledger {} : ",
                                startEntryId, endEntryId, lh.getId(), ie);
                        Thread.currentThread().interrupt();
                        success = false;
                    } catch (BKException bke) {
                        LOG.error("Failed on reading entries ({} - {}) from ledger {} : ",
                                startEntryId, endEntryId, lh.getId(), bke);
                        success = false;
                    }
                    resultHolder.set(success);
                    if (!success) {
                        break;
                    }
                    startEntryId = endEntryId + 1;
                }
            }
        }, "ReadThread(Tid =" + tNo  + ", Lid=" + lh.getId() + ")");
        t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                LOG.error("Uncaught exception in thread {} : ", thread.getName(), throwable);
                synchronized (mainSyncObj) {
                    mainSyncObj.failed = true;
                }
            }
        });
        return t;
    }

    /**
     * Ledger L is handled threads [L, T+(T/L), T+2*(T/L) ... ]
     * Reads are simultaneous, writes are sequential.
     * @throws java.io.IOException
     */
    public void multiLedgerMultiThreadRead(final int numLedgers,
                                           final int numThreads) throws IOException {
        assertTrue(numLedgers != 0 && numThreads >= numLedgers && numThreads % numLedgers == 0);

        // We create numThread/numLedger clients so that each client can be used to open a handle.
        try {
            final List<LedgerHandle> oldLedgerHandles = new ArrayList<LedgerHandle>();
            final List<Long> ledgerIds = new ArrayList<Long>();
            List<Thread> threadList = new ArrayList<Thread>();
            List<AtomicBoolean> writeResults = new ArrayList<AtomicBoolean>();
            // Start write threads.
            // Only one thread writes to a ledger, so just use numLedgers instead.
            for (int i = 0; i < numLedgers; i++) {
                LedgerHandle lh = bkc.createLedger(digestType, ledgerPassword);
                oldLedgerHandles.add(lh);
                ledgerIds.add(lh.getId());
                AtomicBoolean writeResult = new AtomicBoolean(false);
                writeResults.add(writeResult);
                Thread t;
                threadList.add(t = getWriterThread(i, oldLedgerHandles.get(i), writeResult));
                t.start();
            }
            // Wait for the threads to complete
            for (Thread t : threadList) {
                t.join();
            }
            synchronized (mainSyncObj) {
                if (mainSyncObj.failed) {
                    fail("Test failed because we encountered uncaught exception on adding entries.");
                }
            }
            for (int i = 0; i < numLedgers; i++) {
                assertTrue("Failed on adding entries for ledger " + oldLedgerHandles.get(i).getId(),
                           writeResults.get(i).get());
            }
            // Close the ledger handles.
            for (LedgerHandle lh : oldLedgerHandles) {
                try {
                    lh.close();
                } catch (BKException.BKLedgerClosedException e) {
                } catch (Exception e) {
                    fail("Error while closing handle.");
                }
            }
            // Now try to read.
            mainSyncObj.failed = false;
            threadList.clear();

            List<AtomicBoolean> readResults = new ArrayList<AtomicBoolean>();
            for (int i = 0; i < numThreads; i++) {
                AtomicBoolean readResult = new AtomicBoolean(false);
                Thread t;
                threadList.add(t = getReaderThread(i, readBkc.openLedger(ledgerIds.get(i % numLedgers),
                        digestType, ledgerPassword), i % numLedgers, readResult));
                readResults.add(readResult);
                t.start();
            }
            // Wait for the threads to complete.
            for (Thread t : threadList) {
                t.join();
            }
            synchronized (mainSyncObj) {
                if (mainSyncObj.failed) {
                    fail("Test failed because we encountered uncaught exception on reading entries");
                }
            }
            for (AtomicBoolean readResult : readResults) {
                assertTrue("Failed on read entries", readResult.get());
            }
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }

    @Test
    public void test10Ledgers20ThreadsRead() throws IOException {
        multiLedgerMultiThreadRead(10, 20);
    }

    @Test
    public void test10Ledgers200ThreadsRead() throws IOException {
        multiLedgerMultiThreadRead(10, 200);
    }

    @Test
    public void test1Ledger20ThreadsRead() throws IOException {
        multiLedgerMultiThreadRead(1, 20);
    }

    @Override
    public void tearDown() throws Exception {
        readBkc.close();
        super.tearDown();
    }
}
