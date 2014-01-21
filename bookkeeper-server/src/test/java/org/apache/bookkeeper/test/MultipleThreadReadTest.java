package org.apache.bookkeeper.test;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleThreadReadTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(MultipleThreadReadTest.class);
    BookKeeper.DigestType digestType;
    byte[] ledgerPassword = "aaa".getBytes();
    private int entriesPerLedger = 1000;
    final SyncObj mainSyncObj = new SyncObj();

    class SyncObj {
        volatile int counter;
        boolean value;
        boolean failed;

        public SyncObj() {
            counter = 0;
            value = false;
            failed = false;
        }
    }

    final List<BookKeeperTestClient> clients = new ArrayList<BookKeeperTestClient>();

    public MultipleThreadReadTest() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    private void createClients(int numClients) {
        closeClientsAndClear();
        for (int i = 0; i < numClients; i++) {
            try {
                clients.add(new BookKeeperTestClient(baseClientConf));
            } catch (KeeperException e) {
                fail("Keeper exception while creating clients");
            } catch (IOException e) {
                fail("IOException while creating clients");
            } catch (InterruptedException e) {
                fail("Interrupted while creating clients");
            }
        }
    }

    private void closeClientsAndClear() {
        for (BookKeeperTestClient client : clients) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Error closing client");
            }
        }
        clients.clear();
    }

    private Thread getWriterThread(final int tNo, final LedgerHandle lh) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                final SyncObj tSync = new SyncObj();
                for (int j = 0; j < entriesPerLedger; j++) {
                    byte[] entry = ("Entry-" + tNo + "-" + j).getBytes();
                    lh.asyncAddEntry(entry, new AsyncCallback.AddCallback() {
                        @Override
                        public void addComplete(int rc, LedgerHandle ledgerHandle, long eid, Object o) {
                            SyncObj syncObj = (SyncObj) o;
                            try {
                                if (rc != BKException.Code.OK) {
                                    fail("Add entries returned a code that is not OK. rc:" + rc);
                                }
                                synchronized (syncObj) {
                                    syncObj.counter++;
                                    syncObj.notify();
                                }
                            } catch (AssertionError e) {
                                synchronized (syncObj) {
                                    syncObj.failed = true;
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
                    if (tSync.failed) {
                        fail("Failed to add entries.");
                    }
                }
                // close this handle
                try {
                    lh.close();
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
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

    private Thread getReaderThread(final int tNo, final LedgerHandle lh, final int ledgerNumber) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final SyncObj tSync = new SyncObj();
                    lh.asyncReadEntries(0, entriesPerLedger - 1, new AsyncCallback.ReadCallback() {
                        @Override
                        public void readComplete(int rc, LedgerHandle ledgerHandle, Enumeration<LedgerEntry> list,
                                        Object o) {
                            SyncObj syncObj = (SyncObj) o;
                            try {
                                if (rc != BKException.Code.OK) {
                                    fail("Read entries returned a code that is not OK. rc:" + rc);
                                }
                                for (int j = 0; j < entriesPerLedger; j++) {
                                    LedgerEntry e = null;
                                    try {
                                        e = list.nextElement();
                                    } catch (NoSuchElementException exception) {
                                        fail("Short read for ledger:" + ledgerHandle.getId());
                                    }
                                    byte[] data = e.getEntry();
                                    if (!Arrays.equals(("Entry-" + ledgerNumber + "-" + j).getBytes(), data)) {
                                        fail("Wrong entry while reading from ledger");
                                    }
                                }
                                if (list.hasMoreElements()) {
                                    fail("Read more elements than we wrote for ledger:" + ledgerHandle.getId());
                                }
                            } catch (AssertionError e) {
                                synchronized (syncObj) {
                                    syncObj.failed = true;
                                    syncObj.notify();
                                }
                            } finally {
                                synchronized (syncObj) {
                                    syncObj.value = true;
                                    syncObj.notify();
                                }
                            }
                        }
                    }, tSync);
                    synchronized (tSync) {
                        while (!tSync.value) {
                            tSync.wait();
                        }
                        if (tSync.failed) {
                            fail("Invalid read while reading form ledger:" + lh.getId());
                        }
                    }
                } catch (InterruptedException e) {
                    fail("Interrupted while waiting for replies.");
                    Thread.currentThread().interrupt();
                }
            }
        });
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
            // Start write threads.
            // Only one thread writes to a ledger, so just use numLedgers instead.
            for (int i = 0; i < numLedgers; i++) {
                LedgerHandle lh = bkc.createLedger(digestType, ledgerPassword);
                oldLedgerHandles.add(lh);
                ledgerIds.add(lh.getId());
                Thread t;
                threadList.add(t = getWriterThread(i, oldLedgerHandles.get(i)));
                t.start();
            }
            // Wait for the threads to complete
            for (Thread t : threadList) {
                t.join();
            }
            synchronized (mainSyncObj) {
                if (mainSyncObj.failed) {
                    fail("Test failed because we couldn't add entries.");
                }
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

            // Create clients used for reading. Each client is responsible for a disjoint range of numLedgers
            // threads. Client X will be used by threads [numLedgers*X .. numLedgers*(X+1))
            closeClientsAndClear();
            createClients(numThreads / numLedgers);

            for (int i = 0; i < numThreads; i++) {
                Thread t;
                threadList.add(t = getReaderThread(i, clients.get(i / numLedgers)
                    .openLedger(ledgerIds.get(i % numLedgers), digestType, ledgerPassword), i % numLedgers));
                t.start();
            }
            // Wait for the threads to complete.
            for (Thread t : threadList) {
                t.join();
            }
            synchronized (mainSyncObj) {
                if (mainSyncObj.failed) {
                    fail("Test failed because we couldn't read entries");
                }
            }
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
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
    public void test1Ledger50ThreadsRead() throws IOException {
        multiLedgerMultiThreadRead(1, 50);
    }

    @Override
    public void tearDown() throws Exception {
        closeClientsAndClear();
        super.tearDown();
    }
}
