/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.verifier;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.bookkeeper.client.BKException;

/**
 * Encapsulates logic for playing and verifying operations against a bookkeeper-like
 * interface. The test state consists of a set of ledgers in 1 of several states:
 * 1) opening -- waiting for driver to complete open
 * 2) open -- valid targets for reads and writes
 * 3) live -- valid targets for reads
 * 4) deleting
 * Each ledger moves in sequence through these states.  See startWrite for the
 * code driving the lifecycle.
 */
public class BookkeeperVerifier {
    private final Queue<Exception> errors = new LinkedList<>();

    private synchronized boolean checkReturn(long ledgerID, int rc) {
        if (BKException.Code.OK != rc) {
            String error = String.format("Got error %d on ledger %d", rc, ledgerID);
            System.out.println(error);
            propagateExceptionToMain(BKException.create(rc));
            return true;
        } else {
            return false;
        }
    }

    private synchronized void propagateExceptionToMain(Exception e) {
        errors.add(e);
        this.notifyAll();
    }

    private synchronized void printThrowExceptions() throws Exception {
        if (!errors.isEmpty()) {
            for (Exception e: errors) {
                System.out.format("Error found: %s%n", e.toString());
                e.printStackTrace();
            }
            throw errors.poll();
        }
    }

    /**
     * Provides an interface for translating test operations into operations on a
     * cluster.
     */
    public interface BookkeeperDriver {
        void createLedger(
                long ledgerID, int enSize, int writeQSize, int ackQSize,
                Consumer<Integer> cb
        );

        void closeLedger(
                long ledgerID,
                Consumer<Integer> cb
        );

        void deleteLedger(
                long ledgerID,
                Consumer<Integer> cb
        );

        void writeEntry(
                long ledgerID,
                long entryID,
                byte[] data,
                Consumer<Integer> cb
        );

        /**
         * Callback for reads.
         */
        interface ReadCallback {
            void complete(
                    long ledgerID,
                    ArrayList<byte[]> results
            );
        }

        void readEntries(
                long ledgerID,
                long firstEntryID,
                long lastEntryID,
                BiConsumer<Integer, ArrayList<byte[]>> cb);
    }

    private final BookkeeperDriver driver;

    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;
    private final int duration;
    private final int drainTimeout;
    private final int targetConcurrentLedgers;
    private final int targetConcurrentWrites;
    private final int targetWriteGroup;
    private final int targetReadGroup;
    private final int targetLedgers;
    private final int targetEntrySize;
    private final int targetConcurrentReads;
    private final double coldToHotRatio;

    private final long targetLedgerEntries;

    BookkeeperVerifier(
            BookkeeperDriver driver,
            int ensembleSize,
            int writeQuorum,
            int ackQuorum,
            int duration,
            int drainTimeout,
            int targetConcurrentLedgers,
            int targetConcurrentWrites,
            int targetWriteGroup,
            int targetReadGroup,
            int targetLedgers,
            long targetLedgerSize,
            int targetEntrySize,
            int targetConcurrentReads,
            double coldToHotRatio) {
        this.driver = driver;
        this.ensembleSize = ensembleSize;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
        this.duration = duration;
        this.drainTimeout = drainTimeout;
        this.targetConcurrentLedgers = targetConcurrentLedgers;
        this.targetConcurrentWrites = targetConcurrentWrites;
        this.targetWriteGroup = targetWriteGroup;
        this.targetReadGroup = targetReadGroup;
        this.targetLedgers = targetLedgers;
        this.targetEntrySize = targetEntrySize;
        this.targetConcurrentReads = targetConcurrentReads;
        this.coldToHotRatio = coldToHotRatio;

        this.targetLedgerEntries = targetLedgerSize / targetEntrySize;
    }

    private int outstandingWriteCount = 0;
    private int outstandingReadCount = 0;
    private long nextLedger = 0;
    private long getNextLedgerID() {
        return nextLedger++;
    }

    /**
     * State required to regenerate an entry.
     */
    class EntryInfo {
        private final long entryID;
        private final long seed;
        EntryInfo(long entryID, long seed) {
            this.entryID = entryID;
            this.seed = seed;
        }
        byte[] getBuffer() {
            Random rand = new Random(seed);
            byte[] ret = new byte[targetEntrySize];
            rand.nextBytes(ret);
            return ret;
        }
        long getEntryID() {
            return entryID;
        }
    }

    /**
     * Contains the state required to reconstruct the contents of any entry in the ledger.
     * The seed value passed into the constructor fully determines the contents of the
     * ledger.  Each EntryInfo has its own seed generated sequentially from a Random instance
     * seeded from the original seed.  It then uses that seed to generate a secondary Random
     * instance for generating the bytes within the entry.  See EntryIterator for details.
     * Random(seed)
     *  |
     *  E0 -> Random(E0) -> getBuffer()
     *  |
     *  E1 -> Random(E1) -> getBuffer()
     *  |
     *  E2 -> Random(E2) -> getBuffer()
     *  |
     *  E3 -> Random(E3) -> getBuffer()
     *  |
     *  E4 -> Random(E4) -> getBuffer()
     *  |
     *  ...
     */
    class LedgerInfo {
        private final long ledgerID;
        private final long seed;
        private long lastEntryIDCompleted = -1;
        private long confirmedLAC = -1;
        private boolean closed = false;

        final TreeSet<Long> writesInProgress = new TreeSet<>();
        final TreeSet<Long> writesCompleted = new TreeSet<>();
        int readsInProgress = 0;
        Consumer<Consumer<Integer>> onLastOp = null;
        Consumer<Consumer<Integer>> onLastWrite = null;

        EntryIterator iter;

        LedgerInfo(long ledgerID, long seed) {
            this.ledgerID = ledgerID;
            this.seed = seed;
            iter = new EntryIterator();
        }

        long getLastEntryIDCompleted() {
            return lastEntryIDCompleted;
        }

        long getConfirmedLAC() {
            return confirmedLAC;
        }

        ArrayList<EntryInfo> getNextEntries(int num) {
            ArrayList<EntryInfo> ret = new ArrayList<>();
            for (int i = 0; i < num && iter.hasNext(); ++i) {
                ret.add(iter.next());
            }
            return ret;
        }

        class EntryIterator implements Iterator<EntryInfo> {
            Random rand;
            long currentID;
            long currentSeed;

            EntryIterator() {
                seek(-1);
            }

            void seek(long entryID) {
                currentID = -1;
                currentSeed = seed;
                rand = new Random(seed);
                while (currentID < entryID) {
                    advance();
                }
            }

            void advance() {
                currentSeed = rand.nextLong();
                currentID++;
            }

            EntryInfo get() {
                return new EntryInfo(currentID, currentSeed);
            }

            @Override
            public boolean hasNext() {
                return currentID < targetLedgerEntries;
            }


            @Override
            public EntryInfo next() {
                advance();
                return get();
            }
        }

        EntryIterator getIterator() {
            return new EntryIterator();
        }

        void openWrite(long entryID) {
            writesInProgress.add(entryID);
            System.out.format("Open writes, %s%n", writesInProgress.toString());
        }

        void incReads() {
            readsInProgress++;
            System.out.format("Inc reads to %d%n", readsInProgress);
        }

        /**
         * The idea here is that we may need to register an operation which needs to run
         * whenever the final op completes on this Ledger (like deletion).  If there
         * are none, newOnLastOp should be called synchronously with cb.  Otherwise,
         * cb should be called synchronously with cb and newOnLastOp should be called
         * with the cb passed in with the decReads or closeWrite.
         * In the deletion case, cb would be the callback for the error from
         * the deletion operation (if it happens).  The reason for all of this is that
         * the delete case will need to chain an async call to delete into the async callback
         * chain for whatever the last operation to complete on this Ledger.  newOnLastOp
         * would invoke that delete.  The cb passed in allows it to pick up and continue
         * the original chain.
         * @param cb Callback to get result of newOnLastOp if called now
         * @param newOnLastOp Callback to be invoked on the last decReads or closeWrite,
         *                    should be passed the cb passed in with the final closeWrite
         *                    or decReads
         */
        void onLastOpComplete(
                Consumer<Integer> cb,
                Consumer<Consumer<Integer>> newOnLastOp) {
            checkState(onLastOp == null);
            onLastOp = newOnLastOp;
            checkOpComplete(cb);
        }

        /**
         * Very similar to onLastOpComplete, but gets called on the final call to closeWrite.
         * @param cb Callback to get result of newOnLastWrite if called now
         * @param newOnLastWrite Callback to be invoked on the last closeWrite,
         *                       should be passed the cb passed in with the final closeWrite.
         */
        void onLastWriteComplete(
                Consumer<Integer> cb,
                Consumer<Consumer<Integer>> newOnLastWrite) {
            assert (onLastWrite == null);
            onLastWrite = newOnLastWrite;
            checkWriteComplete(cb);
        }

        void closeWrite(long entryID, Consumer<Integer> cb) {
            writesInProgress.remove(entryID);
            writesCompleted.add(entryID);
            long completedTo = writesInProgress.isEmpty() ? Long.MAX_VALUE : writesInProgress.first();
            while (!writesCompleted.isEmpty() && writesCompleted.first() < completedTo) {
                lastEntryIDCompleted = writesCompleted.first();
                writesCompleted.remove(writesCompleted.first());
            }
            checkWriteComplete((rc) -> {
                checkReturn(ledgerID, rc);
                checkOpComplete(cb);
            });
        }

        void updateLAC(long lac) {
            if (lac > confirmedLAC) {
                confirmedLAC = lac;
            }
        }

        void decReads(Consumer<Integer> cb) {
            --readsInProgress;
            checkOpComplete(cb);
        }

        private void checkWriteComplete(Consumer<Integer> cb) {
            if (writesInProgress.isEmpty() && onLastWrite != null) {
                System.out.format("checkWriteComplete: done%n");
                onLastWrite.accept(cb);
                onLastWrite = null;
            } else {
                System.out.format(
                        "checkWriteComplete: ledger %d, writesInProgress %s%n",
                        ledgerID,
                        writesInProgress.toString());
                cb.accept(0);
            }
        }

        private void checkOpComplete(Consumer<Integer> cb) {
            if (readsInProgress == 0 && writesInProgress.isEmpty() && onLastOp != null) {
                System.out.format("checkOpComplete: done%n");
                onLastOp.accept(cb);
                onLastOp = null;
            } else {
                System.out.format(
                        "checkOpComplete: ledger %d, writesInProgress %s, readsInProgress %d%n",
                        ledgerID,
                        writesInProgress.toString(), readsInProgress);
                cb.accept(0);
            }
        }

        public boolean isClosed() {
            return closed;
        }
        public void setClosed() {
            closed = true;
            confirmedLAC = lastEntryIDCompleted;
        }
    }

    private final Set<LedgerInfo> openingLedgers = new HashSet<>();
    private final Set<LedgerInfo> openLedgers = new HashSet<>();
    private final Set<LedgerInfo> liveLedgers = new HashSet<>();
    private final Random opRand = new Random();

    private LedgerInfo getRandomLedger(Collection<LedgerInfo> ledgerCollection) {
        int elem = opRand.nextInt(ledgerCollection.size());
        Iterator<LedgerInfo> iter = ledgerCollection.iterator();
        for (int i = 0; i < elem; ++i) {
            iter.next();
        }
        return iter.next();
    }

    private synchronized boolean startRead() {
        if (outstandingReadCount > targetConcurrentReads) {
            System.out.format("Not starting another read, enough in progress%n");
            /* Caller should exit and wait for outstandingReadCount to fall */
            return false;
        }
        LedgerInfo ledger;
        if (!openLedgers.isEmpty() && (opRand.nextDouble() > coldToHotRatio)) {
            ledger = getRandomLedger(openLedgers);
            System.out.format("Reading from open ledger %d%n", ledger.ledgerID);
        } else if (!liveLedgers.isEmpty()) {
            ledger = getRandomLedger(liveLedgers);
            System.out.format("Reading from cold ledger %d%n", ledger.ledgerID);
        } else {
            /* No readable ledgers, either startWrite can make progress, or there are already ledgers
             * opening.
             */
            return false;
        }
        long lastEntryCompleted = ledger.getConfirmedLAC();
        if (lastEntryCompleted <= 0) {
            System.out.format("No readable entries in ledger %d, let's wait%n", ledger.ledgerID);
            /* Either startWrite can make progress or there are already a bunch in progress */
            return false;
        }
        long start = Math.abs(opRand.nextLong() % lastEntryCompleted);
        long end = start + targetReadGroup > lastEntryCompleted ? lastEntryCompleted : start + targetReadGroup;
        System.out.format("Reading %d -> %d from ledger %d%n", start, end, ledger.ledgerID);
        LedgerInfo finalLedger = ledger;
        ledger.incReads();
        driver.readEntries(ledger.ledgerID, start, end, (rc, results) -> {
            synchronized (BookkeeperVerifier.this) {
                if (checkReturn(ledger.ledgerID, rc)) {
                    return;
                }
                System.out.format("Read %d -> %d from ledger %d complete%n", start, end, ledger.ledgerID);
                long current = start;
                LedgerInfo.EntryIterator iterator = finalLedger.getIterator();
                iterator.seek(current - 1);
                for (byte[] result : results) {
                    byte[] check = iterator.next().getBuffer();
                    if (result.length != check.length) {
                        propagateExceptionToMain(new Exception(String.format(
                                "Mismatched entry length on entry %d for ledger %d, read returned %d, should be %d",
                                current, ledger.ledgerID, result.length, check.length)
                        ));
                    }
                        /* Verify contents */
                    if (!Arrays.equals(check, result)) {
                        int i = 0;
                        for (; i < check.length; ++i) {
                            if (check[i] != result[i]) {
                                break;
                            }
                        }
                        propagateExceptionToMain(new Exception(String.format(
                                "Mismatched entry contents on entry %d for ledger %d at offset %d, length %d",
                                current, ledger.ledgerID, i, check.length)
                        ));
                    }
                    current++;
                }
                finalLedger.decReads((rc2) -> {
                    synchronized (BookkeeperVerifier.this) {
                        checkReturn(ledger.ledgerID, rc2);
                        System.out.format("Read %d -> %d from ledger %d releasing read%n", start, end, ledger.ledgerID);
                        outstandingReadCount--;
                        BookkeeperVerifier.this.notifyAll();
                    }
                });
            }
        });
        ++outstandingReadCount;
        return true;
    }

    class WriteCallback implements Consumer<Integer> {
        private int completed = 0;
        private final int toWaitFor;
        private final LedgerInfo ledger;
        private final long lastEntry;
        private final long pendingLAC;
        WriteCallback(LedgerInfo ledger, long lastEntry, long pendingLAC, int toWaitFor) {
            this.toWaitFor = toWaitFor;
            this.ledger = ledger;
            this.lastEntry = lastEntry;
            this.pendingLAC = pendingLAC;
        }

        @Override
        public void accept(Integer rc) {
            synchronized (BookkeeperVerifier.this) {
                if (checkReturn(ledger.ledgerID, rc)) {
                    return;
                }
                ++completed;
                if (toWaitFor == completed) {
                    System.out.format("Writes ending at %d complete on ledger %d%n", lastEntry, ledger.ledgerID);
                    ledger.closeWrite(lastEntry, (rc2) -> {
                        synchronized (BookkeeperVerifier.this) {
                            checkReturn(ledger.ledgerID, rc2);
                            System.out.format("Writes ending at %d complete on ledger %d releasing write%n",
                                    lastEntry, ledger.ledgerID);
                            --outstandingWriteCount;
                            BookkeeperVerifier.this.notifyAll();
                        }
                    });
                    ledger.updateLAC(pendingLAC);
                }
            }
        }
    }

    /**
     * Attempt to start one more write, return false if too many are in progress.
     * @return false if unable to start more
     */
    private synchronized boolean startWrite() {
        if (outstandingWriteCount > targetConcurrentWrites) {
            System.out.format("Write paused, too many outstanding writes%n");
            /* Caller should release lock and wait for outstandingWriteCount to fall */
            return false;
        }
        if (openLedgers.size() + openingLedgers.size() < targetConcurrentLedgers) {
            /* Not enough open ledgers, open a new one -- counts as a write */
            long newID = getNextLedgerID();
            System.out.format("Creating new ledger %d%n", newID);
            LedgerInfo ledger = new LedgerInfo(newID, opRand.nextLong());
            openingLedgers.add(ledger);
            driver.createLedger(newID, ensembleSize, writeQuorum, ackQuorum, (rc) -> {
                synchronized (BookkeeperVerifier.this) {
                    checkReturn(newID, rc);
                    System.out.format("Created new ledger %d%n", newID);
                    openingLedgers.remove(ledger);
                    openLedgers.add(ledger);
                    --outstandingWriteCount;
                    BookkeeperVerifier.this.notifyAll();
                }
            });
            ++outstandingWriteCount;
            return true;
        } else if (openLedgers.isEmpty()) {
            System.out.format("Not starting a write, no open ledgers, already opening the limit%n");
            /* Caller should release lock and wait for openLedgers to be populated */
            return false;
        } else {
            LedgerInfo ledger = getRandomLedger(openLedgers);
            ArrayList<EntryInfo> toWrite = ledger.getNextEntries(targetWriteGroup);
            long lastEntry = toWrite.get(toWrite.size() - 1).getEntryID();
            System.out.format(
                    "Writing entries %d -> %d to ledger %d%n",
                    toWrite.get(0).getEntryID(),
                    lastEntry,
                    ledger.ledgerID);
            ledger.openWrite(lastEntry);

            WriteCallback writeCB = new WriteCallback(
                    ledger, lastEntry, ledger.getLastEntryIDCompleted(), toWrite.size());
            for (EntryInfo entry: toWrite) {
                driver.writeEntry(ledger.ledgerID, entry.getEntryID(), entry.getBuffer(), writeCB);
            }
            ++outstandingWriteCount;

            if (lastEntry >= targetLedgerEntries) {
                /* Remove this ledger from the writable list, mark for closing once all open writes complete */
                System.out.format("Marking ledger %d for close%n", ledger.ledgerID);
                openLedgers.remove(ledger);
                liveLedgers.add(ledger);
                ledger.onLastWriteComplete((rc) -> checkReturn(ledger.ledgerID, rc), (Consumer<Integer> cb) -> {
                    System.out.format("Closing ledger %d%n", ledger.ledgerID);
                    driver.closeLedger(ledger.ledgerID, (Integer rc) -> {
                        synchronized (BookkeeperVerifier.this) {
                            ledger.setClosed();
                            System.out.format("Closed ledger %d%n", ledger.ledgerID);

                            if (liveLedgers.size() >= targetLedgers) {
                                /* We've closed the ledger, but now we have too many closed but readable ledgers,
                                 * start deleting one. */
                                LedgerInfo toDelete = getRandomLedger(liveLedgers);
                                final long ledgerID = toDelete.ledgerID;
                                System.out.format("Marking ledger %d for deletion%n", ledgerID);
                                liveLedgers.remove(toDelete);
                                toDelete.onLastOpComplete(cb, (Consumer<Integer> cb2) -> {
                                    System.out.format("Deleting ledger %d%n", ledgerID);
                                    driver.deleteLedger(ledgerID, (rc2) -> {
                                        synchronized (BookkeeperVerifier.this) {
                                            System.out.format("Deleted ledger %d%n", ledgerID);
                                            cb2.accept(rc2);
                                        }
                                    });
                                });
                            } else {
                                cb.accept(rc);
                            }
                        }
                    });
                });
            }

            Collections.shuffle(toWrite);
            return true;
        }
    }

    /**
     * This is the method used to invoke the main loop of the IO driver.  run() will loop
     * starting IO requests until the time runs out on the test and all outstanding requests
     * complete.  Test execution state is accessed only under the instance lock for 'this'.
     * There is no fine grained locking, hence run() simply needs to be synchronized and
     * can wait for IOs to complete atomically with startWrite and startRead returning
     * false (see those comments).
     *
     * @throws Exception
     */
    public synchronized void run() throws Exception {
        long start = System.currentTimeMillis();
        long testEnd = start + (duration * 1000);
        long testDrainEnd = testEnd + (drainTimeout * 1000);

        /* Keep IO running until testEnd */
        while (System.currentTimeMillis() < testEnd) {

            /* see startRead and startWrite, they return false once no more IO can be started */
            while (startRead() || startWrite()) {}
            long toWait = testEnd - System.currentTimeMillis();

            /* atomically wait for either IO to complete or the test to end */
            this.wait(toWait < 0 ? 0 : toWait);
            printThrowExceptions();
        }

        /* Wait for all in progress ops to complete, outstanding*Count is updated under the lock */
        while ((System.currentTimeMillis() < testDrainEnd)
               && (outstandingReadCount > 0 || outstandingWriteCount > 0)) {
            System.out.format("reads: %d, writes: %d%n", outstandingReadCount, outstandingWriteCount);
            System.out.format("openingLedgers:%n");
            for (LedgerInfo li: openingLedgers) {
                System.out.format(
                        "Ledger %d has reads: %d, writes: %d%n",
                        li.ledgerID,
                        li.readsInProgress,
                        li.writesInProgress.size());
            }
            System.out.format("openLedgers:%n");
            for (LedgerInfo li: openLedgers) {
                System.out.format(
                        "Ledger %d has reads: %d, writes: %d%n",
                        li.ledgerID,
                        li.readsInProgress,
                        li.writesInProgress.size());
            }
            System.out.format("liveLedgers:%n");
            for (LedgerInfo li: liveLedgers) {
                System.out.format(
                        "Ledger %d has reads: %d, writes: %d%n",
                        li.ledgerID,
                        li.readsInProgress,
                        li.writesInProgress.size());
            }
            long toWait = testDrainEnd - System.currentTimeMillis();
            this.wait(toWait < 0 ? 0 : toWait);
            printThrowExceptions();
        }
        if (outstandingReadCount > 0 || outstandingWriteCount > 0) {
            throw new Exception("Failed to drain ops before timeout%n");
        }
    }
}
