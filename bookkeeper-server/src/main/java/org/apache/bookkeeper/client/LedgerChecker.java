/*
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

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to check the complete ledger and finds the UnderReplicated fragments if any.
 *
 * <p>NOTE: This class is tended to be used by this project only. External users should not rely on it directly.
 */
public class LedgerChecker {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerChecker.class);

    public final BookieClient bookieClient;
    public final BookieWatcher bookieWatcher;

    final Semaphore semaphore;

    static class InvalidFragmentException extends Exception {
        private static final long serialVersionUID = 1467201276417062353L;
    }

    /**
     * This will collect all the entry read call backs and finally it will give
     * call back to previous call back API which is waiting for it once it meets
     * the expected call backs from down.
     */
    private class ReadManyEntriesCallback implements ReadEntryCallback {
        AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicLong numEntries;
        final LedgerFragment fragment;
        final GenericCallback<LedgerFragment> cb;

        ReadManyEntriesCallback(long numEntries, LedgerFragment fragment,
                GenericCallback<LedgerFragment> cb) {
            this.numEntries = new AtomicLong(numEntries);
            this.fragment = fragment;
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                ByteBuf buffer, Object ctx) {
            releasePermit();
            if (rc == BKException.Code.OK) {
                if (numEntries.decrementAndGet() == 0
                        && !completed.getAndSet(true)) {
                    cb.operationComplete(rc, fragment);
                }
            } else if (!completed.getAndSet(true)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Read {}:{} from {} failed, the error code: {}", ledgerId, entryId, ctx, rc);
                }
                cb.operationComplete(rc, fragment);
            }
        }
    }

    /**
     * This will collect the bad bookies inside a ledger fragment.
     */
    private static class LedgerFragmentCallback implements GenericCallback<LedgerFragment> {

        private final LedgerFragment fragment;
        private final int bookieIndex;
        // bookie index -> return code
        private final Map<Integer, Integer> badBookies;
        private final AtomicInteger numBookies;
        private final GenericCallback<LedgerFragment> cb;

        LedgerFragmentCallback(LedgerFragment lf,
                               int bookieIndex,
                               GenericCallback<LedgerFragment> cb,
                               Map<Integer, Integer> badBookies,
                               AtomicInteger numBookies) {
            this.fragment = lf;
            this.bookieIndex = bookieIndex;
            this.cb = cb;
            this.badBookies = badBookies;
            this.numBookies = numBookies;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment lf) {
            if (BKException.Code.OK != rc) {
                synchronized (badBookies) {
                    badBookies.put(bookieIndex, rc);
                }
            }
            if (numBookies.decrementAndGet() == 0) {
                if (badBookies.isEmpty()) {
                    cb.operationComplete(BKException.Code.OK, fragment);
                } else {
                    int rcToReturn = BKException.Code.NoBookieAvailableException;
                    for (Map.Entry<Integer, Integer> entry : badBookies.entrySet()) {
                        rcToReturn = entry.getValue();
                        if (entry.getValue() == BKException.Code.ClientClosedException) {
                            break;
                        }
                    }
                    cb.operationComplete(rcToReturn,
                            fragment.subset(badBookies.keySet()));
                }
            }
        }
    }

    public LedgerChecker(BookKeeper bkc) {
        this(bkc.getBookieClient(), bkc.getBookieWatcher());
    }

    public LedgerChecker(BookieClient client, BookieWatcher watcher) {
        this(client, watcher, -1);
    }

    public LedgerChecker(BookKeeper bkc, int inFlightReadEntryNum) {
        this(bkc.getBookieClient(), bkc.getBookieWatcher(), inFlightReadEntryNum);
    }

    public LedgerChecker(BookieClient client, BookieWatcher watcher, int inFlightReadEntryNum) {
        bookieClient = client;
        bookieWatcher = watcher;
        if (inFlightReadEntryNum > 0) {
            semaphore = new Semaphore(inFlightReadEntryNum);
        } else {
            semaphore = null;
        }
    }

    /**
     * Acquires a permit from permit manager,
     * blocking until all are available.
     */
    public void acquirePermit() throws InterruptedException {
        if (null != semaphore) {
            semaphore.acquire(1);
        }
    }

    /**
     * Release a given permit.
     */
    public void releasePermit() {
        if (null != semaphore) {
            semaphore.release();
        }
    }

    /**
     * Verify a ledger fragment to collect bad bookies.
     *
     * @param fragment
     *          fragment to verify
     * @param cb
     *          callback
     * @throws InvalidFragmentException
     */
    private void verifyLedgerFragment(LedgerFragment fragment,
                                      GenericCallback<LedgerFragment> cb,
                                      Long percentageOfLedgerFragmentToBeVerified)
            throws InvalidFragmentException, BKException, InterruptedException {
        Set<Integer> bookiesToCheck = fragment.getBookiesIndexes();
        if (bookiesToCheck.isEmpty()) {
            cb.operationComplete(BKException.Code.OK, fragment);
            return;
        }

        AtomicInteger numBookies = new AtomicInteger(bookiesToCheck.size());
        Map<Integer, Integer> badBookies = new HashMap<Integer, Integer>();
        for (Integer bookieIndex : bookiesToCheck) {
            LedgerFragmentCallback lfCb = new LedgerFragmentCallback(
                    fragment, bookieIndex, cb, badBookies, numBookies);
            verifyLedgerFragment(fragment, bookieIndex, lfCb, percentageOfLedgerFragmentToBeVerified);
        }
    }

    /**
     * Verify a bookie inside a ledger fragment.
     *
     * @param fragment
     *          ledger fragment
     * @param bookieIndex
     *          bookie index in the fragment
     * @param cb
     *          callback
     * @throws InvalidFragmentException
     */
    private void verifyLedgerFragment(LedgerFragment fragment,
                                      int bookieIndex,
                                      GenericCallback<LedgerFragment> cb,
                                      long percentageOfLedgerFragmentToBeVerified)
            throws InvalidFragmentException, InterruptedException {
        long firstStored = fragment.getFirstStoredEntryId(bookieIndex);
        long lastStored = fragment.getLastStoredEntryId(bookieIndex);

        BookieId bookie = fragment.getAddress(bookieIndex);
        if (null == bookie) {
            throw new InvalidFragmentException();
        }

        if (firstStored == LedgerHandle.INVALID_ENTRY_ID) {
            // this fragment is not on this bookie
            if (lastStored != LedgerHandle.INVALID_ENTRY_ID) {
                throw new InvalidFragmentException();
            }

            if (bookieWatcher.isBookieUnavailable(fragment.getAddress(bookieIndex))) {
                // fragment is on this bookie, but already know it's unavailable, so skip the call
                cb.operationComplete(BKException.Code.BookieHandleNotAvailableException, fragment);
            } else {
                cb.operationComplete(BKException.Code.OK, fragment);
            }
        } else if (bookieWatcher.isBookieUnavailable(fragment.getAddress(bookieIndex))) {
            // fragment is on this bookie, but already know it's unavailable, so skip the call
            cb.operationComplete(BKException.Code.BookieHandleNotAvailableException, fragment);
        } else if (firstStored == lastStored) {
            acquirePermit();
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(1,
                    fragment, cb);
            bookieClient.readEntry(bookie, fragment.getLedgerId(), firstStored,
                                   manycb, bookie, BookieProtocol.FLAG_NONE);
        } else {
            if (lastStored <= firstStored) {
                cb.operationComplete(Code.IncorrectParameterException, null);
                return;
            }

            long lengthOfLedgerFragment = lastStored - firstStored + 1;

            int numberOfEntriesToBeVerified =
                (int) (lengthOfLedgerFragment * (percentageOfLedgerFragmentToBeVerified / 100.0));

            TreeSet<Long> entriesToBeVerified = new TreeSet<Long>();

            if (numberOfEntriesToBeVerified < lengthOfLedgerFragment) {
                // Evenly pick random entries over the length of the fragment
                if (numberOfEntriesToBeVerified > 0) {
                    int lengthOfBucket = (int) (lengthOfLedgerFragment / numberOfEntriesToBeVerified);
                    for (long index = firstStored;
                         index < (lastStored - lengthOfBucket - 1);
                         index += lengthOfBucket) {
                        long potentialEntryId = ThreadLocalRandom.current().nextInt((lengthOfBucket)) + index;
                        if (fragment.isStoredEntryId(potentialEntryId, bookieIndex)) {
                            entriesToBeVerified.add(potentialEntryId);
                        }
                    }
                }
                entriesToBeVerified.add(firstStored);
                entriesToBeVerified.add(lastStored);
            } else {
                // Verify the entire fragment
                while (firstStored <= lastStored) {
                    if (fragment.isStoredEntryId(firstStored, bookieIndex)) {
                        entriesToBeVerified.add(firstStored);
                    }
                    firstStored++;
                }
            }
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(entriesToBeVerified.size(),
                    fragment, cb);
            for (Long entryID: entriesToBeVerified) {
                acquirePermit();
                bookieClient.readEntry(bookie, fragment.getLedgerId(), entryID, manycb, bookie,
                        BookieProtocol.FLAG_NONE);
            }
        }
    }

    /**
     * Callback for checking whether an entry exists or not.
     * It is used to differentiate the cases where it has been written
     * but now cannot be read, and where it never has been written.
     */
    private class EntryExistsCallback implements ReadEntryCallback {
        AtomicBoolean entryMayExist = new AtomicBoolean(false);
        final AtomicInteger numReads;
        final GenericCallback<Boolean> cb;

        EntryExistsCallback(int numReads,
                            GenericCallback<Boolean> cb) {
            this.numReads = new AtomicInteger(numReads);
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                                      ByteBuf buffer, Object ctx) {
            releasePermit();
            if (BKException.Code.NoSuchEntryException != rc && BKException.Code.NoSuchLedgerExistsException != rc
                    && BKException.Code.NoSuchLedgerExistsOnMetadataServerException != rc) {
                entryMayExist.set(true);
            }

            if (numReads.decrementAndGet() == 0) {
                cb.operationComplete(rc, entryMayExist.get());
            }
        }
    }

    /**
     * This will collect all the fragment read call backs and finally it will
     * give call back to above call back API which is waiting for it once it
     * meets the expected call backs from down.
     */
    private static class FullLedgerCallback implements
            GenericCallback<LedgerFragment> {
        final Set<LedgerFragment> badFragments;
        final AtomicLong numFragments;
        final GenericCallback<Set<LedgerFragment>> cb;

        FullLedgerCallback(long numFragments,
                GenericCallback<Set<LedgerFragment>> cb) {
            badFragments = new LinkedHashSet<>();
            this.numFragments = new AtomicLong(numFragments);
            this.cb = cb;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment result) {
            if (rc == BKException.Code.ClientClosedException) {
                cb.operationComplete(BKException.Code.ClientClosedException, badFragments);
                return;
            } else if (rc != BKException.Code.OK) {
                badFragments.add(result);
            }
            if (numFragments.decrementAndGet() == 0) {
                cb.operationComplete(BKException.Code.OK, badFragments);
            }
        }
    }

    /**
     * Check that all the fragments in the passed in ledger, and report those
     * which are missing.
     */
    public void checkLedger(final LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb) {
        checkLedger(lh, cb, 0L);
    }

    public void checkLedger(final LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb,
                            long percentageOfLedgerFragmentToBeVerified) {
        // build a set of all fragment replicas
        final Set<LedgerFragment> fragments = new LinkedHashSet<>();

        Long curEntryId = null;
        List<BookieId> curEnsemble = null;
        for (Map.Entry<Long, ? extends List<BookieId>> e : lh
                .getLedgerMetadata().getAllEnsembles().entrySet()) {
            if (curEntryId != null) {
                Set<Integer> bookieIndexes = new HashSet<Integer>();
                for (int i = 0; i < curEnsemble.size(); i++) {
                    bookieIndexes.add(i);
                }
                fragments.add(new LedgerFragment(lh, curEntryId,
                        e.getKey() - 1, bookieIndexes));
            }
            curEntryId = e.getKey();
            curEnsemble = e.getValue();
        }

        /* Checking the last segment of the ledger can be complicated in some cases.
         * In the case that the ledger is closed, we can just check the fragments of
         * the segment as normal even if no data has ever been written to.
         * In the case that the ledger is open, but enough entries have been written,
         * for lastAddConfirmed to be set above the start entry of the segment, we
         * can also check as normal.
         * However, if ledger is open, sometimes lastAddConfirmed cannot be trusted,
         * such as when it's lower than the first entry id, or not set at all,
         * we cannot be sure if there has been data written to the segment.
         * For this reason, we have to send a read request
         * to the bookies which should have the first entry. If they respond with
         * NoSuchEntry we can assume it was never written. If they respond with anything
         * else, we must assume the entry has been written, so we run the check.
         */
        if (curEntryId != null) {
            long lastEntry = lh.getLastAddConfirmed();

            if (!lh.isClosed() && lastEntry < curEntryId) {
                lastEntry = curEntryId;
            }

            Set<Integer> bookieIndexes = new HashSet<Integer>();
            for (int i = 0; i < curEnsemble.size(); i++) {
                bookieIndexes.add(i);
            }
            final LedgerFragment lastLedgerFragment = new LedgerFragment(lh, curEntryId,
                    lastEntry, bookieIndexes);

            // Check for the case that no last confirmed entry has been set
            if (curEntryId == lastEntry) {
                final long entryToRead = curEntryId;

                final CompletableFuture<Void> future = new CompletableFuture<>();
                future.whenCompleteAsync((re, ex) -> {
                    checkFragments(fragments, cb, percentageOfLedgerFragmentToBeVerified);
                });

                final EntryExistsCallback eecb = new EntryExistsCallback(lh.getLedgerMetadata().getWriteQuorumSize(),
                                              new GenericCallback<Boolean>() {
                                                  @Override
                                                  public void operationComplete(int rc, Boolean result) {
                                                      if (result) {
                                                          fragments.add(lastLedgerFragment);
                                                      }
                                                      future.complete(null);
                                                  }
                                              });

                DistributionSchedule ds = lh.getDistributionSchedule();
                for (int i = 0; i < ds.getWriteQuorumSize(); i++) {
                    try {
                        acquirePermit();
                        BookieId addr = curEnsemble.get(ds.getWriteSetBookieIndex(entryToRead, i));
                        bookieClient.readEntry(addr, lh.getId(), entryToRead,
                                eecb, null, BookieProtocol.FLAG_NONE);
                    } catch (InterruptedException e) {
                        LOG.error("InterruptedException when checking entry : {}", entryToRead, e);
                    }
                }
                return;
            } else {
                fragments.add(lastLedgerFragment);
            }
        }
        checkFragments(fragments, cb, percentageOfLedgerFragmentToBeVerified);
    }

    private void checkFragments(Set<LedgerFragment> fragments,
                                GenericCallback<Set<LedgerFragment>> cb,
                                long percentageOfLedgerFragmentToBeVerified) {
        if (fragments.size() == 0) { // no fragments to verify
            cb.operationComplete(BKException.Code.OK, fragments);
            return;
        }

        // verify all the collected fragment replicas
        FullLedgerCallback allFragmentsCb = new FullLedgerCallback(fragments
                .size(), cb);
        for (LedgerFragment r : fragments) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking fragment {}", r);
            }
            try {
                verifyLedgerFragment(r, allFragmentsCb, percentageOfLedgerFragmentToBeVerified);
            } catch (InvalidFragmentException ife) {
                LOG.error("Invalid fragment found : {}", r);
                allFragmentsCb.operationComplete(
                        BKException.Code.IncorrectParameterException, r);
            } catch (BKException e) {
                LOG.error("BKException when checking fragment : {}", r, e);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException when checking fragment : {}", r, e);
            }
        }
    }

}
