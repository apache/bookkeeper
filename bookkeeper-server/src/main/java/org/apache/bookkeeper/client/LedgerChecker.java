/**
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *Checks the complete ledger and finds the UnderReplicated fragments if any
 */
public class LedgerChecker {
    private static Logger LOG = LoggerFactory.getLogger(LedgerChecker.class);

    public final BookieClient bookieClient;

    static class InvalidFragmentException extends Exception {
        private static final long serialVersionUID = 1467201276417062353L;
    }

    /**
     * This will collect all the entry read call backs and finally it will give
     * call back to previous call back API which is waiting for it once it meets
     * the expected call backs from down
     */
    private static class ReadManyEntriesCallback implements ReadEntryCallback {
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

        public void readEntryComplete(int rc, long ledgerId, long entryId,
                ChannelBuffer buffer, Object ctx) {
            if (rc == BKException.Code.OK) {
                if (numEntries.decrementAndGet() == 0
                        && !completed.getAndSet(true)) {
                    cb.operationComplete(rc, fragment);
                }
            } else if (!completed.getAndSet(true)) {
                cb.operationComplete(rc, fragment);
            }
        }
    }

    public LedgerChecker(BookKeeper bkc) {
        bookieClient = bkc.getBookieClient();
    }

    private void verifyLedgerFragment(LedgerFragment fragment,
            GenericCallback<LedgerFragment> cb) throws InvalidFragmentException {
        long firstStored = fragment.getFirstStoredEntryId();
        long lastStored = fragment.getLastStoredEntryId();

        if (firstStored == LedgerHandle.INVALID_ENTRY_ID) {
            if (lastStored != LedgerHandle.INVALID_ENTRY_ID) {
                throw new InvalidFragmentException();
            }
            cb.operationComplete(BKException.Code.OK, fragment);
            return;
        }
        if (firstStored == lastStored) {
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(1,
                    fragment, cb);
            bookieClient.readEntry(fragment.getAddress(), fragment
                    .getLedgerId(), firstStored, manycb, null);
        } else {
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(2,
                    fragment, cb);
            bookieClient.readEntry(fragment.getAddress(), fragment
                    .getLedgerId(), firstStored, manycb, null);
            bookieClient.readEntry(fragment.getAddress(), fragment
                    .getLedgerId(), lastStored, manycb, null);
        }
    }

    /**
     * Callback for checking whether an entry exists or not.
     * It is used to differentiate the cases where it has been written
     * but now cannot be read, and where it never has been written.
     */
    private static class EntryExistsCallback implements ReadEntryCallback {
        AtomicBoolean entryMayExist = new AtomicBoolean(false);
        final AtomicInteger numReads;
        final GenericCallback<Boolean> cb;

        EntryExistsCallback(int numReads,
                            GenericCallback<Boolean> cb) {
            this.numReads = new AtomicInteger(numReads);
            this.cb = cb;
        }

        public void readEntryComplete(int rc, long ledgerId, long entryId,
                                      ChannelBuffer buffer, Object ctx) {
            if (rc != BKException.Code.NoSuchEntryException) {
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
     * meets the expected call backs from down
     */
    private static class FullLedgerCallback implements
            GenericCallback<LedgerFragment> {
        final Set<LedgerFragment> badFragments;
        final AtomicLong numFragments;
        final GenericCallback<Set<LedgerFragment>> cb;

        FullLedgerCallback(long numFragments,
                GenericCallback<Set<LedgerFragment>> cb) {
            badFragments = new HashSet<LedgerFragment>();
            this.numFragments = new AtomicLong(numFragments);
            this.cb = cb;
        }

        public void operationComplete(int rc, LedgerFragment result) {
            if (rc != BKException.Code.OK) {
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
    public void checkLedger(LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb) {
        // build a set of all fragment replicas
        final Set<LedgerFragment> fragments = new HashSet<LedgerFragment>();

        Long curEntryId = null;
        ArrayList<InetSocketAddress> curEnsemble = null;
        for (Map.Entry<Long, ArrayList<InetSocketAddress>> e : lh
                .getLedgerMetadata().getEnsembles().entrySet()) {
            if (curEntryId != null) {
                for (int i = 0; i < curEnsemble.size(); i++) {
                    fragments.add(new LedgerFragment(lh, curEntryId,
                            e.getKey() - 1, i));
                }
            }
            curEntryId = e.getKey();
            curEnsemble = e.getValue();
        }

        /* Checking the last segment of the ledger can be complicated in some cases.
         * In the case that the ledger is closed, we can just check the fragments of
         * the segment as normal, except in the case that no entry was ever written,
         * to the ledger, in which case we check no fragments.
         * In the case that the ledger is open, but enough entries have been written,
         * for lastAddConfirmed to be set above the start entry of the segment, we
         * can also check as normal.
         * However, if lastAddConfirmed cannot be trusted, such as when it's lower than
         * the first entry id, or not set at all, we cannot be sure if there has been
         * data written to the segment. For this reason, we have to send a read request
         * to the bookies which should have the first entry. If they respond with
         * NoSuchEntry we can assume it was never written. If they respond with anything
         * else, we must assume the entry has been written, so we run the check.
         */
        if (curEntryId != null && !(lh.getLedgerMetadata().isClosed() && lh.getLastAddConfirmed() < curEntryId)) {
            long lastEntry = lh.getLastAddConfirmed();

            if (lastEntry < curEntryId) {
                lastEntry = curEntryId;
            }

            final Set<LedgerFragment> finalSegmentFragments = new HashSet<LedgerFragment>();
            for (int i = 0; i < curEnsemble.size(); i++) {
                finalSegmentFragments.add(new LedgerFragment(lh, curEntryId,
                        lastEntry, i));
            }

            // Check for the case that no last confirmed entry has
            // been set.
            if (curEntryId == lastEntry) {
                final long entryToRead = curEntryId;

                EntryExistsCallback eecb
                    = new EntryExistsCallback(lh.getLedgerMetadata().getWriteQuorumSize(),
                                              new GenericCallback<Boolean>() {
                                                  public void operationComplete(int rc, Boolean result) {
                                                      if (result) {
                                                          fragments.addAll(finalSegmentFragments);
                                                      }
                                                      checkFragments(fragments, cb);
                                                  }
                                              });

                for (int bi : lh.getDistributionSchedule().getWriteSet(entryToRead)) {
                    InetSocketAddress addr = curEnsemble.get(bi);
                    bookieClient.readEntry(addr, lh.getId(),
                                           entryToRead, eecb, null);
                }
                return;
            } else {
                fragments.addAll(finalSegmentFragments);
            }
        }

        checkFragments(fragments, cb);
    }

    private void checkFragments(Set<LedgerFragment> fragments,
                                GenericCallback<Set<LedgerFragment>> cb) {
        if (fragments.size() == 0) { // no fragments to verify
            cb.operationComplete(BKException.Code.OK, fragments);
            return;
        }

        // verify all the collected fragment replicas
        FullLedgerCallback allFragmentsCb = new FullLedgerCallback(fragments
                .size(), cb);
        for (LedgerFragment r : fragments) {
            LOG.debug("Checking fragment {}", r);
            try {
                verifyLedgerFragment(r, allFragmentsCb);
            } catch (InvalidFragmentException ife) {
                LOG.error("Invalid fragment found : {}", r);
                allFragmentsCb.operationComplete(
                        BKException.Code.IncorrectParameterException, r);
            }
        }
    }
}
