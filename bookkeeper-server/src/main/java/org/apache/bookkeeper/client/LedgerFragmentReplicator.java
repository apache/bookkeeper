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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the helper class for replicating the fragments from one bookie to
 * another
 */
public class LedgerFragmentReplicator {

    // BookKeeper instance
    private BookKeeper bkc;

    public LedgerFragmentReplicator(BookKeeper bkc) {
        this.bkc = bkc;
    }

    private static Logger LOG = LoggerFactory
            .getLogger(LedgerFragmentReplicator.class);

    /**
     * This method replicate a ledger fragment which is a contiguous portion of
     * a ledger that was stored in an ensemble that included the failed bookie.
     * 
     * @param lh
     *            LedgerHandle for the ledger
     * @param lf
     *            LedgerFragment to replicate
     * @param ledgerFragmentMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param newBookie
     *            New bookie we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    void replicate(final LedgerHandle lh, final LedgerFragment lf,
                   final SingleFragmentCallback ledgerFragmentMcb,
                   final InetSocketAddress newBookie)
            throws InterruptedException {
        if (!lf.isClosed()) {
            LOG.error("Trying to replicate an unclosed fragment;"
                      + " This is not safe {}", lf);
            ledgerFragmentMcb.processResult(BKException.Code.UnclosedFragmentException,
                                            null, null);
            return;
        }
        Long startEntryId = lf.getFirstStoredEntryId();
        Long endEntryId = lf.getLastStoredEntryId();
        if (endEntryId == null) {
            /*
             * Ideally this should never happen if bookie failure is taken care
             * of properly. Nothing we can do though in this case.
             */
            LOG.warn("Dead bookie (" + lf.getAddress()
                    + ") is still part of the current"
                    + " active ensemble for ledgerId: " + lh.getId());
            ledgerFragmentMcb.processResult(BKException.Code.OK, null, null);
            return;
        }
        if (startEntryId > endEntryId) {
            // for open ledger which there is no entry, the start entry id is 0,
            // the end entry id is -1.
            // we can return immediately to trigger forward read
            ledgerFragmentMcb.processResult(BKException.Code.OK, null, null);
            return;
        }

        /*
         * Add all the entries to entriesToReplicate list from
         * firstStoredEntryId to lastStoredEntryID.
         */
        List<Long> entriesToReplicate = new LinkedList<Long>();
        long lastStoredEntryId = lf.getLastStoredEntryId();
        for (long i = lf.getFirstStoredEntryId(); i <= lastStoredEntryId; i++) {
            entriesToReplicate.add(i);
        }
        /*
         * Now asynchronously replicate all of the entries for the ledger
         * fragment that were on the dead bookie.
         */
        MultiCallback ledgerFragmentEntryMcb = new MultiCallback(
                entriesToReplicate.size(), ledgerFragmentMcb, null, BKException.Code.OK,
                BKException.Code.LedgerRecoveryException);
        for (final Long entryId : entriesToReplicate) {
            recoverLedgerFragmentEntry(entryId, lh, ledgerFragmentEntryMcb,
                    newBookie);
        }
    }

    /**
     * This method asynchronously recovers a specific ledger entry by reading
     * the values via the BookKeeper Client (which would read it from the other
     * replicas) and then writing it to the chosen new bookie.
     * 
     * @param entryId
     *            Ledger Entry ID to recover.
     * @param lh
     *            LedgerHandle for the ledger
     * @param ledgerFragmentEntryMcb
     *            MultiCallback to invoke once we've recovered the current
     *            ledger entry.
     * @param newBookie
     *            New bookie we want to use to recover and replicate the ledger
     *            entries that were stored on the failed bookie.
     */
    private void recoverLedgerFragmentEntry(final Long entryId,
            final LedgerHandle lh,
            final AsyncCallback.VoidCallback ledgerFragmentEntryMcb,
            final InetSocketAddress newBookie) throws InterruptedException {
        /*
         * Read the ledger entry using the LedgerHandle. This will allow us to
         * read the entry from one of the other replicated bookies other than
         * the dead one.
         */
        lh.asyncReadEntries(entryId, entryId, new ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh,
                    Enumeration<LedgerEntry> seq, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("BK error reading ledger entry: " + entryId,
                            BKException.create(rc));
                    ledgerFragmentEntryMcb.processResult(rc, null, null);
                    return;
                }
                /*
                 * Now that we've read the ledger entry, write it to the new
                 * bookie we've selected.
                 */
                LedgerEntry entry = seq.nextElement();
                byte[] data = entry.getEntry();
                ChannelBuffer toSend = lh.getDigestManager()
                        .computeDigestAndPackageForSending(entryId,
                                lh.getLastAddConfirmed(), entry.getLength(),
                                data, 0, data.length);
                bkc.getBookieClient().addEntry(newBookie, lh.getId(),
                        lh.getLedgerKey(), entryId, toSend,
                        new WriteCallback() {
                            @Override
                            public void writeComplete(int rc, long ledgerId,
                                    long entryId, InetSocketAddress addr,
                                    Object ctx) {
                                if (rc != Code.OK.intValue()) {
                                    LOG.error(
                                            "BK error writing entry for ledgerId: "
                                                    + ledgerId + ", entryId: "
                                                    + entryId + ", bookie: "
                                                    + addr, BKException
                                                    .create(rc));
                                } else {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Success writing ledger id "
                                                + ledgerId + ", entry id "
                                                + entryId + " to a new bookie "
                                                + addr + "!");
                                    }
                                }
                                /*
                                 * Pass the return code result up the chain with
                                 * the parent callback.
                                 */
                                ledgerFragmentEntryMcb.processResult(rc, null,
                                        null);
                            }
                        }, null, BookieProtocol.FLAG_RECOVERY_ADD);
            }
        }, null);
    }
    
    /**
     * Callback for recovery of a single ledger fragment. Once the fragment has
     * had all entries replicated, update the ensemble in zookeeper. Once
     * finished propogate callback up to ledgerFragmentsMcb which should be a
     * multicallback responsible for all fragments in a single ledger
     */
    static class SingleFragmentCallback implements AsyncCallback.VoidCallback {
        final AsyncCallback.VoidCallback ledgerFragmentsMcb;
        final LedgerHandle lh;
        final long fragmentStartId;
        final InetSocketAddress oldBookie;
        final InetSocketAddress newBookie;

        SingleFragmentCallback(AsyncCallback.VoidCallback ledgerFragmentsMcb,
                LedgerHandle lh, long fragmentStartId,
                InetSocketAddress oldBookie, InetSocketAddress newBookie) {
            this.ledgerFragmentsMcb = ledgerFragmentsMcb;
            this.lh = lh;
            this.fragmentStartId = fragmentStartId;
            this.newBookie = newBookie;
            this.oldBookie = oldBookie;
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc != Code.OK.intValue()) {
                LOG.error("BK error replicating ledger fragments for ledger: "
                        + lh.getId(), BKException.create(rc));
                ledgerFragmentsMcb.processResult(rc, null, null);
                return;
            }
            writeLedgerConfig();
        }

        protected void writeLedgerConfig() {
            /*
             * Update the ledger metadata's ensemble info to point to the new
             * bookie.
             */
            ArrayList<InetSocketAddress> ensemble = lh.getLedgerMetadata()
                    .getEnsembles().get(fragmentStartId);
            int deadBookieIndex = ensemble.indexOf(oldBookie);
            ensemble.remove(deadBookieIndex);
            ensemble.add(deadBookieIndex, newBookie);
            lh.writeLedgerConfig(new WriteCb());
        }

        private class WriteCb implements GenericCallback<Void> {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc == BKException.Code.MetadataVersionException) {
                    LOG
                            .warn("Two fragments attempted update at once; ledger id: "
                                    + lh.getId()
                                    + " startid: "
                                    + fragmentStartId);
                    // try again, the previous success (with which this has
                    // conflicted)
                    // will have updated the stat
                    // other operations such as (addEnsemble) would update it
                    // too.
                    lh.rereadMetadata(new GenericCallback<LedgerMetadata>() {
                        @Override
                        public void operationComplete(int rc,
                                LedgerMetadata newMeta) {
                            if (rc != BKException.Code.OK) {
                                LOG
                                        .error("Error reading updated ledger metadata for ledger "
                                                + lh.getId());
                                ledgerFragmentsMcb
                                        .processResult(rc, null, null);
                            } else {
                                lh.metadata = newMeta;
                                writeLedgerConfig();
                            }
                        }
                    });
                    return;
                } else if (rc != BKException.Code.OK) {
                    LOG
                            .error("Error updating ledger config metadata for ledgerId "
                                    + lh.getId()
                                    + " : "
                                    + BKException.getMessage(rc));
                } else {
                    LOG
                            .info("Updated ZK for ledgerId: ("
                                    + lh.getId()
                                    + " : "
                                    + fragmentStartId
                                    + ") to point ledger fragments from old dead bookie: ("
                                    + oldBookie + ") to new bookie: ("
                                    + newBookie + ")");
                }
                /*
                 * Pass the return code result up the chain with the parent
                 * callback.
                 */
                ledgerFragmentsMcb.processResult(rc, null, null);
            }
        };
    }

}
