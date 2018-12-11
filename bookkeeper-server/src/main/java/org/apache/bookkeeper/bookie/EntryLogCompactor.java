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

package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the basic entry log compactor to compact entry logs.
 * The compaction is done by scanning the old entry log file, copy the active ledgers to the
 * current entry logger and remove the old entry log when the scan is over.
 */
public class EntryLogCompactor extends AbstractLogCompactor {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogCompactor.class);

    final CompactionScannerFactory scannerFactory = new CompactionScannerFactory();
    final EntryLogger entryLogger;
    final CompactableLedgerStorage ledgerStorage;
    private final int maxOutstandingRequests;

    public EntryLogCompactor(
            ServerConfiguration conf,
            EntryLogger entryLogger,
            CompactableLedgerStorage ledgerStorage,
            LogRemovalListener logRemover) {
        super(conf, logRemover);
        this.maxOutstandingRequests = conf.getCompactionMaxOutstandingRequests();
        this.entryLogger = entryLogger;
        this.ledgerStorage = ledgerStorage;
    }

    @Override
    public boolean compact(EntryLogMetadata entryLogMeta) {
        try {
            entryLogger.scanEntryLog(entryLogMeta.getEntryLogId(),
                scannerFactory.newScanner(entryLogMeta));
            scannerFactory.flush();
            LOG.info("Removing entry log {} after compaction", entryLogMeta.getEntryLogId());
            logRemovalListener.removeEntryLog(entryLogMeta.getEntryLogId());
        } catch (LedgerDirsManager.NoWritableLedgerDirException nwlde) {
            LOG.warn("No writable ledger directory available, aborting compaction", nwlde);
            return false;
        } catch (IOException ioe) {
            // if compact entry log throws IOException, we don't want to remove that
            // entry log. however, if some entries from that log have been re-added
            // to the entry log, and the offset updated, it's ok to flush that
            LOG.error("Error compacting entry log. Log won't be deleted", ioe);
            return false;
        }
        return true;
    }

    /**
     * A scanner wrapper to check whether a ledger is alive in an entry log file.
     */
    class CompactionScannerFactory {
        List<EntryLocation> offsets = new ArrayList<EntryLocation>();

        EntryLogger.EntryLogScanner newScanner(final EntryLogMetadata meta) {

            return new EntryLogger.EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return meta.containsLedger(ledgerId);
                }

                @Override
                public void process(final long ledgerId, long offset, ByteBuf entry) throws IOException {
                    throttler.acquire(entry.readableBytes());

                    if (offsets.size() > maxOutstandingRequests) {
                        flush();
                    }
                    long entryId = entry.getLong(entry.readerIndex() + 8);

                    long newoffset = entryLogger.addEntry(ledgerId, entry);
                    offsets.add(new EntryLocation(ledgerId, entryId, newoffset));

                }
            };
        }

        void flush() throws IOException {
            if (offsets.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping entry log flushing, as there are no offset!");
                }
                return;
            }

            // Before updating the index, we want to wait until all the compacted entries are flushed into the
            // entryLog
            try {
                entryLogger.flush();
                ledgerStorage.updateEntriesLocations(offsets);
                ledgerStorage.flushEntriesLocationsIndex();
            } finally {
                offsets.clear();
            }
        }
    }
}
