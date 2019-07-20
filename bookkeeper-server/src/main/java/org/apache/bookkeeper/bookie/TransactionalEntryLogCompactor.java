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

package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.HardLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for compaction. Compaction is done in several transactional phases.
 * Phase 1: Scan old entry log and compact entries to a new .compacting log file.
 * Phase 2: Flush .compacting log to disk and it becomes .compacted log file when this completes.
 * Phase 3: Flush ledger cache and .compacted file becomes .log file when this completes. Remove old
 * entry log file afterwards.
 */
public class TransactionalEntryLogCompactor extends AbstractLogCompactor {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalEntryLogCompactor.class);

    final EntryLogger entryLogger;
    final CompactableLedgerStorage ledgerStorage;
    final List<EntryLocation> offsets = new ArrayList<>();

    // compaction log file suffix
    static final String COMPACTING_SUFFIX = ".log.compacting";
    // flushed compaction log file suffix
    static final String COMPACTED_SUFFIX = ".compacted";

    public TransactionalEntryLogCompactor(
            ServerConfiguration conf,
            EntryLogger entryLogger,
            CompactableLedgerStorage ledgerStorage,
            LogRemovalListener logRemover) {
        super(conf, logRemover);
        this.entryLogger = entryLogger;
        this.ledgerStorage = ledgerStorage;
    }

    /**
     * Delete all previously incomplete compacting logs and recover the index for compacted logs.
     */
    @Override
    public void cleanUpAndRecover() {
        // clean up compacting logs and recover index for already compacted logs
        List<File> ledgerDirs = entryLogger.getLedgerDirsManager().getAllLedgerDirs();
        for (File dir : ledgerDirs) {
            File[] compactingPhaseFiles = dir.listFiles(file -> file.getName().endsWith(COMPACTING_SUFFIX));
            if (compactingPhaseFiles != null) {
                for (File file : compactingPhaseFiles) {
                    if (file.delete()) {
                        LOG.info("Deleted failed compaction file {}", file);
                    }
                }
            }
            File[] compactedPhaseFiles = dir.listFiles(file -> file.getName().endsWith(COMPACTED_SUFFIX));
            if (compactedPhaseFiles != null) {
                for (File compactedFile : compactedPhaseFiles) {
                    LOG.info("Found compacted log file {} has partially flushed index, recovering index.",
                            compactedFile);
                    CompactionPhase updateIndex = new UpdateIndexPhase(compactedFile, true);
                    updateIndex.run();
                }
            }
        }
    }

    @Override
    public boolean compact(EntryLogMetadata metadata) {
        if (metadata != null) {
            LOG.info("Compacting entry log {} with usage {}.",
                metadata.getEntryLogId(), metadata.getUsage());
            CompactionPhase scanEntryLog = new ScanEntryLogPhase(metadata);
            if (!scanEntryLog.run()) {
                LOG.info("Compaction for entry log {} end in ScanEntryLogPhase.", metadata.getEntryLogId());
                return false;
            }
            File compactionLogFile = entryLogger.getCurCompactionLogFile();
            CompactionPhase flushCompactionLog = new FlushCompactionLogPhase(metadata.getEntryLogId());
            if (!flushCompactionLog.run()) {
                LOG.info("Compaction for entry log {} end in FlushCompactionLogPhase.", metadata.getEntryLogId());
                return false;
            }
            File compactedLogFile = getCompactedLogFile(compactionLogFile, metadata.getEntryLogId());
            CompactionPhase updateIndex = new UpdateIndexPhase(compactedLogFile);
            if (!updateIndex.run()) {
                LOG.info("Compaction for entry log {} end in UpdateIndexPhase.", metadata.getEntryLogId());
                return false;
            }
            LOG.info("Compacted entry log : {}.", metadata.getEntryLogId());
            return true;
        }
        return false;
    }

    /**
     * An abstract class that would be extended to be the actual transactional phases for compaction.
     */
    abstract static class CompactionPhase {
        private String phaseName = "";

        CompactionPhase(String phaseName) {
            this.phaseName = phaseName;
        }

        boolean run() {
            try {
                start();
                return complete();
            } catch (IOException e) {
                LOG.error("Encounter exception in compaction phase {}. Abort current compaction.", phaseName, e);
                abort();
            }
            return false;
        }

        abstract void start() throws IOException;

        abstract boolean complete() throws IOException;

        abstract void abort();

    }

    /**
     * Assume we're compacting entry log 1 to entry log 3.
     * The first phase is to scan entries in 1.log and copy them to compaction log file "3.log.compacting".
     * We'll try to allocate a new compaction log before scanning to make sure we have a log file to write.
     * If after scanning, there's no data written, it means there's no valid entries to be compacted,
     * so we can remove 1.log directly, clear the offsets and end the compaction.
     * Otherwise, we should move on to the next phase.
     *
     * <p>If anything failed in this phase, we should delete the compaction log and clean the offsets.
     */
    class ScanEntryLogPhase extends CompactionPhase {
        private final EntryLogMetadata metadata;

        ScanEntryLogPhase(EntryLogMetadata metadata) {
            super("ScanEntryLogPhase");
            this.metadata = metadata;
        }

        @Override
        void start() throws IOException {
            // scan entry log into compaction log and offset list
            entryLogger.createNewCompactionLog();
            entryLogger.scanEntryLog(metadata.getEntryLogId(), new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return metadata.containsLedger(ledgerId);
                }

                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    throttler.acquire(entry.readableBytes());
                    synchronized (TransactionalEntryLogCompactor.this) {
                        long lid = entry.getLong(entry.readerIndex());
                        long entryId = entry.getLong(entry.readerIndex() + 8);
                        if (lid != ledgerId || entryId < -1) {
                            LOG.warn("Scanning expected ledgerId {}, but found invalid entry "
                                    + "with ledgerId {} entryId {} at offset {}",
                                    ledgerId, lid, entryId, offset);
                            throw new IOException("Invalid entry found @ offset " + offset);
                        }
                        long newOffset = entryLogger.addEntryForCompaction(ledgerId, entry);
                        offsets.add(new EntryLocation(ledgerId, entryId, newOffset));

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Compact add entry : lid = {}, eid = {}, offset = {}",
                                    ledgerId, entryId, newOffset);
                        }
                    }
                }
            });
        }

        @Override
        boolean complete() {
            if (offsets.isEmpty()) {
                // no valid entries is compacted, delete entry log file
                LOG.info("No valid entry is found in entry log after scan, removing entry log now.");
                logRemovalListener.removeEntryLog(metadata.getEntryLogId());
                entryLogger.removeCurCompactionLog();
                return false;
            }
            return true;
        }

        @Override
        void abort() {
            offsets.clear();
            // since we haven't flushed yet, we only need to delete the unflushed compaction file.
            entryLogger.removeCurCompactionLog();
        }

    }

    /**
     * Assume we're compacting log 1 to log 3.
     * This phase is to flush the compaction log.
     * When this phase starts, there should be a compaction log file like "3.log.compacting"
     * When compaction log is flushed, in order to indicate this phase is completed,
     * a hardlink file "3.log.1.compacted" should be created, and "3.log.compacting" should be deleted.
     */
    class FlushCompactionLogPhase extends CompactionPhase {
        private final long compactingLogId;
        private File compactedLogFile;

        FlushCompactionLogPhase(long compactingLogId) {
            super("FlushCompactionLogPhase");
            this.compactingLogId = compactingLogId;
        }

        @Override
        void start() throws IOException {
            // flush the current compaction log.
            File compactionLogFile = entryLogger.getCurCompactionLogFile();
            if (compactionLogFile == null || !compactionLogFile.exists()) {
                throw new IOException("Compaction log doesn't exist during flushing");
            }
            entryLogger.flushCompactionLog();
        }

        @Override
        boolean complete() throws IOException {
            // create a hard link file named "x.log.y.compacted" for file "x.log.compacting".
            // where x is compactionLogId and y is compactingLogId.
            File compactionLogFile = entryLogger.getCurCompactionLogFile();
            if (compactionLogFile == null || !compactionLogFile.exists()) {
                LOG.warn("Compaction log doesn't exist any more after flush");
                return false;
            }
            compactedLogFile = getCompactedLogFile(compactionLogFile, compactingLogId);
            if (compactedLogFile != null && !compactedLogFile.exists()) {
                HardLink.createHardLink(compactionLogFile, compactedLogFile);
            }
            entryLogger.removeCurCompactionLog();
            return true;
        }

        @Override
        void abort() {
            offsets.clear();
            // remove compaction log file and its hardlink
            entryLogger.removeCurCompactionLog();
            if (compactedLogFile != null && compactedLogFile.exists()) {
                if (!compactedLogFile.delete()) {
                    LOG.warn("Could not delete compacted log file {}", compactedLogFile);
                }
            }
        }
    }

    /**
     * Assume we're compacting log 1 to log 3.
     * This phase is to update the entry locations and flush the index.
     * When the phase start, there should be a compacted file like "3.log.1.compacted",
     * where 3 is the new compaction logId and 1 is the old entry logId.
     * After the index the flushed successfully, a hardlink "3.log" file should be created,
     * and 3.log.1.compacted file should be deleted to indicate the phase is succeed.
     *
     * <p>This phase can also used to recover partially flushed index when we pass isInRecovery=true
     */
    class UpdateIndexPhase extends CompactionPhase {
        File compactedLogFile;
        File newEntryLogFile;
        private final boolean isInRecovery;

        public UpdateIndexPhase(File compactedLogFile) {
            this(compactedLogFile, false);
        }

        public UpdateIndexPhase(File compactedLogFile, boolean isInRecovery) {
            super("UpdateIndexPhase");
            this.compactedLogFile = compactedLogFile;
            this.isInRecovery = isInRecovery;
        }

        @Override
        void start() throws IOException {
            if (compactedLogFile != null && compactedLogFile.exists()) {
                File dir = compactedLogFile.getParentFile();
                String compactedFilename = compactedLogFile.getName();
                // create a hard link "x.log" for file "x.log.y.compacted"
                this.newEntryLogFile = new File(dir, compactedFilename.substring(0,
                            compactedFilename.indexOf(".log") + 4));
                if (!newEntryLogFile.exists()) {
                    HardLink.createHardLink(compactedLogFile, newEntryLogFile);
                }
                if (isInRecovery) {
                    recoverEntryLocations(EntryLogger.fileName2LogId(newEntryLogFile.getName()));
                }
                if (!offsets.isEmpty()) {
                    // update entry locations and flush index
                    ledgerStorage.updateEntriesLocations(offsets);
                    ledgerStorage.flushEntriesLocationsIndex();
                }
            } else {
                throw new IOException("Failed to find compacted log file in UpdateIndexPhase");
            }
        }

        @Override
        boolean complete() {
            // When index is flushed, and entry log is removed,
            // delete the ".compacted" file to indicate this phase is completed.
            offsets.clear();
            if (compactedLogFile != null) {
                if (!compactedLogFile.delete()) {
                    LOG.warn("Could not delete compacted log file {}", compactedLogFile);
                }
                // Now delete the old entry log file since it's compacted
                String compactedFilename = compactedLogFile.getName();
                String oldEntryLogFilename = compactedFilename.substring(compactedFilename.indexOf(".log") + 5);
                long entryLogId = EntryLogger.fileName2LogId(oldEntryLogFilename);
                logRemovalListener.removeEntryLog(entryLogId);
            }
            return true;
        }

        @Override
        void abort() {
            offsets.clear();
        }

        /**
         * Scan entry log to recover entry locations.
         */
        private void recoverEntryLocations(long compactedLogId) throws IOException {
            entryLogger.scanEntryLog(compactedLogId, new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return true;
                }

                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    long lid = entry.getLong(entry.readerIndex());
                    long entryId = entry.getLong(entry.readerIndex() + 8);
                    if (lid != ledgerId || entryId < -1) {
                        LOG.warn("Scanning expected ledgerId {}, but found invalid entry "
                                + "with ledgerId {} entryId {} at offset {}",
                                ledgerId, lid, entryId, offset);
                        throw new IOException("Invalid entry found @ offset " + offset);
                    }
                    long location = (compactedLogId << 32L) | (offset + 4);
                    offsets.add(new EntryLocation(lid, entryId, location));
                }
            });
            LOG.info("Recovered {} entry locations from compacted log {}", offsets.size(), compactedLogId);
        }
    }

    File getCompactedLogFile(File compactionLogFile, long compactingLogId) {
        if (compactionLogFile == null) {
            return null;
        }
        File dir = compactionLogFile.getParentFile();
        String filename = compactionLogFile.getName();
        String newSuffix = ".log." + EntryLogger.logId2HexString(compactingLogId) + COMPACTED_SUFFIX;
        String hardLinkFilename = filename.replace(COMPACTING_SUFFIX, newSuffix);
        return new File(dir, hardLinkFilename);
    }

}
