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
package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.DefaultEntryLogger;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.EntryLogScanner;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;

/**
 * Scan all entries in the journal and entry log files then rebuilds the ledgers index.
 * Notable stuff:
 * - Fences every ledger as even if we check the metadata, we cannot guarantee that
 *   a fence request was served while the rebuild was taking place (even if the bookie
 *   is running in read-only mode).
 *   Losing the fenced status of a ledger is UNSAFE.
 * - Sets the master key as an empty byte array. This is correct as empty master keys
 *   are overwritten and we cannot use the password from metadata, and cannot know 100%
 *   for sure how a digest for the password was generated.
 */
@CustomLog
public class LedgersIndexRebuildOp {

    private final ServerConfiguration conf;
    private final boolean verbose;
    private static final String LedgersSubPath = "ledgers";

    public LedgersIndexRebuildOp(ServerConfiguration conf, boolean verbose) {
        this.conf = conf;
        this.verbose = verbose;
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean initiate()  {
        log.info("Starting ledger index rebuilding");
        File[] indexDirs = conf.getIndexDirs();
        if (indexDirs == null) {
            indexDirs = conf.getLedgerDirs();
        }
        if (indexDirs.length != conf.getLedgerDirs().length) {
            log.error("ledger and index dirs size not matched");
            return false;
        }

        for (int i = 0; i < indexDirs.length; i++) {
            File indexDir = indexDirs[i];
            File ledgerDir = conf.getLedgerDirs()[i];

            String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date());
            String indexBasePath = BookieImpl.getCurrentDirectory(indexDir).toString();
            String tempLedgersSubPath = LedgersSubPath + ".TEMP-" + timestamp;
            Path indexTempPath = FileSystems.getDefault().getPath(indexBasePath, tempLedgersSubPath);
            Path indexCurrentPath = FileSystems.getDefault().getPath(indexBasePath, LedgersSubPath);

            log.info("Starting scan phase (scans journal and entry log files)");

            try {
                Set<Long> ledgers = new HashSet<>();
                scanJournals(ledgers);
                File[] lDirs = new File[1];
                lDirs[0] = ledgerDir;
                scanEntryLogFiles(ledgers, lDirs);

                log.info().attr("count", ledgers.size())
                        .log("Scan complete. Starting to build a new ledgers index");

                try (KeyValueStorage newIndex = KeyValueStorageRocksDB.factory.newKeyValueStorage(
                        indexBasePath, tempLedgersSubPath, DbConfigType.Default, conf)) {
                    log.info().attr("tempPath", indexTempPath).log("Created ledgers index at temp location");

                    for (Long ledgerId : ledgers) {
                        DbLedgerStorageDataFormats.LedgerData ledgerData =
                                DbLedgerStorageDataFormats.LedgerData.newBuilder()
                                        .setExists(true)
                                        .setFenced(true)
                                        .setMasterKey(ByteString.EMPTY).build();

                        byte[] ledgerArray = new byte[16];
                        ArrayUtil.setLong(ledgerArray, 0, ledgerId);
                        newIndex.put(ledgerArray, ledgerData.toByteArray());
                    }

                    newIndex.sync();
                }
            } catch (Throwable t) {
                log.error().exception(t).log("Error during rebuild, the original index remains unchanged");
                delete(indexTempPath);
                return false;
            }

            // replace the existing index
            try {
                Path prevPath = FileSystems.getDefault().getPath(indexBasePath,
                        LedgersSubPath + ".PREV-" + timestamp);
                log.info()
                        .attr("from", indexCurrentPath)
                        .attr("to", prevPath)
                        .log("Moving original index to back-up location");
                Files.move(indexCurrentPath, prevPath);
                log.info()
                        .attr("from", indexTempPath)
                        .attr("to", indexCurrentPath)
                        .log("Moving rebuilt index");
                Files.move(indexTempPath, indexCurrentPath);
                log.info().attr("backupPath", prevPath)
                        .log("Original index has been replaced with the new index");
            } catch (IOException e) {
                log.error().exception(e).log("Could not replace original index with rebuilt index. "
                        + "To return to the original state, ensure the original index is in its original location");
                return false;
            }
        }

        return true;
    }

    private void scanEntryLogFiles(Set<Long> ledgers, File[] lDirs) throws IOException {
        DefaultEntryLogger entryLogger = new DefaultEntryLogger(conf, new LedgerDirsManager(conf, lDirs,
                new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold())));
        Set<Long> entryLogs = entryLogger.getEntryLogsSet();

        int totalEntryLogs = entryLogs.size();
        int completedEntryLogs = 0;
        log.info().attr("count", totalEntryLogs).log("Scanning entry logs");

        for (long entryLogId : entryLogs) {
            entryLogger.scanEntryLog(entryLogId, new EntryLogScanner() {
                @Override
                public void process(long ledgerId, long offset, ByteBuf entry) throws IOException {
                    if (ledgers.add(ledgerId)) {
                        if (verbose) {
                            log.info().attr("ledgerId", ledgerId).log("Found ledger in entry log");
                        }
                    }
                }

                @Override
                public boolean accept(long ledgerId) {
                    return true;
                }
            });

            ++completedEntryLogs;
            log.info().attr("entryLogId", Long.toHexString(entryLogId))
                    .attr("completed", completedEntryLogs).attr("total", totalEntryLogs)
                    .log("Completed scanning of entry log");
        }
    }

    private void scanJournals(Set<Long> ledgers) throws IOException {
        for (Journal journal : getJournals(conf)) {
            List<Long> journalIds = Journal.listJournalIds(journal.getJournalDirectory(),
                    new Journal.JournalIdFilter() {
                @Override
                public boolean accept(long journalId) {
                    return true;
                }
            });

            for (Long journalId : journalIds) {
                scanJournal(journal, journalId, ledgers);
            }
        }
    }

    private List<Journal> getJournals(ServerConfiguration conf) throws IOException {
        List<Journal> journals = Lists.newArrayListWithCapacity(conf.getJournalDirs().length);
        int idx = 0;
        for (File journalDir : conf.getJournalDirs()) {
            journals.add(new Journal(idx++, new File(journalDir, BookKeeperConstants.CURRENT_DIR), conf,
                    new LedgerDirsManager(conf, conf.getLedgerDirs(),
                            new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()))));
        }

        return journals;
    }

    private void scanJournal(Journal journal, long journalId, Set<Long> ledgers) throws IOException {
        log.info()
                .attr("journalId", journalId)
                .attr("journalFile", Long.toHexString(journalId) + ".txn")
                .log("Scanning journal");
        journal.scanJournal(journalId, 0L, new Journal.JournalScanner() {
            @Override
            public void process(int journalVersion, long offset, ByteBuffer entry) {
                ByteBuf buf = Unpooled.wrappedBuffer(entry);
                long ledgerId = buf.readLong();

                if (ledgers.add(ledgerId) && verbose) {
                    log.info().attr("ledgerId", ledgerId).log("Found ledger in journal");
                }
            }
        }, false);
    }

    private void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            log.warn()
                    .attr("path", path.toAbsolutePath())
                    .exception(e)
                    .log("Unable to delete");
        }
    }
}
