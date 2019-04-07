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
 */
package org.apache.bookkeeper.tools.cli.commands.bookie;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A command to convert bookie indexes from DbLedgerStorage to InterleavedStorage format.
 */
public class ConvertToInterleavedStorageCommand extends BookieCommand<ConvertToInterleavedStorageCommand.CTISFlags> {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertToInterleavedStorageCommand.class);
    private static final String NAME = "converttointerleavedstorage";
    private static final String DESC = "Convert bookie indexes from DbLedgerStorage to InterleavedStorage format";
    private static final String NOT_INIT = "default formatter";

    @Setter
    private LedgerIdFormatter ledgerIdFormatter;

    public ConvertToInterleavedStorageCommand() {
        this(new CTISFlags());
    }

    public ConvertToInterleavedStorageCommand(CTISFlags flags) {
        super(CliSpec.<CTISFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for this command.
     */
    @Accessors(fluent = true)
    public static class CTISFlags extends CliFlags{

        @Parameter(names = { "-l", "--ledgeridformatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter = NOT_INIT;

    }

    @Override
    public boolean apply(ServerConfiguration conf, CTISFlags cmdFlags) {
        initLedgerIdFormatter(conf, cmdFlags);
        try {
            return handle(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean handle(ServerConfiguration bkConf) throws Exception {
        LOG.info("=== Converting DbLedgerStorage ===");
        ServerConfiguration conf = new ServerConfiguration(bkConf);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(bkConf, bkConf.getLedgerDirs(),
            new DiskChecker(bkConf.getDiskUsageThreshold(), bkConf.getDiskUsageWarnThreshold()));
        LedgerDirsManager ledgerIndexManager = new LedgerDirsManager(bkConf, bkConf.getLedgerDirs(),
            new DiskChecker(bkConf.getDiskUsageThreshold(), bkConf.getDiskUsageWarnThreshold()));

        DbLedgerStorage dbStorage = new DbLedgerStorage();
        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();

        CheckpointSource checkpointSource = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                return Checkpoint.MAX;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact) {}
        };
        Checkpointer checkpointer = new Checkpointer() {
            @Override
            public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {
                // No-op
            }

            @Override
            public void start() {
                // no-op
            }
        };

        dbStorage.initialize(conf, null, ledgerDirsManager, ledgerIndexManager, null,
            checkpointSource, checkpointer, NullStatsLogger.INSTANCE, PooledByteBufAllocator.DEFAULT);
        interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerIndexManager,
            null, checkpointSource, checkpointer, NullStatsLogger.INSTANCE, PooledByteBufAllocator.DEFAULT);
        LedgerCache interleavedLedgerCache = interleavedStorage.getLedgerCache();

        int convertedLedgers = 0;
        for (long ledgerId : dbStorage.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Converting ledger {}", ledgerIdFormatter.formatLedgerId(ledgerId));
            }

            interleavedStorage.setMasterKey(ledgerId, dbStorage.readMasterKey(ledgerId));
            if (dbStorage.isFenced(ledgerId)) {
                interleavedStorage.setFenced(ledgerId);
            }

            long lastEntryInLedger = dbStorage.getLastEntryInLedger(ledgerId);
            for (long entryId = 0; entryId <= lastEntryInLedger; entryId++) {
                try {
                    long location = dbStorage.getLocation(ledgerId, entryId);
                    if (location != 0L) {
                        interleavedLedgerCache.putEntryOffset(ledgerId, entryId, location);
                    }
                } catch (Bookie.NoEntryException e) {
                    // Ignore entry
                }
            }

            if (++convertedLedgers % 1000 == 0) {
                LOG.info("Converted {} ledgers", convertedLedgers);
            }
        }

        dbStorage.shutdown();

        interleavedLedgerCache.flushLedger(true);
        interleavedStorage.flush();
        interleavedStorage.shutdown();

        String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();

        // Rename databases and keep backup
        Files.move(FileSystems.getDefault().getPath(baseDir, "ledgers"),
            FileSystems.getDefault().getPath(baseDir, "ledgers.backup"));

        Files.move(FileSystems.getDefault().getPath(baseDir, "locations"),
            FileSystems.getDefault().getPath(baseDir, "locations.backup"));

        LOG.info("---- Done Converting {} ledgers ----", convertedLedgers);
        return true;
    }

    private void initLedgerIdFormatter(ServerConfiguration conf, CTISFlags flags) {
        if (this.ledgerIdFormatter != null) {
            return;
        }
        if (flags.ledgerIdFormatter.equals(NOT_INIT)) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        } else {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(flags.ledgerIdFormatter, conf);
        }
    }
}
