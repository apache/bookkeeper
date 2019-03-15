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
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A command to convert bookie indexes from InterleavedStorage to DbLedgerStorage format.
 */
public class ConvertToDBStorageCommand extends BookieCommand<ConvertToDBStorageCommand.CTDBFlags> {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertToDBStorageCommand.class);
    private static final String NAME = "converttodbstorage";
    private static final String DESC = "Convert bookie indexes from InterleavedStorage to DbLedgerStorage format";
    private static final String NOT_INIT = "default formatter";

    @Setter
    private LedgerIdFormatter ledgerIdFormatter;

    public ConvertToDBStorageCommand() {
        this(new CTDBFlags());
    }
    public ConvertToDBStorageCommand(CTDBFlags flags) {
        super(CliSpec.<CTDBFlags>newBuilder().withFlags(flags).withName(NAME).withDescription(DESC).build());
    }

    /**
     * Flags for this command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class CTDBFlags extends CliFlags {
        @Parameter(names = { "-l", "--ledgeridformatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter = NOT_INIT;
    }

    @Override
    public boolean apply(ServerConfiguration conf, CTDBFlags cmdFlags) {
        initLedgerIdFormatter(conf, cmdFlags);
        try {
            return handle(conf);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean handle(ServerConfiguration conf) throws Exception {
        LOG.info("=== Converting to DbLedgerStorage ===");
        ServerConfiguration bkConf = new ServerConfiguration(conf);

        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        Bookie.mountLedgerStorageOffline(bkConf, interleavedStorage);

        DbLedgerStorage dbStorage = new DbLedgerStorage();
        Bookie.mountLedgerStorageOffline(bkConf, dbStorage);

        int convertedLedgers = 0;
        for (long ledgerId : interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Converting ledger {}", ledgerIdFormatter.formatLedgerId(ledgerId));
            }

            LedgerCache.LedgerIndexMetadata fi = interleavedStorage.readLedgerIndexMetadata(ledgerId);

            LedgerCache.PageEntriesIterable pages = interleavedStorage.getIndexEntries(ledgerId);

            long numberOfEntries = dbStorage.addLedgerToIndex(ledgerId, fi.fenced, fi.masterKey, pages);
            if (LOG.isDebugEnabled()) {
                LOG.debug("   -- done. fenced={} entries={}", fi.fenced, numberOfEntries);
            }

            // Remove index from old storage
            interleavedStorage.deleteLedger(ledgerId);

            if (++convertedLedgers % 1000 == 0) {
                LOG.info("Converted {} ledgers", convertedLedgers);
            }
        }

        dbStorage.shutdown();
        interleavedStorage.shutdown();

        LOG.info("---- Done Converting ----");
        return true;
    }

    private void initLedgerIdFormatter(ServerConfiguration conf, CTDBFlags flags) {
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
