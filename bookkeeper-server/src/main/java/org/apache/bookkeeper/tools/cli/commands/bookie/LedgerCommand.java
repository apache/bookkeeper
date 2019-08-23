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
import java.io.IOException;
import java.util.function.Consumer;

import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to dump ledger index entries into readable format.
 */
public class LedgerCommand extends BookieCommand<LedgerCommand.LedgerFlags> {

    static final Logger LOG = LoggerFactory.getLogger(LedgerCommand.class);

    private static final String NAME = "ledger";
    private static final String DESC = "Dump ledger index entries into readable format";

    private LedgerIdFormatter ledgerIdFormatter;

    private Consumer<String> print = this::printInfoLine;

    public void setPrint(Consumer<String> print) {
        this.print = print;
    }

    public LedgerCommand() {
        this(new LedgerFlags());
    }

    public LedgerCommand(LedgerIdFormatter ledgerIdFormatter) {
        this(new LedgerFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    private LedgerCommand(LedgerFlags flags) {
        super(CliSpec.<LedgerFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for ledger command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class LedgerFlags extends CliFlags {

        @Parameter(names = { "-id", "--ledgerId" }, description = "Specific ledger id", required = true)
        private long ledgerId;

        @Parameter(names = { "-m", "--meta" }, description = "Print meta information")
        private boolean meta;

        @Parameter(names = { "-l", "--ledgeridformatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter = "";
    }

    @Override
    public boolean apply(ServerConfiguration conf, LedgerFlags cmdFlags) {
        initLedgerIdFormatter(conf, cmdFlags);
        long ledgerId = cmdFlags.ledgerId;
        if (conf.getLedgerStorageClass().equals(DbLedgerStorage.class.getName())) {
            // dump ledger info
            return dumpLedgerInfo(ledgerId, conf);
        } else if (conf.getLedgerStorageClass().equals(SortedLedgerStorage.class.getName())
                || conf.getLedgerStorageClass().equals(InterleavedLedgerStorage.class.getName())) {
            ServerConfiguration tConf = new ServerConfiguration(conf);
            InterleavedLedgerStorage interleavedLedgerStorage = new InterleavedLedgerStorage();
            try {
                Bookie.mountLedgerStorageOffline(tConf, interleavedLedgerStorage);
            } catch (IOException e) {
                throw new UncheckedExecutionException(e.getMessage(), e);
            }

            if (cmdFlags.meta) {
                // print meta
                printMeta(ledgerId, interleavedLedgerStorage);
            }

            try {
                print.accept("===== LEDGER: " + ledgerIdFormatter.formatLedgerId(ledgerId) + " =====");
                for (LedgerCache.PageEntries page : interleavedLedgerStorage.getIndexEntries(ledgerId)) {
                    if (printPageEntries(page)) {
                        return true;
                    }
                }
            } catch (IOException e) {
                LOG.error("Failed to read index page");
                return true;
            }

        }
        return true;
    }

    private void initLedgerIdFormatter(ServerConfiguration conf, LedgerFlags flags) {
        if (flags.ledgerIdFormatter.equals("")) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        } else {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(flags.ledgerIdFormatter, conf);
        }
    }

    private boolean dumpLedgerInfo(long ledgerId, ServerConfiguration conf) {
        try {
            DbLedgerStorage.readLedgerIndexEntries(ledgerId, conf, (currentEntry, entryLodId, position) -> System.out
                    .println("entry " + currentEntry + "\t:\t(log: " + entryLodId + ", pos: " + position + ")"));
        } catch (IOException e) {
            System.err.printf("ERROR: initializing dbLedgerStorage %s", e.getMessage());
            return false;
        }
        return true;
    }

    private void printMeta(long ledgerId, InterleavedLedgerStorage interleavedLedgerStorage) {
        print.accept("===== LEDGER: " + ledgerIdFormatter.formatLedgerId(ledgerId) + " =====");
        try {
            LedgerCache.LedgerIndexMetadata meta = interleavedLedgerStorage.readLedgerIndexMetadata(ledgerId);
            print.accept("master key  : " + meta.getMasterKeyHex());
            long size = meta.size;
            if (size % 8 == 0) {
                print.accept("size         : " + size);
            } else {
                print.accept("size : " + size + "(not aligned with 8, may be corrupted or under flushing now)");
            }

            print.accept("entries      : " + (size / 8));
            print.accept("isFenced     : " + meta.fenced);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean printPageEntries(LedgerCache.PageEntries page) {
        final MutableLong curEntry = new MutableLong(page.getFirstEntry());
        try (LedgerEntryPage lep = page.getLEP()) {
            lep.getEntries((entry, offset) -> {
                while (curEntry.longValue() < entry) {
                    print.accept("entry " + curEntry + "\t:\tN/A");
                    curEntry.increment();
                }
                long entryLogId = offset >> 32L;
                long pos = offset & 0xffffffffL;
                print.accept("entry " + curEntry + "\t:\t(log:" + entryLogId + ", pos: " + pos + ")");
                curEntry.increment();
                return true;
            });
        } catch (Exception e) {
            print.accept(
                    "Failed to read index page @ " + page.getFirstEntry() + ", the index file may be corrupted : " + e
                            .getMessage());
            return true;
        }

        while (curEntry.longValue() < page.getLastEntry()) {
            print.accept("entry " + curEntry + "\t:\tN/A");
            curEntry.increment();
        }

        return false;
    }


    private void printInfoLine(String mes) {
        System.out.println(mes);
    }
}
