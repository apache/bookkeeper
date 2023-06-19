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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to scan a journal file and format the entries into readable format.
 */
public class ReadJournalCommand extends BookieCommand<ReadJournalCommand.ReadJournalFlags> {

    private static final String NAME = "readjournal";
    private static final String DESC = "Scan a journal file and format the entries into readable format.";
    private static final long DEFAULT_JOURNALID = -1L;
    private static final String DEFAULT = "";
    private LedgerIdFormatter ledgerIdFormatter;
    private EntryFormatter entryFormatter;
    private static final Logger LOG = LoggerFactory.getLogger(ReadJournalCommand.class);

    List<Journal> journals = null;

    public ReadJournalCommand() {
        this(new ReadJournalFlags());
    }

    public ReadJournalCommand(LedgerIdFormatter idFormatter, EntryFormatter entryFormatter) {
        this(new ReadJournalFlags());
        this.ledgerIdFormatter = idFormatter;
        this.entryFormatter = entryFormatter;
    }

    ReadJournalCommand(ReadJournalFlags flags) {
        super(CliSpec.<ReadJournalFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flag for read journal command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class ReadJournalFlags extends CliFlags {

        @Parameter(names = {"-m", "--msg"}, description = "Print message body")
        private boolean msg;

        @Parameter(names = { "-d", "--dir" }, description = "Journal directory (needed if more than one journal "
                                                                + "configured)")
        private String dir = DEFAULT;

        @Parameter(names = {"-id", "--journalid"}, description = "Journal Id")
        private long journalId = DEFAULT_JOURNALID;

        @Parameter(names = {"-f", "--filename"}, description = "Journal file name")
        private String fileName = DEFAULT;

        @Parameter(names = {"-l", "--ledgerIdFormatter"}, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;

        @Parameter(names = {"-e", "--entryformatter"}, description = "set entry formatter")
        private String entryFormatter = DEFAULT;

    }

    @Override
    public boolean apply(ServerConfiguration conf, ReadJournalFlags cmdFlags) {
        initTools(conf, cmdFlags);
        if (!checkArgs(cmdFlags)) {
            return false;
        }
        try {
            return handler(conf, cmdFlags);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private void initTools(ServerConfiguration conf, ReadJournalFlags flags) {
        if (!flags.ledgerIdFormatter.equals(DEFAULT)) {
            ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(flags.ledgerIdFormatter, conf);
        } else {
            ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }

        if (!flags.entryFormatter.equals(DEFAULT)) {
            entryFormatter = EntryFormatter.newEntryFormatter(flags.entryFormatter, conf);
        } else {
            entryFormatter = EntryFormatter.newEntryFormatter(conf);
        }
    }
    private boolean handler(ServerConfiguration conf, ReadJournalFlags cmd) throws IOException {
        Journal journal = null;
        if (getJournals(conf).size() > 1) {
            if (cmd.dir.equals(DEFAULT)) {
                LOG.error("ERROR: invalid or missing journal directory");
                usage();
                return false;
            }
            File journalDirectory = new File(cmd.dir);
            for (Journal j : getJournals(conf)) {
                if (j.getJournalDirectory().equals(journalDirectory)) {
                    journal = j;
                    break;
                }
            }

            if (journal == null) {
                LOG.error("ERROR: journal directory not found");
                usage();
                return false;
            }
        } else {
            journal = getJournals(conf).get(0);
        }

        long journalId = cmd.journalId;
        if (cmd.journalId == DEFAULT_JOURNALID && !cmd.fileName.equals(DEFAULT)) {
            File f = new File(cmd.fileName);
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                LOG.error("ERROR: invalid journal file name {}", cmd.fileName);
                usage();
                return false;
            }
            String idString = name.split("\\.")[0];
            journalId = Long.parseLong(idString, 16);
        }
        scanJournal(journal, journalId, cmd.msg);
        return true;
    }

    private boolean checkArgs(ReadJournalFlags flags) {
        if ((flags.fileName.equals(DEFAULT) && flags.journalId == DEFAULT_JOURNALID)) {
            LOG.info("ERROR: You should figure jounalId or journal filename");
            return false;
        }

        return true;
    }

    private synchronized List<Journal> getJournals(ServerConfiguration conf) throws IOException {
        if (null == journals) {
            journals = Lists.newArrayListWithCapacity(conf.getJournalDirs().length);
            int idx = 0;
            for (File journalDir : conf.getJournalDirs()) {
                journals.add(new Journal(idx++, new File(journalDir, BookKeeperConstants.CURRENT_DIR), conf,
                         new LedgerDirsManager(conf, conf.getLedgerDirs(),
                               new DiskChecker(conf.getDiskUsageThreshold(), conf.getDiskUsageWarnThreshold()))));
            }
        }
        return journals;
    }

    /**
      * Scan a journal file.
      *
      * @param journalId Journal File Id
      * @param printMsg Whether printing the entry data.
      */
    private void scanJournal(Journal journal, long journalId, final boolean printMsg) throws IOException {
        LOG.info("Scan journal {} ({}.txn)", journalId, Long.toHexString(journalId));
        scanJournal(journal, journalId, new Journal.JournalScanner() {
            boolean printJournalVersion = false;

            @Override
            public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                if (!printJournalVersion) {
                    LOG.info("Journal Version : {}", journalVersion);
                    printJournalVersion = true;
                }
                FormatUtil
                    .formatEntry(offset, Unpooled.wrappedBuffer(entry), printMsg, ledgerIdFormatter, entryFormatter);
            }
        });
    }

    private void scanJournal(Journal journal, long journalId, Journal.JournalScanner scanner) throws IOException {
        journal.scanJournal(journalId, 0L, scanner, false);
    }
}
