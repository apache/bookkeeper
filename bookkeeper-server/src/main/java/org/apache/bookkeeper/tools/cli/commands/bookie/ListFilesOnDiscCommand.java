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
import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to list the files in JournalDirectories/LedgerDirectories/IndexDirectories.
 */
public class ListFilesOnDiscCommand extends BookieCommand<ListFilesOnDiscCommand.LFODFlags > {

    private static final String NAME = "listfilesondisc";
    private static final String DESC = "List the files in JournalDirectories/LedgerDirectories/IndexDirectories.";
    private static final Logger LOG = LoggerFactory.getLogger(ListFilesOnDiscCommand.class);

    public ListFilesOnDiscCommand() {
        this(new LFODFlags());
    }

    public ListFilesOnDiscCommand(LFODFlags flags) {
        super(CliSpec.<LFODFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for list files on disc command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class LFODFlags extends CliFlags {
        @Parameter(names = {"-txn", "--journal"}, description = "Print list of Journal Files")
        private boolean journal;

        @Parameter(names = {"-log", "--entrylog"}, description = "Print list of EntryLog Files")
        private boolean entrylog;

        @Parameter(names = {"-idx", "--index"}, description = "Print list of Index Files")
        private boolean index;
    }

    @Override
    public boolean apply(ServerConfiguration conf, LFODFlags cmdFlags) {
        try {
            return handler(conf, cmdFlags);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean handler(ServerConfiguration conf, LFODFlags cmd) throws IOException {
        if (cmd.journal) {
            File[] journalDirs = conf.getJournalDirs();
            List<File> journalFiles = BookieShell.listFilesAndSort(journalDirs, "txn");
            LOG.info("--------- Printing the list of Journal Files ---------");
            for (File journalFile : journalFiles) {
                LOG.info("{}", journalFile.getCanonicalPath());
            }
            LOG.info("");
        }
        if (cmd.entrylog) {
            File[] ledgerDirs = conf.getLedgerDirs();
            List<File> ledgerFiles = BookieShell.listFilesAndSort(ledgerDirs, "log");
            LOG.info("--------- Printing the list of EntryLog/Ledger Files ---------");
            for (File ledgerFile : ledgerFiles) {
                LOG.info("{}", ledgerFile.getCanonicalPath());
            }
            LOG.info("");
        }
        if (cmd.index) {
            File[] indexDirs = (conf.getIndexDirs() == null) ? conf.getLedgerDirs() : conf.getIndexDirs();
            List<File> indexFiles = BookieShell.listFilesAndSort(indexDirs, "idx");
            LOG.info("--------- Printing the list of Index Files ---------");
            for (File indexFile : indexFiles) {
                LOG.info("{}", indexFile.getCanonicalPath());
            }
        }
        return true;
    }
}
