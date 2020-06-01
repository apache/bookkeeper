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
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.ReadOnlyEntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLogMetadataCommand.ReadLogMetadataFlags;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to print metadata of entry log.
 */
public class ReadLogMetadataCommand extends BookieCommand<ReadLogMetadataFlags> {

    static final Logger LOG = LoggerFactory.getLogger(ReadLogMetadataCommand.class);

    private static final String NAME = "readlogmetadata";
    private static final String DESC = "Prints entrylog's metadata";

    private static final long DEFAULT_LOGID = -1L;
    private static final String DEFAULT_FILENAME = "";
    private static final String DEFAULT = "";

    private LedgerIdFormatter ledgerIdFormatter;

    EntryLogger entryLogger = null;

    public ReadLogMetadataCommand() {
        this(new ReadLogMetadataFlags());
    }

    public ReadLogMetadataCommand(LedgerIdFormatter ledgerIdFormatter) {
        this(new ReadLogMetadataFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
    }

    private ReadLogMetadataCommand(ReadLogMetadataFlags flags) {
        super(CliSpec.<ReadLogMetadataFlags>newBuilder()
                     .withName(NAME)
                     .withDescription(DESC)
                     .withFlags(flags)
                     .build());
    }

    /**
     * Flags for read log metadata command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class ReadLogMetadataFlags extends CliFlags {

        @Parameter(names = { "-l", "--logid" }, description = "Entry log id")
        private long logId;

        @Parameter(names = { "-f", "--filename" }, description = "Entry log filename")
        private String logFilename;

        @Parameter(names = { "-lf", "--ledgeridformatter" }, description = "Set ledger id formatter")
        private String ledgerIdFormatter = DEFAULT;

    }

    @Override
    public boolean apply(ServerConfiguration conf, ReadLogMetadataFlags cmdFlags) {
        if (!cmdFlags.ledgerIdFormatter.equals(DEFAULT) && ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
        } else if (ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }
        if (cmdFlags.logId == DEFAULT_LOGID && cmdFlags.logFilename.equals(DEFAULT_FILENAME)) {
            System.err.println("Missing entry log id or entry log file name");
            return false;
        }
        try {
            return readLogMetadata(conf, cmdFlags);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    public boolean readLogMetadata(ServerConfiguration conf, ReadLogMetadataFlags flags) throws IOException {
        long logid = DEFAULT_LOGID;
        if (flags.logId != DEFAULT_LOGID) {
            logid = flags.logId;
        } else if (!flags.logFilename.equals(DEFAULT_FILENAME)) {
            File f = new File(flags.logFilename);
            String name = f.getName();
            if (!name.endsWith(".log")) {
                LOG.error("ERROR: invalid entry log file name " + flags.logFilename);
                return false;
            }
            String idString = name.split("\\.")[0];
            logid = Long.parseLong(idString, 16);
        }

        printEntryLogMetadata(conf, logid);
        return true;
    }

    private void printEntryLogMetadata(ServerConfiguration conf, long logId) throws IOException {
        LOG.info("Print entryLogMetadata of entrylog {} ({}.log)", logId, Long.toHexString(logId));
        initEntryLogger(conf);
        EntryLogMetadata entryLogMetadata = entryLogger.getEntryLogMetadata(logId);
        entryLogMetadata.getLedgersMap().forEach((ledgerId, size) -> {
            LOG.info("--------- Lid={}, TotalSizeOfEntriesOfLedger={}  ---------",
                     ledgerIdFormatter.formatLedgerId(ledgerId), size);
        });
    }

    private synchronized void initEntryLogger(ServerConfiguration conf) throws IOException {
        // provide read only entry logger
        if (null == entryLogger) {
            entryLogger = new ReadOnlyEntryLogger(conf);
        }
    }
}
