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
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.ReadOnlyEntryLogger;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.tools.cli.helpers.BookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.commons.lang.mutable.MutableBoolean;

/**
 * Command to read entry log files.
 */
public class ReadLogCommand extends BookieCommand<ReadLogCommand.ReadLogFlags> {

    private static final String NAME = "readlog";
    private static final String DESC = "Scan an entry file and format the entries into readable format.";

    private EntryLogger entryLogger;
    private EntryFormatter entryFormatter;
    private LedgerIdFormatter ledgerIdFormatter;

    public ReadLogCommand() {
        this(new ReadLogFlags());
    }

    public ReadLogCommand(LedgerIdFormatter ledgerIdFormatter, EntryFormatter entryFormatter) {
        this(new ReadLogFlags());
        this.ledgerIdFormatter = ledgerIdFormatter;
        this.entryFormatter = entryFormatter;
    }
    private ReadLogCommand(ReadLogFlags flags) {
        super(CliSpec.<ReadLogFlags>newBuilder().withName(NAME).withDescription(DESC).withFlags(flags).build());
    }

    /**
     * Flags for read log command.
     */
    @Accessors(fluent = true)
    @Setter
    public static class ReadLogFlags extends CliFlags {

        @Parameter(names = { "-m", "msg" }, description = "Print message body")
        private boolean msg;

        @Parameter(names = { "-l", "--ledgerid" }, description = "Ledger ID")
        private long ledgerId = -1;

        @Parameter(names = { "-e", "--entryid" }, description = "Entry ID")
        private long entryId = -1;

        @Parameter(names = { "-sp", "--startpos" }, description = "Start Position")
        private long startPos = -1;

        @Parameter(names = { "-ep", "--endpos" }, description = "End Position")
        private long endPos = -1;

        @Parameter(names = { "-f", "--filename" }, description = "Entry log filename")
        private String filename;

        @Parameter(names = { "-li", "--entrylogid" }, description = "Entry log id")
        private long entryLogId = -1;

        @Parameter(names = {"-lf", "--ledgerIdFormatter"}, description = "Set ledger id formatter")
        private String ledgerIdFormatter;

        @Parameter(names = {"-ef", "--entryformatter"}, description = "set entry formatter")
        private String entryFormatter;
    }

    @Override
    public boolean apply(ServerConfiguration conf, ReadLogFlags cmdFlags) {

        if (cmdFlags.ledgerIdFormatter != null && this.ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(cmdFlags.ledgerIdFormatter, conf);
        } else if (this.ledgerIdFormatter == null) {
            this.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(conf);
        }

        if (cmdFlags.entryFormatter != null && this.entryFormatter == null) {
            this.entryFormatter = EntryFormatter.newEntryFormatter(cmdFlags.entryFormatter, conf);
        } else if (this.entryFormatter == null) {
            this.entryFormatter = EntryFormatter.newEntryFormatter(conf);
        }

        if (cmdFlags.entryLogId == -1 && cmdFlags.filename == null) {
            System.err.println("Missing entry log id or entry log file name");
            usage();
            return false;
        }
        try {
            return readLog(conf, cmdFlags);
        } catch (Exception e) {
            throw new UncheckedExecutionException(e.getMessage(), e);
        }
    }

    private boolean readLog(ServerConfiguration conf, ReadLogFlags flags) throws Exception {
        long logId = flags.entryLogId;
        if (logId == -1 && flags.filename != null) {
            File f = new File(flags.filename);
            String name = f.getName();
            if (!name.endsWith(".log")) {
                System.err.println("Invalid entry log file name " + flags.filename);
                usage();
                return false;
            }
            String idString = name.split("\\.")[0];
            logId = Long.parseLong(idString, 16);
        }

        final long lId = flags.ledgerId;
        final long eId = flags.entryId;
        final long startpos = flags.startPos;
        final long endpos = flags.endPos;

        // scan entry log
        if (startpos != -1) {
            if ((endpos != -1) && (endpos < startpos)) {
                System.err
                    .println("ERROR: StartPosition of the range should be lesser than or equal to EndPosition");
                return false;
            }
            scanEntryLogForPositionRange(conf, logId, startpos, endpos, flags.msg);
        } else if (lId != -1) {
            scanEntryLogForSpecificEntry(conf, logId, lId, eId, flags.msg);
        } else {
            scanEntryLog(conf, logId, flags.msg);
        }
        return true;
    }

    /**
     * Scan over an entry log file for entries in the given position range.
     *
     * @param logId Entry Log File id.
     * @param rangeStartPos Start position of the entry we are looking for
     * @param rangeEndPos End position of the entry we are looking for (-1 for till the end of the entrylog)
     * @param printMsg Whether printing the entry data.
     * @throws Exception
     */
    private void scanEntryLogForPositionRange(ServerConfiguration conf, long logId, final long rangeStartPos,
                                              final long rangeEndPos,
                                                final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for PositionRange: "
                           + rangeStartPos + " - " + rangeEndPos);
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(conf, logId, new EntryLogger.EntryLogScanner() {
            private MutableBoolean stopScanning = new MutableBoolean(false);

            @Override
            public boolean accept(long ledgerId) {
                return !stopScanning.booleanValue();
            }

            @Override
            public void process(long ledgerId, long entryStartPos, ByteBuf entry) throws IOException {
                if (!stopScanning.booleanValue()) {
                    if ((rangeEndPos != -1) && (entryStartPos > rangeEndPos)) {
                        stopScanning.setValue(true);
                    } else {
                        int entrySize = entry.readableBytes();
                        /**
                         * entrySize of an entry (inclusive of payload and
                         * header) value is stored as int value in log file, but
                         * it is not counted in the entrySize, hence for calculating
                         * the end position of the entry we need to add additional
                         * 4 (intsize of entrySize). Please check
                         * EntryLogger.scanEntryLog.
                         */
                        long entryEndPos = entryStartPos + entrySize + 4 - 1;
                        if (((rangeEndPos == -1) || (entryStartPos <= rangeEndPos)) && (rangeStartPos <= entryEndPos)) {
                            FormatUtil.formatEntry(entryStartPos, entry, printMsg, ledgerIdFormatter, entryFormatter);
                            entryFound.setValue(true);
                        }
                    }
                }
            }
        });
        if (!entryFound.booleanValue()) {
            System.out.println(
                "Entry log " + logId + " (" + Long.toHexString(logId) + ".log) doesn't has any entry in the range "
                + rangeStartPos + " - " + rangeEndPos
                + ". Probably the position range, you have provided is lesser than the LOGFILE_HEADER_SIZE (1024) "
                + "or greater than the current log filesize.");
        }
    }

    /**
     * Scan over entry log.
     *
     * @param logId   Entry Log Id
     * @param scanner Entry Log Scanner
     */
    private void scanEntryLog(ServerConfiguration conf, long logId, EntryLogger.EntryLogScanner scanner)
        throws IOException {
        initEntryLogger(conf);
        entryLogger.scanEntryLog(logId, scanner);
    }

    private synchronized void initEntryLogger(ServerConfiguration conf) throws IOException {
        if (null == entryLogger) {
            // provide read only entry logger
            entryLogger = new ReadOnlyEntryLogger(conf);
        }
    }

    /**
     * Scan over an entry log file for a particular entry.
     *
     * @param logId Entry Log File id.
     * @param ledgerId id of the ledger
     * @param entryId entryId of the ledger we are looking for (-1 for all of the entries of the ledger)
     * @param printMsg Whether printing the entry data.
     * @throws Exception
     */
    private void scanEntryLogForSpecificEntry(ServerConfiguration conf, long logId, final long ledgerId,
                                                final long entryId,
                                                final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for LedgerId "
                           + ledgerId + ((entryId == -1) ? "" : " for EntryId " + entryId));
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(conf, logId, new EntryLogger.EntryLogScanner() {
            @Override
            public boolean accept(long candidateLedgerId) {
                return ((candidateLedgerId == ledgerId) && ((!entryFound.booleanValue()) || (entryId == -1)));
            }

            @Override
            public void process(long candidateLedgerId, long startPos, ByteBuf entry) {
                long entrysLedgerId = entry.getLong(entry.readerIndex());
                long entrysEntryId = entry.getLong(entry.readerIndex() + 8);
                if ((candidateLedgerId == entrysLedgerId) && (candidateLedgerId == ledgerId)
                    && ((entrysEntryId == entryId) || (entryId == -1))) {
                    entryFound.setValue(true);
                    FormatUtil.formatEntry(startPos, entry, printMsg, ledgerIdFormatter, entryFormatter);
                }
            }
        });
        if (!entryFound.booleanValue()) {
            System.out.println("LedgerId " + ledgerId + ((entryId == -1) ? "" : " EntryId " + entryId)
                               + " is not available in the entry log " + logId + " (" + Long.toHexString(logId)
                               + ".log)");
        }
    }

    /**
     * Scan over an entry log file.
     *
     * @param logId
     *          Entry Log File id.
     * @param printMsg
     *          Whether printing the entry data.
     */
    private void scanEntryLog(ServerConfiguration conf, long logId, final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        scanEntryLog(conf, logId, new EntryLogger.EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return true;
            }

            @Override
            public void process(long ledgerId, long startPos, ByteBuf entry) {
                FormatUtil.formatEntry(startPos, entry, printMsg, ledgerIdFormatter, entryFormatter);
            }
        });
    }
}
