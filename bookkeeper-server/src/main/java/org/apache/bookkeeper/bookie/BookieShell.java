/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.Tool;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bookie Shell to read/check bookie files.
 */
public class BookieShell implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(BookieShell.class);

    static final String ENTRY_FORMATTER_CLASS = "entryFormatterClass";

    static final String CMD_LEDGER = "ledger";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_HELP = "help";

    final ServerConfiguration bkConf = new ServerConfiguration();
    File[] ledgerDirectories;
    File journalDirectory;

    EntryLogger entryLogger = null;
    Journal journal = null;
    EntryFormatter formatter;

    int pageSize;
    int entriesPerPage;

    interface Command {
        public int runCmd(String[] args) throws Exception;
        public void printUsage();
    }

    abstract class MyCommand implements Command {
        abstract Options getOptions();
        abstract String getDescription();
        abstract String getUsage();
        abstract int runCmd(CommandLine cmdLine) throws Exception;

        String cmdName;

        MyCommand(String cmdName) {
            this.cmdName = cmdName;
        }

        @Override
        public int runCmd(String[] args) throws Exception {
            try {
                BasicParser parser = new BasicParser();
                CommandLine cmdLine = parser.parse(getOptions(), args);
                return runCmd(cmdLine);
            } catch (ParseException e) {
                LOG.error("Error parsing command line arguments : ", e);
                printUsage();
                return -1;
            }
        }

        @Override
        public void printUsage() {
            HelpFormatter hf = new HelpFormatter();
            System.err.println(cmdName + ": " + getDescription());
            hf.printHelp(getUsage(), getOptions());
        }
    }

    /**
     * Ledger Command Handles ledger related operations
     */
    class LedgerCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerCmd() {
            super(CMD_LEDGER);
            lOpts.addOption("m", "meta", false, "Print meta information");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing ledger id");
                printUsage();
                return -1;
            }

            boolean printMeta = false;
            if (cmdLine.hasOption("m")) {
                printMeta = true;
            }
            long ledgerId;
            try {
                ledgerId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid ledger id " + leftArgs[0]);
                printUsage();
                return -1;
            }
            if (printMeta) {
                // print meta
                readLedgerMeta(ledgerId);
            }
            // dump ledger info
            readLedgerIndexEntries(ledgerId);
            return 0;
        }

        @Override
        String getDescription() {
            return "Dump ledger index entries into readable format.";
        }

        @Override
        String getUsage() {
            return "ledger [-m] <ledger_id>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command to read entry log files.
     */
    class ReadLogCmd extends MyCommand {
        Options rlOpts = new Options();

        ReadLogCmd() {
            super(CMD_READLOG);
            rlOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a entry log id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".log")) {
                    // not a log file
                    System.err.println("ERROR: invalid entry log file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                logId = Long.parseLong(idString, 16);
            }
            // scan entry log
            scanEntryLog(logId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan an entry file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readlog [-m] <entry_log_id | entry_log_file_name>";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }

    /**
     * Command to read journal files
     */
    class ReadJournalCmd extends MyCommand {
        Options rjOpts = new Options();

        ReadJournalCmd() {
            super(CMD_READJOURNAL);
            rjOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing journal id or journal file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long journalId;
            try {
                journalId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a journal id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".txn")) {
                    // not a journal file
                    System.err.println("ERROR: invalid journal file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                journalId = Long.parseLong(idString, 16);
            }
            // scan journal
            scanJournal(journalId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan a journal file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readjournal [-m] <journal_id | journal_file_name>";
        }

        @Override
        Options getOptions() {
            return rjOpts;
        }
    }

    /**
     * Command to print last log mark
     */
    class LastMarkCmd implements Command {
        @Override
        public int runCmd(String[] args) throws Exception {
            printLastLogMark();
            return 0;
        }

        @Override
        public void printUsage() {
            System.err.println("lastmark: Print last log marker.");
        }
    }

    /**
     * Command to print help message
     */
    class HelpCmd implements Command {
        @Override
        public int runCmd(String[] args) throws Exception {
            if (args.length == 0) {
                printShellUsage();
                return 0;
            }
            String cmdName = args[0];
            Command cmd = commands.get(cmdName);
            if (null == cmd) {
                System.err.println("Unknown command " + cmdName);
                printShellUsage();
                return -1;
            }
            cmd.printUsage();
            return 0;
        }

        @Override
        public void printUsage() {
            System.err.println("help: Describe the usage of this program or its subcommands.");
            System.err.println("usage: help [COMMAND]");
        }
    }

    final Map<String, Command> commands;
    {
        commands = new HashMap<String, Command>();
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_HELP, new HelpCmd());
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectory = Bookie.getCurrentDirectory(bkConf.getJournalDir());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        formatter = EntryFormatter.newEntryFormatter(bkConf, ENTRY_FORMATTER_CLASS);
        LOG.info("Using entry formatter " + formatter.getClass().getName());
        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private static void printShellUsage() {
        System.err.println("Usage: BookieShell [-conf configuration] <command>");
        System.err.println();
        System.err.println("       ledger      [-meta] <ledger_id>");
        System.err.println("       readlog     [-msg] <entry_log_id|entry_log_file_name>");
        System.err.println("       readjournal [-msg] <journal_id|journal_file_name>");
        System.err.println("       lastmark");
        System.err.println("       help");
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length <= 0) {
            printShellUsage();
            return -1;
        }
        String cmdName = args[0];
        Command cmd = commands.get(cmdName);
        if (null == cmd) {
            System.err.println("ERROR: Unknown command " + cmdName);
            printShellUsage();
            return -1;
        }
        // prepare new args
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        return cmd.runCmd(newArgs);
    }

    public static void main(String argv[]) throws Exception {
        if (argv.length <= 0) {
            printShellUsage();
            System.exit(-1);
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        // load configuration
        if ("-conf".equals(argv[0])) {
            if (argv.length <= 1) {
                printShellUsage();
                System.exit(-1);
            }
            conf.addConfiguration(new PropertiesConfiguration(
                                  new File(argv[1]).toURI().toURL()));

            String[] newArgv = new String[argv.length - 2];
            System.arraycopy(argv, 2, newArgv, 0, newArgv.length);
            argv = newArgv;
        }

        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(argv);
        System.exit(res);
    }

    ///
    /// Bookie File Operations
    ///

    /**
     * Get the ledger file of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     *
     * @return file object.
     */
    private File getLedgerFile(long ledgerId) {
        String ledgerName = LedgerCacheImpl.getLedgerName(ledgerId);
        File lf = null;
        for (File d : ledgerDirectories) {
            lf = new File(d, ledgerName);
            if (lf.exists()) {
                break;
            }
            lf = null;
        }
        return lf;
    }

    /**
     * Get FileInfo for a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @return read only file info instance
     */
    ReadOnlyFileInfo getFileInfo(long ledgerId) throws IOException {
        File ledgerFile = getLedgerFile(ledgerId);
        if (null == ledgerFile) {
            throw new FileNotFoundException("No index file found for ledger " + ledgerId + ". It may be not flushed yet.");
        }
        ReadOnlyFileInfo fi = new ReadOnlyFileInfo(ledgerFile, null);
        fi.readHeader();
        return fi;
    }

    private synchronized void initEntryLogger() throws IOException {
        if (null == entryLogger) {
            // provide read only entry logger
            entryLogger = new ReadOnlyEntryLogger(bkConf);
        }
    }

    /**
     * scan over entry log
     *
     * @param logId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     */
    protected void scanEntryLog(long logId, EntryLogScanner scanner) throws IOException {
        initEntryLogger();
        entryLogger.scanEntryLog(logId, scanner);
    }

    private synchronized void initJournal() throws IOException {
        if (null == journal) {
            journal = new Journal(bkConf);
        }
    }

    /**
     * Scan journal file
     *
     * @param journalId
     *          Journal File Id
     * @param scanner
     *          Journal File Scanner
     */
    protected void scanJournal(long journalId, JournalScanner scanner) throws IOException {
        initJournal();
        journal.scanJournal(journalId, 0L, scanner);
    }

    ///
    /// Bookie Shell Commands
    ///

    /**
     * Read ledger meta
     *
     * @param ledgerId
     *          Ledger Id
     */
    protected void readLedgerMeta(long ledgerId) throws Exception {
        System.out.println("===== LEDGER: " + ledgerId + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        byte[] masterKey = fi.getMasterKey();
        if (null == masterKey) {
            System.out.println("master key  : NULL");
        } else {
            System.out.println("master key  : " + bytes2Hex(fi.getMasterKey()));
        }
        long size = fi.size();
        if (size % 8 == 0) {
            System.out.println("size        : " + size);
        } else {
            System.out.println("size : " + size + " (not aligned with 8, may be corrupted or under flushing now)");
        }
        System.out.println("entries     : " + (size / 8));
    }

    /**
     * Read ledger index entires
     *
     * @param ledgerId
     *          Ledger Id
     * @throws IOException
     */
    protected void readLedgerIndexEntries(long ledgerId) throws IOException {
        System.out.println("===== LEDGER: " + ledgerId + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        long size = fi.size();
        System.out.println("size        : " + size);
        long curSize = 0;
        long curEntry = 0;
        LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
        lep.usePage();
        try {
            while (curSize < size) {
                lep.setLedger(ledgerId);
                lep.setFirstEntry(curEntry);
                lep.readPage(fi);

                // process a page
                for (int i=0; i<entriesPerPage; i++) {
                    long offset = lep.getOffset(i * 8);
                    if (0 == offset) {
                        System.out.println("entry " + curEntry + "\t:\tN/A");
                    } else {
                        long entryLogId = offset >> 32L;
                        long pos = offset & 0xffffffffL;
                        System.out.println("entry " + curEntry + "\t:\t(log:" + entryLogId + ", pos: " + pos + ")");
                    }
                    ++curEntry;
                }

                curSize += pageSize;
            }
        } catch (IOException ie) {
            LOG.error("Failed to read index page : ", ie);
            if (curSize + pageSize < size) {
                System.out.println("Failed to read index page @ " + curSize + ", the index file may be corrupted : " + ie.getMessage());
            } else {
                System.out.println("Failed to read last index page @ " + curSize
                                 + ", the index file may be corrupted or last index page is not fully flushed yet : " + ie.getMessage());
            }
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
    protected void scanEntryLog(long logId, final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return true;
            }
            @Override
            public void process(long ledgerId, long startPos, ByteBuffer entry) {
                formatEntry(startPos, entry, printMsg);
            }
        });
    }

    /**
     * Scan a journal file
     *
     * @param journalId
     *          Journal File Id
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanJournal(long journalId, final boolean printMsg) throws Exception {
        System.out.println("Scan journal " + journalId + " (" + Long.toHexString(journalId) + ".txn)");
        scanJournal(journalId, new JournalScanner() {
            boolean printJournalVersion = false;
            @Override
            public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                if (!printJournalVersion) {
                    System.out.println("Journal Version : " + journalVersion);
                    printJournalVersion = true;
                }
                formatEntry(offset, entry, printMsg);
            }
        });
    }

    /**
     * Print last log mark
     */
    protected void printLastLogMark() throws IOException {
        initJournal();
        LastLogMark lastLogMark = journal.getLastLogMark();
        System.out.println("LastLogMark: Journal Id - " + lastLogMark.txnLogId + "("
                         + Long.toHexString(lastLogMark.txnLogId) + ".txn), Pos - "
                         + lastLogMark.txnLogPosition);
    }

    /**
     * Format the message into a readable format.
     *
     * @param pos
     *          File offset of the message stored in entry log file
     * @param recBuff
     *          Entry Data
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(long pos, ByteBuffer recBuff, boolean printMsg) {
        long ledgerId = recBuff.getLong();
        long entryId = recBuff.getLong();
        int entrySize = recBuff.limit();

        System.out.println("--------- Lid=" + ledgerId + ", Eid=" + entryId
                         + ", ByteOffset=" + pos + ", EntrySize=" + entrySize + " ---------");
        if (entryId == Bookie.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.getInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.get(masterKey);
            System.out.println("Type:           META");
            System.out.println("MasterKey:      " + bytes2Hex(masterKey));
            System.out.println();
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.getLong();
        long length = recBuff.getLong();
        System.out.println("Type:           DATA");
        System.out.println("LastConfirmed:  " + lastAddConfirmed);
        if (!printMsg) {
            System.out.println();
            return;
        }
        // skip digest checking
        recBuff.position(32 + 8);
        System.out.println("Data:");
        System.out.println();
        try {
            byte[] ret = new byte[recBuff.remaining()];
            recBuff.get(ret);
            formatter.formatEntry(ret);
        } catch (Exception e) {
            System.out.println("N/A. Corrupted.");
        }
        System.out.println();
    }

    static String bytes2Hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : data) {
            formatter.format("%02x", b);
        }
        return sb.toString();
    }
}
