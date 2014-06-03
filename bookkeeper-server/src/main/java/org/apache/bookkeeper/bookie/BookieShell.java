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
import java.nio.ByteBuffer;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

import com.google.common.util.concurrent.AbstractFuture;
import static com.google.common.base.Charsets.UTF_8;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bookie Shell is to provide utilities for users to administer a bookkeeper cluster.
 */
public class BookieShell implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(BookieShell.class);

    static final String ENTRY_FORMATTER_CLASS = "entryFormatterClass";

    static final String CMD_METAFORMAT = "metaformat";
    static final String CMD_BOOKIEFORMAT = "bookieformat";
    static final String CMD_RECOVER = "recover";
    static final String CMD_LEDGER = "ledger";
    static final String CMD_LISTLEDGERS = "listledgers";
    static final String CMD_LEDGERMETADATA = "ledgermetadata";
    static final String CMD_LISTUNDERREPLICATED = "listunderreplicated";
    static final String CMD_WHOISAUDITOR = "whoisauditor";
    static final String CMD_SIMPLETEST = "simpletest";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_AUTORECOVERY = "autorecovery";
    static final String CMD_LISTBOOKIES = "listbookies";
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
     * Format the bookkeeper metadata present in zookeeper
     */
    class MetaFormatCmd extends MyCommand {
        Options opts = new Options();

        MetaFormatCmd() {
            super(CMD_METAFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format bookkeeper metadata in zookeeper";
        }

        @Override
        String getUsage() {
            return "metaformat   [-nonInteractive] [-force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            boolean result = BookKeeperAdmin.format(adminConf, interactive,
                    force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Formats the local data present in current bookie server
     */
    class BookieFormatCmd extends MyCommand {
        Options opts = new Options();

        public BookieFormatCmd() {
            super(CMD_BOOKIEFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt..?");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format the current server contents";
        }

        @Override
        String getUsage() {
            return "bookieformat [-nonInteractive] [-force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = Bookie.format(conf, interactive, force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Recover command for ledger data recovery for failed bookie
     */
    class RecoverCmd extends MyCommand {
        Options opts = new Options();

        public RecoverCmd() {
            super(CMD_RECOVER);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Recover the ledger data for failed bookie";
        }

        @Override
        String getUsage() {
            return "recover      <bookieSrc> [bookieDest]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length < 1) {
                throw new MissingArgumentException(
                        "'bookieSrc' argument required");
            }

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                return bkRecovery(admin, args);
            } finally {
                if (null != admin) {
                    admin.close();
                }
            }
        }

        private int bkRecovery(BookKeeperAdmin bkAdmin, String[] args)
                throws InterruptedException, BKException {
            final String bookieSrcString[] = args[0].split(":");
            if (bookieSrcString.length != 2) {
                System.err.println("BookieSrc inputted has invalid format"
                        + "(host:port expected): " + args[0]);
                return -1;
            }
            final BookieSocketAddress bookieSrc = new BookieSocketAddress(
                    bookieSrcString[0], Integer.parseInt(bookieSrcString[1]));
            BookieSocketAddress bookieDest = null;
            if (args.length >= 2) {
                final String bookieDestString[] = args[1].split(":");
                if (bookieDestString.length < 2) {
                    System.err.println("BookieDest inputted has invalid format"
                            + "(host:port expected): " + args[1]);
                    return -1;
                }
                bookieDest = new BookieSocketAddress(bookieDestString[0],
                        Integer.parseInt(bookieDestString[1]));
            }

            bkAdmin.recoverBookieData(bookieSrc, bookieDest);
            return 0;
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
            return "ledger       [-m] <ledger_id>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command for listing underreplicated ledgers
     */
    class ListUnderreplicatedCmd extends MyCommand {
        Options opts = new Options();

        public ListUnderreplicatedCmd() {
            super(CMD_LISTUNDERREPLICATED);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "List ledgers marked as underreplicated";
        }

        @Override
        String getUsage() {
            return "listunderreplicated";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                Iterator<Long> iter = underreplicationManager.listLedgersToRereplicate();
                while (iter.hasNext()) {
                    System.out.println(iter.next());
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    final static int LIST_BATCH_SIZE = 1000;
    /**
     * Command to list all ledgers in the cluster
     */
    class ListLedgersCmd extends MyCommand {
        Options lOpts = new Options();

        ListLedgersCmd() {
            super(CMD_LISTLEDGERS);
            lOpts.addOption("m", "meta", false, "Print metadata");

        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerManager m = mFactory.newLedgerManager();
                LedgerRangeIterator iter = m.getLedgerRanges();
                if (cmdLine.hasOption("m")) {
                    List<ReadMetadataCallback> futures
                        = new ArrayList<ReadMetadataCallback>(LIST_BATCH_SIZE);
                    while (iter.hasNext()) {
                        LedgerRange r = iter.next();
                        for (Long lid : r.getLedgers()) {
                            ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                            m.readLedgerMetadata(lid, cb);
                            futures.add(cb);
                        }
                        if (futures.size() >= LIST_BATCH_SIZE) {
                            while (futures.size() > 0) {
                                ReadMetadataCallback cb = futures.remove(0);
                                printLedgerMetadata(cb);
                            }
                        }
                    }
                    while (futures.size() > 0) {
                        ReadMetadataCallback cb = futures.remove(0);
                        printLedgerMetadata(cb);
                    }
                } else {
                    while (iter.hasNext()) {
                        LedgerRange r = iter.next();
                        for (Long lid : r.getLedgers()) {
                            System.out.println(Long.toString(lid));
                        }
                    }
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "List all ledgers on the cluster (this may take a long time)";
        }

        @Override
        String getUsage() {
            return "listledgers  [-meta]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    static void printLedgerMetadata(ReadMetadataCallback cb) throws Exception {
        LedgerMetadata md = cb.get();
        System.out.println("ledgerID: " + cb.getLedgerId());
        System.out.println(new String(md.serialize(), UTF_8));
    }

    static class ReadMetadataCallback extends AbstractFuture<LedgerMetadata>
        implements GenericCallback<LedgerMetadata> {
        final long ledgerId;

        ReadMetadataCallback(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        public void operationComplete(int rc, LedgerMetadata result) {
            if (rc != 0) {
                setException(BKException.create(rc));
            } else {
                set(result);
            }
        }
    }

    /**
     * Print the metadata for a ledger
     */
    class LedgerMetadataCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerMetadataCmd() {
            super(CMD_LEDGERMETADATA);
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final long lid = getOptionLongValue(cmdLine, "ledgerid", -1);
            if (lid == -1) {
                System.err.println("Must specify a ledger id");
                return -1;
            }

            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerManager m = mFactory.newLedgerManager();
                ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                m.readLedgerMetadata(lid, cb);
                printLedgerMetadata(cb);
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Print the metadata for a ledger";
        }

        @Override
        String getUsage() {
            return "ledgermetadata -ledgerid <ledgerid>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Simple test to create a ledger and write to it
     */
    class SimpleTestCmd extends MyCommand {
        Options lOpts = new Options();

        SimpleTestCmd() {
            super(CMD_SIMPLETEST);
            lOpts.addOption("e", "ensemble", true, "Ensemble size (default 3)");
            lOpts.addOption("w", "writeQuorum", true, "Write quorum size (default 2)");
            lOpts.addOption("a", "ackQuorum", true, "Ack quorum size (default 2)");
            lOpts.addOption("n", "numEntries", true, "Entries to write (default 1000)");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            byte[] data = new byte[100]; // test data

            int ensemble = getOptionIntValue(cmdLine, "ensemble", 3);
            int writeQuorum = getOptionIntValue(cmdLine, "writeQuorum", 2);
            int ackQuorum = getOptionIntValue(cmdLine, "ackQuorum", 2);
            int numEntries = getOptionIntValue(cmdLine, "numEntries", 1000);

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            BookKeeper bk = new BookKeeper(conf);
            LedgerHandle lh = bk.createLedger(ensemble, writeQuorum, ackQuorum,
                                              BookKeeper.DigestType.MAC, new byte[0]);
            System.out.println("Ledger ID: " + lh.getId());
            long lastReport = System.nanoTime();
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(data);
                if (TimeUnit.SECONDS.convert(System.nanoTime() - lastReport,
                                             TimeUnit.NANOSECONDS) > 1) {
                    System.out.println(i + " entries written");
                    lastReport = System.nanoTime();
                }
            }

            lh.close();
            bk.close();
            System.out.println(numEntries + " entries written to ledger " + lh.getId());

            return 0;
        }

        @Override
        String getDescription() {
            return "Simple test to create a ledger and write entries to it";
        }

        @Override
        String getUsage() {
            return "simpletest   [-ensemble N] [-writeQuorum N] [-ackQuorum N] [-numEntries N]";
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
            return "readlog      [-msg] <entry_log_id | entry_log_file_name>";
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
            return "readjournal  [-msg] <journal_id | journal_file_name>";
        }

        @Override
        Options getOptions() {
            return rjOpts;
        }
    }

    /**
     * Command to print last log mark
     */
    class LastMarkCmd extends MyCommand {
        LastMarkCmd() {
            super(CMD_LASTMARK);
        }

        @Override
        public int runCmd(CommandLine c) throws Exception {
            printLastLogMark();
            return 0;
        }

        @Override
        String getDescription() {
            return "Print last log marker.";
        }

        @Override
        String getUsage() {
            return "lastmark";
        }

        @Override
        Options getOptions() {
            return new Options();
        }
    }

    /**
     * List available bookies
     */
    class ListBookiesCmd extends MyCommand {
        Options opts = new Options();

        ListBookiesCmd() {
            super(CMD_LISTBOOKIES);
            opts.addOption("rw", "readwrite", false, "Print readwrite bookies");
            opts.addOption("ro", "readonly", false, "Print readonly bookies");
            opts.addOption("h", "hostnames", false,
                    "Also print hostname of the bookie");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            boolean readwrite = cmdLine.hasOption("rw");
            boolean readonly = cmdLine.hasOption("ro");

            if ((!readwrite && !readonly) || (readwrite && readonly)) {
                LOG.error("One and only one of -readwrite and -readonly must be specified");
                printUsage();
                return 1;
            }
            ClientConfiguration clientconf = new ClientConfiguration(bkConf)
                .setZkServers(bkConf.getZkServers());
            BookKeeperAdmin bka = new BookKeeperAdmin(clientconf);

            int count = 0;
            Collection<BookieSocketAddress> bookies = new ArrayList<BookieSocketAddress>();
            if (cmdLine.hasOption("rw")) {
                Collection<BookieSocketAddress> availableBookies = bka
                        .getAvailableBookies();
                bookies.addAll(availableBookies);
            } else if (cmdLine.hasOption("ro")) {
                Collection<BookieSocketAddress> roBookies = bka
                        .getReadOnlyBookies();
                bookies.addAll(roBookies);
            }
            for (BookieSocketAddress b : bookies) {
                System.out.print(b);
                if (cmdLine.hasOption("h")) {
                    System.out.print("\t" + b.getSocketAddress().getHostName());
                }
                System.out.println("");
                count++;
            }
            if (count == 0) {
                System.err.println("No bookie exists!");
                return 1;
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "List the bookies, which are running as either readwrite or readonly mode.";
        }

        @Override
        String getUsage() {
            return "listbookies  [-readwrite|-readonly] [-hostnames]";
        }

        @Override
        Options getOptions() {
            return opts;
        }
    }

    /**
     * Command to print help message
     */
    class HelpCmd extends MyCommand {
        HelpCmd() {
            super(CMD_HELP);
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
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
        String getDescription() {
            return "Describe the usage of this program or its subcommands.";
        }

        @Override
        String getUsage() {
            return "help         [COMMAND]";
        }

        @Override
        Options getOptions() {
            return new Options();
        }
    }

    /**
     * Command for administration of autorecovery
     */
    class AutoRecoveryCmd extends MyCommand {
        Options opts = new Options();

        public AutoRecoveryCmd() {
            super(CMD_AUTORECOVERY);
            opts.addOption("e", "enable", false,
                           "Enable auto recovery of underreplicated ledgers");
            opts.addOption("d", "disable", false,
                           "Disable auto recovery of underreplicated ledgers");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Enable or disable autorecovery in the cluster.";
        }

        @Override
        String getUsage() {
            return "autorecovery [-enable|-disable]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean disable = cmdLine.hasOption("d");
            boolean enable = cmdLine.hasOption("e");

            if ((!disable && !enable)
                || (enable && disable)) {
                LOG.error("One and only one of -enable and -disable must be specified");
                printUsage();
                return 1;
            }
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (enable) {
                    if (underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already enabled. Doing nothing");
                    } else {
                        LOG.info("Enabling autorecovery");
                        underreplicationManager.enableLedgerReplication();
                    }
                } else {
                    if (!underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already disabled. Doing nothing");
                    } else {
                        LOG.info("Disabling autorecovery");
                        underreplicationManager.disableLedgerReplication();
                    }
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    /**
     * Print which node has the auditor lock
     */
    class WhoIsAuditorCmd extends MyCommand {
        Options opts = new Options();

        public WhoIsAuditorCmd() {
            super(CMD_WHOISAUDITOR);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Print the node which holds the auditor lock";
        }

        @Override
        String getUsage() {
            return "whoisauditor";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(bkConf.getZkTimeout());
                zk = ZkUtils.createConnectedZookeeperClient(bkConf.getZkServers(), w);
                BookieSocketAddress bookieId = AuditorElector.getCurrentAuditor(bkConf, zk);
                if (bookieId == null) {
                    LOG.info("No auditor elected");
                    return -1;
                }
                LOG.info("Auditor: {}/{}:{}",
                         new Object[] {
                             bookieId.getSocketAddress().getAddress().getCanonicalHostName(),
                             bookieId.getSocketAddress().getAddress().getHostAddress(),
                             bookieId.getSocketAddress().getPort() });
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    final Map<String, MyCommand> commands = new HashMap<String, MyCommand>();
    {
        commands.put(CMD_METAFORMAT, new MetaFormatCmd());
        commands.put(CMD_BOOKIEFORMAT, new BookieFormatCmd());
        commands.put(CMD_RECOVER, new RecoverCmd());
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_LISTLEDGERS, new ListLedgersCmd());
        commands.put(CMD_LISTUNDERREPLICATED, new ListUnderreplicatedCmd());
        commands.put(CMD_WHOISAUDITOR, new WhoIsAuditorCmd());
        commands.put(CMD_LEDGERMETADATA, new LedgerMetadataCmd());
        commands.put(CMD_SIMPLETEST, new SimpleTestCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_AUTORECOVERY, new AutoRecoveryCmd());
        commands.put(CMD_LISTBOOKIES, new ListBookiesCmd());
        commands.put(CMD_HELP, new HelpCmd());
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectory = Bookie.getCurrentDirectory(bkConf.getJournalDir());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        formatter = EntryFormatter.newEntryFormatter(bkConf, ENTRY_FORMATTER_CLASS);
        LOG.debug("Using entry formatter {}", formatter.getClass().getName());
        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private void printShellUsage() {
        System.err.println("Usage: BookieShell [-conf configuration] <command>");
        System.err.println();
        List<String> commandNames = new ArrayList<String>();
        for (MyCommand c : commands.values()) {
            commandNames.add("       " + c.getUsage());
        }
        Collections.sort(commandNames);
        for (String s : commandNames) {
            System.err.println(s);
        }
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
        BookieShell shell = new BookieShell();
        if (argv.length <= 0) {
            shell.printShellUsage();
            System.exit(-1);
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        // load configuration
        if ("-conf".equals(argv[0])) {
            if (argv.length <= 1) {
                shell.printShellUsage();
                System.exit(-1);
            }
            conf.addConfiguration(new PropertiesConfiguration(
                                  new File(argv[1]).toURI().toURL()));

            String[] newArgv = new String[argv.length - 2];
            System.arraycopy(argv, 2, newArgv, 0, newArgv.length);
            argv = newArgv;
        }


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
        String ledgerName = IndexPersistenceMgr.getLedgerName(ledgerId);
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

    private synchronized Journal getJournal() throws IOException {
        if (null == journal) {
            journal = new Journal(bkConf, new LedgerDirsManager(bkConf, bkConf.getLedgerDirs()));
        }
        return journal;
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
        getJournal().scanJournal(journalId, 0L, scanner);
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
                lep.setLedgerAndFirstEntry(ledgerId, curEntry);
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
        LogMark lastLogMark = getJournal().getLastLogMark().getCurMark();
        System.out.println("LastLogMark: Journal Id - " + lastLogMark.getLogFileId() + "("
                + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                + lastLogMark.getLogFileOffset());
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
        if (entryId == Bookie.METAENTRY_ID_FENCE_KEY) {
            System.out.println("Type:           META");
            System.out.println("Fenced");
            System.out.println();
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.getLong();
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
        formatter.close();
        return sb.toString();
    }

    private static int getOptionIntValue(CommandLine cmdLine, String option, int defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private static long getOptionLongValue(CommandLine cmdLine, String option, long defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid value for option " + option + " : " + val);
                return defaultVal;
            }
        }
        return defaultVal;
    }
}
