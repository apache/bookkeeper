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

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.replication.BookieLedgerIndexer;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;

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
    static final String CMD_READ_LEDGER_ENTRIES = "readledger";
    static final String CMD_LISTLEDGERS = "listledgers";
    static final String CMD_LEDGERMETADATA = "ledgermetadata";
    static final String CMD_LISTUNDERREPLICATED = "listunderreplicated";
    static final String CMD_WHOISAUDITOR = "whoisauditor";
    static final String CMD_SIMPLETEST = "simpletest";
    static final String CMD_BOOKIESANITYTEST = "bookiesanity";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_AUTORECOVERY = "autorecovery";
    static final String CMD_LISTBOOKIES = "listbookies";
    static final String CMD_LISTFILESONDISC = "listfilesondisc";
    static final String CMD_UPDATECOOKIE = "updatecookie";
    static final String CMD_EXPANDSTORAGE = "expandstorage";
    static final String CMD_UPDATELEDGER = "updateledgers";
    static final String CMD_DELETELEDGER = "deleteledger";
    static final String CMD_BOOKIEINFO = "bookieinfo";
    static final String CMD_DECOMMISSIONBOOKIE = "decommissionbookie";
    static final String CMD_LOSTBOOKIERECOVERYDELAY = "lostbookierecoverydelay"; 
    static final String CMD_TRIGGERAUDIT = "triggeraudit";
    static final String CMD_HELP = "help";

    final ServerConfiguration bkConf = new ServerConfiguration();
    File[] indexDirectories;
    File[] ledgerDirectories;
    File[] journalDirectories;

    EntryLogger entryLogger = null;
    List<Journal> journals = null;
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
            opts.addOption("d", "deleteCookie", false, "Delete its cookie on zookeeper");
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
            return "bookieformat [-nonInteractive] [-force] [-deleteCookie]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = Bookie.format(conf, interactive, force);
            // delete cookie
            if (cmdLine.hasOption("d")) {
                ZooKeeperClient zkc =
                        ZooKeeperClient.newBuilder()
                                .connectString(conf.getZkServers())
                                .sessionTimeoutMs(conf.getZkTimeout())
                                .build();
                try {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(zkc, conf);
                    cookie.getValue().deleteFromZooKeeper(zkc, conf, cookie.getVersion());
                } catch (KeeperException.NoNodeException nne) {
                    LOG.warn("No cookie to remove : ", nne);
                } finally {
                    zkc.close();
                }
            }
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
            opts.addOption("d", "deleteCookie", false, "Delete cookie node for the bookie.");
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
            return "recover [-deleteCookie] <bookieSrc> [bookieDest]";
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
                return bkRecovery(adminConf, admin, args, cmdLine.hasOption("d"));
            } finally {
                admin.close();
            }
        }

        private int bkRecovery(ClientConfiguration conf, BookKeeperAdmin bkAdmin,
                               String[] args, boolean deleteCookie)
                throws InterruptedException, BKException, KeeperException, IOException {
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
            if (deleteCookie) {
                try {
                    Versioned<Cookie> cookie = Cookie.readFromZooKeeper(bkAdmin.getZooKeeper(), conf, bookieSrc);
                    cookie.getValue().deleteFromZooKeeper(bkAdmin.getZooKeeper(), conf, bookieSrc, cookie.getVersion());
                } catch (KeeperException.NoNodeException nne) {
                    LOG.warn("No cookie to remove for {} : ", bookieSrc, nne);
                }
            }
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
     * Command for reading ledger entries
     */
    class ReadLedgerEntriesCmd extends MyCommand {
        Options lOpts = new Options();

        ReadLedgerEntriesCmd() {
            super(CMD_READ_LEDGER_ENTRIES);
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Read a range of entries from a ledger";
        }

        @Override
        String getUsage() {
            return "readledger <ledger_id> [<start_entry_id> [<end_entry_id>]]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing ledger id");
                printUsage();
                return -1;
            }

            long ledgerId;
            long firstEntry = 0;
            long lastEntry = -1;
            try {
                ledgerId = Long.parseLong(leftArgs[0]);
                if (leftArgs.length >= 2) {
                    firstEntry = Long.parseLong(leftArgs[1]);
                }
                if (leftArgs.length >= 3) {
                    lastEntry = Long.parseLong(leftArgs[2]);
                }
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid number " + nfe.getMessage());
                printUsage();
                return -1;
            }

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);

            BookKeeperAdmin bk = null;
            try {
                bk = new BookKeeperAdmin(conf);
                Iterator<LedgerEntry> entries = bk.readEntries(ledgerId, firstEntry, lastEntry).iterator();
                while (entries.hasNext()) {
                    LedgerEntry entry = entries.next();
                    formatEntry(entry, true);
                }
            } catch (Exception e) {
                LOG.error("Error reading entries from ledger {}", ledgerId, e.getCause());
                return -1;
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }

            return 0;
        }

    }

    /**
     * Command for listing underreplicated ledgers
     */
    class ListUnderreplicatedCmd extends MyCommand {
        Options opts = new Options();

        public ListUnderreplicatedCmd() {
            super(CMD_LISTUNDERREPLICATED);
            opts.addOption("missingreplica", true, "Bookie Id of missing replica");
            opts.addOption("excludingmissingreplica", true, "Bookie Id of missing replica to ignore");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "List ledgers marked as underreplicated, with optional options to specify missingreplica (BookieId) and to exclude missingreplica";
        }

        @Override
        String getUsage() {
            return "listunderreplicated [[-missingreplica <bookieaddress>] [-excludingmissingreplica <bookieaddress>]]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {

            final String includingBookieId = cmdLine.getOptionValue("missingreplica");
            final String excludingBookieId = cmdLine.getOptionValue("excludingmissingreplica");

            Predicate<List<String>> predicate = null;
            if (!StringUtils.isBlank(includingBookieId) && !StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> (replicasList.contains(includingBookieId)
                        && !replicasList.contains(excludingBookieId));
            } else if (!StringUtils.isBlank(includingBookieId)) {
                predicate = replicasList -> replicasList.contains(includingBookieId);
            } else if (!StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> !replicasList.contains(excludingBookieId);
            }

            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                Iterator<Long> iter = underreplicationManager.listLedgersToRereplicate(predicate);
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
            LedgerManagerFactory mFactory = null;
            LedgerManager m = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                m = mFactory.newLedgerManager();
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
                if (m != null) {
                    try {
                      m.close();
                      mFactory.uninitialize();
                    } catch (IOException ioe) {
                      LOG.error("Failed to close ledger manager : ", ioe);
                    }
                }
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
            LedgerManagerFactory mFactory = null;
            LedgerManager m = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                m = mFactory.newLedgerManager();
                ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                m.readLedgerMetadata(lid, cb);
                printLedgerMetadata(cb);
            } finally {
                if (m != null) {
                    try {
                        m.close();
                        mFactory.uninitialize();
                    } catch (IOException ioe) {
                        LOG.error("Failed to close ledger manager : ", ioe);
                    }
                }
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
     * Command to run a bookie sanity test
     */
    class BookieSanityTestCmd extends MyCommand {
        Options lOpts = new Options();

        BookieSanityTestCmd() {
            super(CMD_BOOKIESANITYTEST);
            lOpts.addOption("e", "entries", true, "Total entries to be added for the test (default 10)");
            lOpts.addOption("t", "timeout", true, "Timeout for write/read operations in seconds (default 1)");
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Sanity test for local bookie. Create ledger and write/reads entries on local bookie.";
        }

        @Override
        String getUsage() {
            return "bookiesanity [-entries N] [-timeout N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            int numberOfEntries = getOptionIntValue(cmdLine, "entries", 10);
            int timeoutSecs= getOptionIntValue(cmdLine, "timeout", 1);

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            conf.setEnsemblePlacementPolicy(LocalBookieEnsemblePlacementPolicy.class);
            conf.setAddEntryTimeout(timeoutSecs);
            conf.setReadEntryTimeout(timeoutSecs);

            BookKeeper bk = new BookKeeper(conf);
            LedgerHandle lh = null;
            try {
                lh = bk.createLedger(1, 1, DigestType.MAC, new byte[0]);
                LOG.info("Created ledger {}", lh.getId());

                for (int i = 0; i < numberOfEntries; i++) {
                    String content = "entry-" + i;
                    lh.addEntry(content.getBytes(UTF_8));
                }

                LOG.info("Written {} entries in ledger {}", numberOfEntries, lh.getId());

                // Reopen the ledger and read entries
                lh = bk.openLedger(lh.getId(), DigestType.MAC, new byte[0]);
                if (lh.getLastAddConfirmed() != (numberOfEntries - 1)) {
                    throw new Exception("Invalid last entry found on ledger. expecting: " + (numberOfEntries - 1)
                            + " -- found: " + lh.getLastAddConfirmed());
                }

                Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries - 1);
                int i = 0;
                while (entries.hasMoreElements()) {
                    LedgerEntry entry = entries.nextElement();
                    String actualMsg = new String(entry.getEntry(), UTF_8);
                    String expectedMsg = "entry-" + (i++);
                    if (!expectedMsg.equals(actualMsg)) {
                        throw new Exception("Failed validation of received message - Expected: " + expectedMsg
                                + ", Actual: " + actualMsg);
                    }
                }

                LOG.info("Read {} entries from ledger {}", entries, lh.getId());
            } catch (Exception e) {
                LOG.warn("Error in bookie sanity test", e);
                return -1;
            } finally {
                if (lh != null) {
                    bk.deleteLedger(lh.getId());
                    LOG.info("Deleted ledger {}", lh.getId());
                }

                bk.close();
            }

            LOG.info("Bookie sanity test succeeded");
            return 0;
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
            rlOpts.addOption("l", "ledgerid", true, "Ledger ID");
            rlOpts.addOption("e", "entryid", true, "EntryID");
            rlOpts.addOption("sp", "startpos", true, "Start Position");
            rlOpts.addOption("ep", "endpos", true, "End Position");
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

            final long lId = getOptionLongValue(cmdLine, "ledgerid", -1);
            final long eId = getOptionLongValue(cmdLine, "entryid", -1);
            final long startpos = getOptionLongValue(cmdLine, "startpos", -1);
            final long endpos = getOptionLongValue(cmdLine, "endpos", -1);

            // scan entry log
            if (startpos != -1) {
                if ((endpos != -1) && (endpos < startpos)) {
                    System.err
                            .println("ERROR: StartPosition of the range should be lesser than or equal to EndPosition");
                    return -1;
                }
                scanEntryLogForPositionRange(logId, startpos, endpos, printMsg);
            } else if (lId != -1) {
                scanEntryLogForSpecificEntry(logId, lId, eId, printMsg);
            } else {
                scanEntryLog(logId, printMsg);
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Scan an entry file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readlog      [-msg] <entry_log_id | entry_log_file_name> [-ledgerid <ledgerid> [-entryid <entryid>]] "
                    + "[-startpos <startEntryLogBytePos> [-endpos <endEntryLogBytePos>]]";
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
            rjOpts.addOption("dir", false, "Journal directory (needed if more than one journal configured)");
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

            Journal journal = null;
            if (getJournals().size() > 1) {
                if (!cmdLine.hasOption("dir")) {
                    System.err.println("ERROR: invalid or missing journal directory");
                    printUsage();
                    return -1;
                }

                File journalDirectory = new File(cmdLine.getOptionValue("dir"));
                for (Journal j : getJournals()) {
                    if (j.getJournalDirectory().equals(journalDirectory)) {
                        journal = j;
                        break;
                    }
                }

                if (journal == null) {
                    System.err.println("ERROR: journal directory not found");
                    printUsage();
                    return -1;
                }
            } else {
                journal = getJournals().get(0);
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
            scanJournal(journal, journalId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan a journal file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readjournal [-dir] [-msg] <journal_id | journal_file_name>";
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

    class ListDiskFilesCmd extends MyCommand {
        Options opts = new Options();

        ListDiskFilesCmd() {
            super(CMD_LISTFILESONDISC);
            opts.addOption("txn", "journal", false, "Print list of Journal Files");
            opts.addOption("log", "entrylog", false, "Print list of EntryLog Files");
            opts.addOption("idx", "index", false, "Print list of Index files");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {

            boolean journal = cmdLine.hasOption("txn");
            boolean entrylog = cmdLine.hasOption("log");
            boolean index = cmdLine.hasOption("idx");
            boolean all = false;

            if (!journal && !entrylog && !index && !all) {
                all = true;
            }

            if (all || journal) {
                File[] journalDirs = bkConf.getJournalDirs();
                List<File> journalFiles = listFilesAndSort(journalDirs, "txn");
                System.out.println("--------- Printing the list of Journal Files ---------");
                for (File journalFile : journalFiles) {
                    System.out.println(journalFile.getName());
                }
                System.out.println();
            }
            if (all || entrylog) {
                File[] ledgerDirs = bkConf.getLedgerDirs();
                List<File> ledgerFiles = listFilesAndSort(ledgerDirs, "log");
                System.out.println("--------- Printing the list of EntryLog/Ledger Files ---------");
                for (File ledgerFile : ledgerFiles) {
                    System.out.println(ledgerFile.getName());
                }
                System.out.println();
            }
            if (all || index) {
                File[] indexDirs = (bkConf.getIndexDirs() == null) ? bkConf.getLedgerDirs() : bkConf.getIndexDirs();
                List<File> indexFiles = listFilesAndSort(indexDirs, "idx");
                System.out.println("--------- Printing the list of Index Files ---------");
                for (File indexFile : indexFiles) {
                    System.out.println(indexFile.getName());
                }
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "List the files in JournalDirectory/LedgerDirectories/IndexDirectories";
        }

        @Override
        String getUsage() {
            return "listfilesondisc  [-journal|-entrylog|-index]";
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

            if (enable && disable) {
                LOG.error("Only one of -enable and -disable can be specified");
                printUsage();
                return 1;
            }
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (!enable && !disable) {
                    boolean enabled = underreplicationManager.isLedgerReplicationEnabled();
                    System.out.println("Autorecovery is " + (enabled ? "enabled." : "disabled."));
                } else if (enable) {
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
     * Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper
     */
    class LostBookieRecoveryDelayCmd extends MyCommand {
        Options opts = new Options();

        public LostBookieRecoveryDelayCmd() {
            super(CMD_LOSTBOOKIERECOVERYDELAY);
            opts.addOption("g", "get", false, "Get LostBookieRecoveryDelay value (in seconds)");
            opts.addOption("s", "set", true, "Set LostBookieRecoveryDelay value (in seconds)");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Setter and Getter for LostBookieRecoveryDelay value (in seconds) in Zookeeper";
        }

        @Override
        String getUsage() {
            return "lostbookierecoverydelay [-get|-set <value>]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean getter = cmdLine.hasOption("g");
            boolean setter = cmdLine.hasOption("s");

            if ((!getter && !setter) || (getter && setter)) {
                LOG.error("One and only one of -get and -set must be specified");
                printUsage();
                return 1;
            }
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder().connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout()).build();
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (getter) {
                    int lostBookieRecoveryDelay = underreplicationManager.getLostBookieRecoveryDelay();
                    LOG.info("LostBookieRecoveryDelay value in ZK: {}", String.valueOf(lostBookieRecoveryDelay));
                } else {
                    int lostBookieRecoveryDelay = Integer.parseInt(cmdLine.getOptionValue("set"));
                    underreplicationManager.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);
                    LOG.info("Successfully set LostBookieRecoveryDelay value in ZK: {}",
                            String.valueOf(lostBookieRecoveryDelay));
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
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
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

    /**
     * Update cookie command
     */
    class UpdateCookieCmd extends MyCommand {
        Options opts = new Options();

        UpdateCookieCmd() {
            super(CMD_UPDATECOOKIE);
            opts.addOption("b", "bookieId", true, "Bookie Id");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Update bookie id in cookie";
        }

        @Override
        String getUsage() {
            return "updatecookie -bookieId <hostname|ip>";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final String bookieId = cmdLine.getOptionValue("bookieId");
            if (StringUtils.isBlank(bookieId)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }
            if (!StringUtils.equals(bookieId, "hostname") && !StringUtils.equals(bookieId, "ip")) {
                LOG.error("Invalid option value:" + bookieId);
                this.printUsage();
                return -1;
            }
            boolean useHostName = getOptionalValue(bookieId, "hostname");
            if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=true as the option value passed is 'hostname'");
                return -1;
            } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                return -1;
            }
            return updateBookieIdInCookie(bookieId, useHostName);
        }

        private int updateBookieIdInCookie(final String bookieId, final boolean useHostname) throws IOException,
                InterruptedException {
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                ServerConfiguration conf = new ServerConfiguration(bkConf);
                String newBookieId = Bookie.getBookieAddress(conf).toString();
                // read oldcookie
                Versioned<Cookie> oldCookie = null;
                try {
                    conf.setUseHostNameAsBookieID(!useHostname);
                    oldCookie = Cookie.readFromZooKeeper(zk, conf);
                } catch (KeeperException.NoNodeException nne) {
                    LOG.error("Either cookie already updated with UseHostNameAsBookieID={} or no cookie exists!",
                            useHostname, nne);
                    return -1;
                }
                Cookie newCookie = Cookie.newBuilder(oldCookie.getValue()).setBookieHost(newBookieId).build();
                boolean hasCookieUpdatedInDirs = verifyCookie(newCookie, journalDirectories[0]);
                for (File dir : ledgerDirectories) {
                    hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                }
                if (indexDirectories != ledgerDirectories) {
                    for (File dir : indexDirectories) {
                        hasCookieUpdatedInDirs &= verifyCookie(newCookie, dir);
                    }
                }

                if (hasCookieUpdatedInDirs) {
                    try {
                        conf.setUseHostNameAsBookieID(useHostname);
                        Cookie.readFromZooKeeper(zk, conf);
                        // since newcookie exists, just do cleanup of oldcookie and return
                        conf.setUseHostNameAsBookieID(!useHostname);
                        oldCookie.getValue().deleteFromZooKeeper(zk, conf, oldCookie.getVersion());
                        return 0;
                    } catch (KeeperException.NoNodeException nne) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Ignoring, cookie will be written to zookeeper");
                        }
                    }
                } else {
                    // writes newcookie to local dirs
                    for (File journalDirectory : journalDirectories) {
                        newCookie.writeToDirectory(journalDirectory);
                        LOG.info("Updated cookie file present in journalDirectory {}", journalDirectory);
                    }
                    for (File dir : ledgerDirectories) {
                        newCookie.writeToDirectory(dir);
                    }
                    LOG.info("Updated cookie file present in ledgerDirectories {}", ledgerDirectories);
                    if (ledgerDirectories != indexDirectories) {
                        for (File dir : indexDirectories) {
                            newCookie.writeToDirectory(dir);
                        }
                        LOG.info("Updated cookie file present in indexDirectories {}", indexDirectories);
                    }
                }
                // writes newcookie to zookeeper
                conf.setUseHostNameAsBookieID(useHostname);
                newCookie.writeToZooKeeper(zk, conf, Version.NEW);

                // delete oldcookie
                conf.setUseHostNameAsBookieID(!useHostname);
                oldCookie.getValue().deleteFromZooKeeper(zk, conf, oldCookie.getVersion());
            } catch (KeeperException ke) {
                LOG.error("KeeperException during cookie updation!", ke);
                return -1;
            } catch (IOException ioe) {
                LOG.error("IOException during cookie updation!", ioe);
                return -1;
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
            return 0;
        }

        private boolean verifyCookie(Cookie oldCookie, File dir) throws IOException {
            try {
                Cookie cookie = Cookie.readFromDirectory(dir);
                cookie.verify(oldCookie);
            } catch (InvalidCookieException e) {
                return false;
            }
            return true;
        }
    }

    /**
     * Expand the storage directories owned by a bookie
     */
    class ExpandStorageCmd extends MyCommand {
        Options opts = new Options();

        ExpandStorageCmd() {
            super(CMD_EXPANDSTORAGE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Add new empty ledger/index directories. Update the directories"
                   + "info in the conf file before running the command.";
        }

        @Override
        String getUsage() {
            return "expandstorage";
        }

        @Override
        int runCmd(CommandLine cmdLine) {
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            ZooKeeper zk;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout()).build();
            } catch (KeeperException | InterruptedException | IOException e) {
                LOG.error("Exception while establishing zookeeper connection.", e);
                return -1;
            }

            List<File> allLedgerDirs = Lists.newArrayList();
            allLedgerDirs.addAll(Arrays.asList(ledgerDirectories));
            if (indexDirectories != ledgerDirectories) {
                allLedgerDirs.addAll(Arrays.asList(indexDirectories));
            }

            try {
                Bookie.checkEnvironmentWithStorageExpansion(conf, zk,
                        Lists.newArrayList(journalDirectories), allLedgerDirs);
            } catch (BookieException | IOException e) {
                LOG.error(
                        "Exception while updating cookie for storage expansion", e);
                return -1;
            }
            return 0;
        }
    }

    /**
     * Update ledger command
     */
    class UpdateLedgerCmd extends MyCommand {
        private final Options opts = new Options();

        UpdateLedgerCmd() {
            super(CMD_UPDATELEDGER);
            opts.addOption("b", "bookieId", true, "Bookie Id");
            opts.addOption("s", "updatespersec", true, "Number of ledgers updating per second (default: 5 per sec)");
            opts.addOption("l", "limit", true, "Maximum number of ledgers to update (default: no limit)");
            opts.addOption("v", "verbose", true, "Print status of the ledger updation (default: false)");
            opts.addOption("p", "printprogress", true,
                    "Print messages on every configured seconds if verbose turned on (default: 10 secs)");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Update bookie id in ledgers (this may take a long time)";
        }

        @Override
        String getUsage() {
            return "updateledger -bookieId <hostname|ip> [-updatespersec N] [-limit N] [-verbose true/false] [-printprogress N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final String bookieId = cmdLine.getOptionValue("bookieId");
            if (StringUtils.isBlank(bookieId)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }
            if (!StringUtils.equals(bookieId, "hostname") && !StringUtils.equals(bookieId, "ip")) {
                LOG.error("Invalid option value {} for bookieId, expected hostname/ip", bookieId);
                this.printUsage();
                return -1;
            }
            boolean useHostName = getOptionalValue(bookieId, "hostname");
            if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=true as the option value passed is 'hostname'");
                return -1;
            } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                return -1;
            }
            final int rate = getOptionIntValue(cmdLine, "updatespersec", 5);
            if (rate <= 0) {
                LOG.error("Invalid updatespersec {}, should be > 0", rate);
                return -1;
            }
            final int limit = getOptionIntValue(cmdLine, "limit", Integer.MIN_VALUE);
            if (limit <= 0 && limit != Integer.MIN_VALUE) {
                LOG.error("Invalid limit {}, should be > 0", limit);
                return -1;
            }
            final boolean verbose = getOptionBooleanValue(cmdLine, "verbose", false);
            final long printprogress;
            if (!verbose) {
                if (cmdLine.hasOption("printprogress")) {
                    LOG.warn("Ignoring option 'printprogress', this is applicable when 'verbose' is true");
                }
                printprogress = Integer.MIN_VALUE;
            } else {
                // defaulting to 10 seconds
                printprogress = getOptionLongValue(cmdLine, "printprogress", 10);
            }
            final ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);
            final BookKeeper bk = new BookKeeper(conf);
            final BookKeeperAdmin admin = new BookKeeperAdmin(conf);
            final UpdateLedgerOp updateLedgerOp = new UpdateLedgerOp(bk, admin);
            final ServerConfiguration serverConf = new ServerConfiguration(bkConf);
            final BookieSocketAddress newBookieId = Bookie.getBookieAddress(serverConf);
            serverConf.setUseHostNameAsBookieID(!useHostName);
            final BookieSocketAddress oldBookieId = Bookie.getBookieAddress(serverConf);

            UpdateLedgerNotifier progressable = new UpdateLedgerNotifier() {
                long lastReport = System.nanoTime();

                @Override
                public void progress(long updated, long issued) {
                    if (printprogress <= 0) {
                        return; // disabled
                    }
                    if (TimeUnit.MILLISECONDS.toSeconds(MathUtils.elapsedMSec(lastReport)) >= printprogress) {
                        LOG.info("Number of ledgers issued={}, updated={}", issued, updated);
                        lastReport = MathUtils.nowInNano();
                    }
                }
            };
            try {
                updateLedgerOp.updateBookieIdInLedgers(oldBookieId, newBookieId, rate, limit, progressable);
            } catch (BKException | IOException e) {
                LOG.error("Failed to update ledger metadata", e);
                return -1;
            }
            return 0;
        }

    }

    /**
     * Command to delete a given ledger.
     */
    class DeleteLedgerCmd extends MyCommand {
        Options lOpts = new Options();

        DeleteLedgerCmd() {
            super(CMD_DELETELEDGER);
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
            lOpts.addOption("f", "force", false, "Whether to force delete the Ledger without prompt..?");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final String lidStr = cmdLine.getOptionValue("ledgerid");
            if (StringUtils.isBlank(lidStr)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }

            final long lid;
            try {
                lid = Long.parseLong(lidStr);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid ledger id " + lidStr);
                printUsage();
                return -1;
            }

            boolean force = cmdLine.hasOption("f");
            boolean confirm = false;
            if (!force) {
                confirm = IOUtils.confirmPrompt("Are you sure to delete Ledger : " + lid + "?");
            }

            BookKeeper bk = null;
            try {
                if (force || confirm) {
                    ClientConfiguration conf = new ClientConfiguration();
                    conf.addConfiguration(bkConf);
                    bk = new BookKeeper(conf);
                    bk.deleteLedger(lid);
                }
            } finally {
                if (bk != null) {
                    bk.close();
                }
            }

            return 0;
        }

        @Override
        String getDescription() {
            return "Delete a ledger";
        }

        @Override
        String getUsage() {
            return "deleteledger -ledgerid <ledgerid> [-force]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /*
     * Command to retrieve bookie information like free disk space, etc from all
     * the bookies in the cluster.
     */
    class BookieInfoCmd extends MyCommand {
        Options lOpts = new Options();

        BookieInfoCmd() {
            super(CMD_BOOKIEINFO);
        }

        @Override
        String getDescription() {
            return "Retrieve bookie info such as free and total disk space";
        }

        @Override
        String getUsage() {
            return "bookieinfo";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        String getReadable(long val) {
            String unit[] = {"", "KB", "MB", "GB", "TB" };
            int cnt = 0;
            double d = val;
            while (d >= 1000 && cnt < unit.length-1) {
                d = d/1000;
                cnt++;
            }
            DecimalFormat df = new DecimalFormat("#.###");
            df.setRoundingMode(RoundingMode.DOWN);
            return cnt > 0 ? "(" + df.format(d) + unit[cnt] + ")" : unit[cnt];
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ClientConfiguration clientConf = new ClientConfiguration(bkConf);
            clientConf.setDiskWeightBasedPlacementEnabled(true);
            BookKeeper bk = new BookKeeper(clientConf);

            Map<BookieSocketAddress, BookieInfo> map = bk.getBookieInfo();
            if (map.size() == 0) {
                System.out.println("Failed to retrieve bookie information from any of the bookies");
                bk.close();
                return 0;
            }

            System.out.println("Free disk space info:");
            long totalFree = 0, total=0;
            for (Map.Entry<BookieSocketAddress, BookieInfo> e : map.entrySet()) {
                BookieInfo bInfo = e.getValue();
                System.out.println(e.getKey() + ":\tFree: " + bInfo.getFreeDiskSpace() +  getReadable(bInfo.getFreeDiskSpace()) +
                        "\tTotal: " + bInfo.getTotalDiskSpace() +  getReadable(bInfo.getTotalDiskSpace()));
                totalFree += bInfo.getFreeDiskSpace();
                total += bInfo.getTotalDiskSpace();
            }
            System.out.println("Total free disk space in the cluster:\t" + totalFree + getReadable(totalFree));
            System.out.println("Total disk capacity in the cluster:\t" + total + getReadable(total));
            bk.close();
            return 0;
        }
    }

    /**
     * Command to trigger AuditTask by resetting lostBookieRecoveryDelay to its current value
     */
    class TriggerAuditCmd extends MyCommand {
        Options opts = new Options();

        TriggerAuditCmd() {
            super(CMD_TRIGGERAUDIT);
        }

        @Override
        String getDescription() {
            return "Force trigger the Audit by resetting the lostBookieRecoveryDelay";
        }

        @Override
        String getUsage() {
            return CMD_TRIGGERAUDIT;
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            LedgerUnderreplicationManager underreplicationManager = null;
            try {
                zk = ZooKeeperClient.newBuilder()
                        .connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                LedgerManagerFactory ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                underreplicationManager = ledgerManagerFactory.newLedgerUnderreplicationManager();

                if (!underreplicationManager.isLedgerReplicationEnabled()) {
                    LOG.error("Autorecovery is disabled. So giving up!");
                    return -1;
                }
                BookieSocketAddress auditorId = AuditorElector.getCurrentAuditor(bkConf, zk);
                if (auditorId == null) {
                    LOG.error("No auditor elected, though Autorecovery is enabled. So giving up.");
                    return -1;
                }

                int previousLostBookieRecoveryDelayValue = underreplicationManager.getLostBookieRecoveryDelay();
                LOG.info("Resetting LostBookieRecoveryDelay value: {}, to kickstart audit task",
                        previousLostBookieRecoveryDelayValue);
                underreplicationManager.setLostBookieRecoveryDelay(previousLostBookieRecoveryDelayValue);
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
            return 0;
        }
    }
    
    /**
     * Command to trigger AuditTask by resetting lostBookieRecoveryDelay and then make sure the 
     * ledgers stored in the bookie are properly replicated.
     */
    class DecommissionBookieCmd extends MyCommand {
        Options lOpts = new Options();
        int maxSleepTimeInBetweenChecks = 10 * 60 * 1000; // 10 minutes
        int sleepTimePerLedger = 30 * 1000; // 30 secs
        LedgerUnderreplicationManager underreplicationManager = null;
        
        DecommissionBookieCmd() {
            super(CMD_DECOMMISSIONBOOKIE);
        }

        @Override
        String getDescription() {
            return "Force trigger the Audittask and make sure all the ledgers stored in the decommissioning bookie are replicated";
        }

        @Override
        String getUsage() {
            return CMD_DECOMMISSIONBOOKIE;
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.newBuilder().connectString(bkConf.getZkServers())
                        .sessionTimeoutMs(bkConf.getZkTimeout()).build();
                LedgerManagerFactory ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                underreplicationManager = ledgerManagerFactory.newLedgerUnderreplicationManager();

                if (!underreplicationManager.isLedgerReplicationEnabled()) {
                    LOG.error("Autorecovery is disabled. So giving up!");
                    return -1;
                }
                BookieSocketAddress auditorId = AuditorElector.getCurrentAuditor(bkConf, zk);
                if (auditorId == null) {
                    LOG.error("No auditor elected, though Autorecovery is enabled. So giving up.");
                    return -1;
                }

                int previousLostBookieRecoveryDelayValue = underreplicationManager.getLostBookieRecoveryDelay();
                LOG.info("Current LostBookieRecoveryDelay value: {}, resetting it to kickstart audit task",
                        previousLostBookieRecoveryDelayValue);
                underreplicationManager.setLostBookieRecoveryDelay(previousLostBookieRecoveryDelayValue);
                
                /*
                 * Sleep for 30 secs, so that Auditor gets chance to trigger its
                 * force audittask and let the underreplicationmanager process
                 * to do its replication process
                 */
                Thread.sleep(30 * 1000);
                
                /*
                 * get the collection of the ledgers which are stored in this
                 * bookie, by making a call to
                 * bookieLedgerIndexer.getBookieToLedgerIndex.
                 */
                LedgerManager ledgerManager = ledgerManagerFactory.newLedgerManager();
                BookieLedgerIndexer bookieLedgerIndexer = new BookieLedgerIndexer(ledgerManager);
                Map<String, Set<Long>> bookieToLedgersMap = bookieLedgerIndexer.getBookieToLedgerIndex();
                BookieSocketAddress thisBookieAddress = Bookie.getBookieAddress(bkConf);
                Set<Long> ledgersStoredInThisBookie = bookieToLedgersMap.get(thisBookieAddress.toString());
                if ((ledgersStoredInThisBookie != null) && (!ledgersStoredInThisBookie.isEmpty())) {
                    /*
                     * wait untill all the ledgers are replicated to other
                     * bookies by making sure that these ledgers metadata don't
                     * contain this bookie as part of their ensemble.
                     */
                    waitForLedgersToBeReplicated(ledgersStoredInThisBookie, thisBookieAddress, ledgerManager);
                }

                // for double-checking, check if any ledgers are listed as underreplicated because of this bookie
                Predicate<List<String>> predicate = replicasList -> replicasList.contains(thisBookieAddress.toString());
                Iterator<Long> urLedgerIterator = underreplicationManager.listLedgersToRereplicate(predicate);
                if (urLedgerIterator.hasNext()) {
                    //if there are any then wait and make sure those ledgers are replicated properly
                    LOG.info("Still in some underreplicated ledgers metadata, this bookie is part of its ensemble. "
                            + "Have to make sure that those ledger fragments are rereplicated");
                    List<Long> urLedgers = new ArrayList<>();
                    urLedgerIterator.forEachRemaining(urLedgers::add);
                    waitForLedgersToBeReplicated(urLedgers, thisBookieAddress, ledgerManager);
                }
                return 0;
            } catch (Exception e) {
                LOG.error("Received exception in DecommissionBookieCmd ", e);
                return -1;
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
        }

        private void waitForLedgersToBeReplicated(Collection<Long> ledgers, BookieSocketAddress thisBookieAddress,
                LedgerManager ledgerManager) throws InterruptedException, TimeoutException {
            Predicate<Long> validateBookieIsNotPartOfEnsemble = ledgerId -> !areEntriesOfLedgerStoredInTheBookie(ledgerId,
                    thisBookieAddress, ledgerManager);
            while (!ledgers.isEmpty()) {
                LOG.info("Count of Ledgers which need to be rereplicated: {}", ledgers.size());
                int sleepTimeForThisCheck = ledgers.size() * sleepTimePerLedger > maxSleepTimeInBetweenChecks
                        ? maxSleepTimeInBetweenChecks : ledgers.size() * sleepTimePerLedger;
                Thread.sleep(sleepTimeForThisCheck);
                LOG.debug("Making sure following ledgers replication to be completed: {}", ledgers);
                ledgers.removeIf(validateBookieIsNotPartOfEnsemble);
            }
        }

        private boolean areEntriesOfLedgerStoredInTheBookie(long ledgerId, BookieSocketAddress bookieAddress,
                LedgerManager ledgerManager) {
            ReadMetadataCallback cb = new ReadMetadataCallback(ledgerId);
            ledgerManager.readLedgerMetadata(ledgerId, cb);
            try {
                LedgerMetadata ledgerMetadata = cb.get();
                Collection<ArrayList<BookieSocketAddress>> ensemblesOfSegments = ledgerMetadata.getEnsembles().values();
                Iterator<ArrayList<BookieSocketAddress>> ensemblesOfSegmentsIterator = ensemblesOfSegments.iterator();
                ArrayList<BookieSocketAddress> ensemble;
                int segmentNo = 0;
                while (ensemblesOfSegmentsIterator.hasNext()) {
                    ensemble = ensemblesOfSegmentsIterator.next();
                    if (ensemble.contains(bookieAddress)) {
                        if (areEntriesOfSegmentStoredInTheBookie(ledgerMetadata, bookieAddress, segmentNo++)) {
                            return true;
                        }
                    }
                }
                return false;
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() != null
                        && e.getCause().getClass().equals(BKException.BKNoSuchLedgerExistsException.class)) {
                    LOG.debug("Ledger: {} has been deleted", ledgerId);
                    return false;
                } else {
                    LOG.error("Got exception while trying to read LedgerMeatadata of " + ledgerId, e);
                    throw new RuntimeException(e);
                }
            }
        }

        private boolean areEntriesOfSegmentStoredInTheBookie(LedgerMetadata ledgerMetadata,
                BookieSocketAddress bookieAddress, int segmentNo) {
            boolean isLedgerClosed = ledgerMetadata.isClosed();
            int ensembleSize = ledgerMetadata.getEnsembleSize();
            int writeQuorumSize = ledgerMetadata.getWriteQuorumSize();

            List<Entry<Long, ArrayList<BookieSocketAddress>>> segments = new LinkedList<Entry<Long, ArrayList<BookieSocketAddress>>>(
                    ledgerMetadata.getEnsembles().entrySet());

            boolean lastSegment = (segmentNo == (segments.size() - 1));
            
            /*
             * Checking the last segment of the ledger can be complicated in
             * some cases. In the case that the ledger is closed, we can just
             * check the fragments of the segment as normal, except in the case
             * that no entry was ever written, to the ledger, in which case we
             * check no fragments.
             * 
             * Following the same approach as in LedgerChecker.checkLedger
             */
            if (lastSegment && isLedgerClosed && (ledgerMetadata.getLastEntryId() < segments.get(segmentNo).getKey())) {
                return false;
            }

            /*
             * if ensembleSize is equal to writeQuorumSize, then ofcourse all
             * the entries of this segment are supposed to be stored in this
             * bookie. If this is last segment of the ledger and if the ledger
             * is not closed (this is a corner case), then we have to return
             * true. For more info. Check BOOKKEEPER-237 and BOOKKEEPER-325.
             */
            if ((lastSegment && !isLedgerClosed) || (ensembleSize == writeQuorumSize)) {
                return true;
            }

            /*
             * the following check is required because ensembleSize can be
             * greater than writeQuorumSize and in this case if there are only
             * couple of entries then based on RoundRobinDistributionSchedule
             * there might not be any entry copy in this bookie though this
             * bookie is part of the ensemble of this segment. If no entry is
             * stored in this bookie then we should return false, because
             * ReplicationWorker wont take care of fixing the ledgerMetadata of
             * this segment in this case.
             * 
             * if ensembleSize > writeQuorumSize, then in LedgerFragment.java
             * firstEntryID may not be equal to firstStoredEntryId lastEntryId
             * may not be equalto lastStoredEntryId. firstStoredEntryId and
             * lastStoredEntryId will be LedgerHandle.INVALID_ENTRY_ID, if no
             * entry of this segment stored in this bookie. In this case
             * LedgerChecker.verifyLedgerFragment will not consider it as
             * unavailable/bad fragment though this bookie is part of the
             * ensemble of the segment and it is down.
             */
            DistributionSchedule distributionSchedule = new RoundRobinDistributionSchedule(
                    ledgerMetadata.getWriteQuorumSize(), ledgerMetadata.getAckQuorumSize(),
                    ledgerMetadata.getEnsembleSize());
            ArrayList<BookieSocketAddress> currentSegmentEnsemble = segments.get(segmentNo).getValue();
            int thisBookieIndexInCurrentEnsemble = currentSegmentEnsemble.indexOf(bookieAddress);
            long firstEntryId = segments.get(segmentNo).getKey();
            long lastEntryId = lastSegment ? ledgerMetadata.getLastEntryId() : segments.get(segmentNo + 1).getKey() - 1;
            long firstStoredEntryId = LedgerHandle.INVALID_ENTRY_ID;
            long firstEntryIter = firstEntryId;
            // following the same approach followed in LedgerFragment.getFirstStoredEntryId()
            for (int i = 0; i < ensembleSize && firstEntryIter <= lastEntryId; i++) {
                if (distributionSchedule.hasEntry(firstEntryIter, thisBookieIndexInCurrentEnsemble)) {
                    firstStoredEntryId = firstEntryIter;
                    break;
                } else {
                    firstEntryIter++;
                }
            }
            return firstStoredEntryId != LedgerHandle.INVALID_ENTRY_ID;
        }
    }
    
    /**
     * A facility for reporting update ledger progress.
     */
    public interface UpdateLedgerNotifier {
        void progress(long updated, long issued);
    }

    final Map<String, MyCommand> commands = new HashMap<String, MyCommand>();
    {
        commands.put(CMD_METAFORMAT, new MetaFormatCmd());
        commands.put(CMD_BOOKIEFORMAT, new BookieFormatCmd());
        commands.put(CMD_RECOVER, new RecoverCmd());
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_READ_LEDGER_ENTRIES, new ReadLedgerEntriesCmd());
        commands.put(CMD_LISTLEDGERS, new ListLedgersCmd());
        commands.put(CMD_LISTUNDERREPLICATED, new ListUnderreplicatedCmd());
        commands.put(CMD_WHOISAUDITOR, new WhoIsAuditorCmd());
        commands.put(CMD_LEDGERMETADATA, new LedgerMetadataCmd());
        commands.put(CMD_SIMPLETEST, new SimpleTestCmd());
        commands.put(CMD_BOOKIESANITYTEST, new BookieSanityTestCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_AUTORECOVERY, new AutoRecoveryCmd());
        commands.put(CMD_LISTBOOKIES, new ListBookiesCmd());
        commands.put(CMD_LISTFILESONDISC, new ListDiskFilesCmd());
        commands.put(CMD_UPDATECOOKIE, new UpdateCookieCmd());
        commands.put(CMD_EXPANDSTORAGE, new ExpandStorageCmd());
        commands.put(CMD_UPDATELEDGER, new UpdateLedgerCmd());
        commands.put(CMD_DELETELEDGER, new DeleteLedgerCmd());
        commands.put(CMD_BOOKIEINFO, new BookieInfoCmd());
        commands.put(CMD_DECOMMISSIONBOOKIE, new DecommissionBookieCmd());
        commands.put(CMD_HELP, new HelpCmd());
        commands.put(CMD_LOSTBOOKIERECOVERYDELAY, new LostBookieRecoveryDelayCmd());  
        commands.put(CMD_TRIGGERAUDIT, new TriggerAuditCmd());
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectories = Bookie.getCurrentDirectories(bkConf.getJournalDirs());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        if (null == bkConf.getIndexDirs()) {
            indexDirectories = ledgerDirectories;
        } else {
            indexDirectories = Bookie.getCurrentDirectories(bkConf.getIndexDirs());
        }
        formatter = EntryFormatter.newEntryFormatter(bkConf, ENTRY_FORMATTER_CLASS);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Using entry formatter {}", formatter.getClass().getName());
        }
        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private void printShellUsage() {
        System.err.println(
                "Usage: BookieShell [-conf configuration] <command>");
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

    /**
     * Returns the sorted list of the files in the given folders with the given file extensions.
     * Sorting is done on the basis of CreationTime if the CreationTime is not available or if they are equal
     * then sorting is done by LastModifiedTime
     * @param folderNames - array of folders which we need to look recursively for files with given extensions
     * @param extensions - the file extensions, which we are interested in
     * @return sorted list of files
     */
    private static List<File> listFilesAndSort(File[] folderNames, String... extensions) {
        List<File> completeFilesList = new ArrayList<File>();
        for (int i = 0; i < folderNames.length; i++) {
            Collection<File> filesCollection = FileUtils.listFiles(folderNames[i], extensions, true);
            completeFilesList.addAll(filesCollection);
        }
        Collections.sort(completeFilesList, new FilesTimeComparator());
        return completeFilesList;
    }

    private static class FilesTimeComparator implements Comparator<File>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public int compare(File file1, File file2) {
            Path file1Path = Paths.get(file1.getAbsolutePath());
            Path file2Path = Paths.get(file2.getAbsolutePath());
            try {
                BasicFileAttributes file1Attributes = Files.readAttributes(file1Path, BasicFileAttributes.class);
                BasicFileAttributes file2Attributes = Files.readAttributes(file2Path, BasicFileAttributes.class);
                FileTime file1CreationTime = file1Attributes.creationTime();
                FileTime file2CreationTime = file2Attributes.creationTime();
                int compareValue = file1CreationTime.compareTo(file2CreationTime);
                /*
                 * please check https://docs.oracle.com/javase/7/docs/api/java/nio/file/attribute/BasicFileAttributes.html#creationTime()
                 * So not all file system implementation store creation time, in that case creationTime()
                 * method may return FileTime representing the epoch (1970-01-01T00:00:00Z). So in that case
                 * it would be better to compare lastModifiedTime
                 */
                if (compareValue == 0) {
                    FileTime file1LastModifiedTime = file1Attributes.lastModifiedTime();
                    FileTime file2LastModifiedTime = file2Attributes.lastModifiedTime();
                    compareValue = file1LastModifiedTime.compareTo(file2LastModifiedTime);
                }
                return compareValue;
            } catch (IOException e) {
                return 0;
            }
        }
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
        for (File d : indexDirectories) {
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

    private synchronized List<Journal> getJournals() throws IOException {
        if (null == journals) {
            journals = Lists.newArrayListWithCapacity(bkConf.getJournalDirs().length);
            for (File journalDir : bkConf.getJournalDirs()) {
                journals.add(new Journal(journalDir, bkConf, new LedgerDirsManager(bkConf, bkConf.getLedgerDirs())));
            }
        }
        return journals;
    }

    /**
     * Scan journal file
     *
     * @param journalId
     *          Journal File Id
     * @param scanner
     *          Journal File Scanner
     */
    protected void scanJournal(Journal journal, long journalId, JournalScanner scanner) throws IOException {
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
        System.out.println("isFenced    : " + fi.isFenced());
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
     * Scan over an entry log file for a particular entry
     *
     * @param logId
     *          Entry Log File id.
     * @param ledgerId
     *          id of the ledger
     * @param entryId
     *          entryId of the ledger we are looking for (-1 for all of the entries of the ledger)
     * @param printMsg
     *          Whether printing the entry data.
     * @throws Exception
     */
    protected void scanEntryLogForSpecificEntry(long logId, final long lId, final long eId, final boolean printMsg)
            throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for LedgerId " + lId
                + ((eId == -1) ? "" : " for EntryId " + eId));
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return ((lId == ledgerId) && ((!entryFound.booleanValue()) || (eId == -1)));
            }

            @Override
            public void process(long ledgerId, long startPos, ByteBuffer entry) {
                long entrysLedgerId = entry.getLong();
                long entrysEntryId = entry.getLong();
                entry.rewind();
                if ((ledgerId == entrysLedgerId) && (ledgerId == lId) && ((entrysEntryId == eId)) || (eId == -1)) {
                    entryFound.setValue(true);
                    formatEntry(startPos, entry, printMsg);
                }
            }
        });
        if (!entryFound.booleanValue()) {
            System.out.println("LedgerId " + lId + ((eId == -1) ? "" : " EntryId " + eId)
                    + " is not available in the entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        }
    }

    /**
     * Scan over an entry log file for entries in the given position range
     *
     * @param logId
     *          Entry Log File id.
     * @param rangeStartPos
     *          Start position of the entry we are looking for
     * @param rangeEndPos
     *          End position of the entry we are looking for (-1 for till the end of the entrylog)
     * @param printMsg
     *          Whether printing the entry data.
     * @throws Exception
     */
    protected void scanEntryLogForPositionRange(long logId, final long rangeStartPos, final long rangeEndPos, final boolean printMsg)
 throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for PositionRange: "
                + rangeStartPos + " - " + rangeEndPos);
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(logId, new EntryLogScanner() {
            private MutableBoolean stopScanning = new MutableBoolean(false);

            @Override
            public boolean accept(long ledgerId) {
                return !stopScanning.booleanValue();
            }

            @Override
            public void process(long ledgerId, long entryStartPos, ByteBuffer entry) {
                if (!stopScanning.booleanValue()) {
                    if ((rangeEndPos != -1) && (entryStartPos > rangeEndPos)) {
                        stopScanning.setValue(true);
                    } else {
                        int entrySize = entry.limit();
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
                            formatEntry(entryStartPos, entry, printMsg);
                            entryFound.setValue(true);
                        }
                    }
                }
            }
        });
        if (!entryFound.booleanValue()) {
            System.out.println("Entry log " + logId + " (" + Long.toHexString(logId)
                    + ".log) doesn't has any entry in the range " + rangeStartPos + " - " + rangeEndPos
                    + ". Probably the position range, you have provided is lesser than the LOGFILE_HEADER_SIZE (1024) "
                    + "or greater than the current log filesize.");
        }
    }

    /**
     * Scan a journal file
     *
     * @param journalId
     *          Journal File Id
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanJournal(Journal journal, long journalId, final boolean printMsg) throws Exception {
        System.out.println("Scan journal " + journalId + " (" + Long.toHexString(journalId) + ".txn)");
        scanJournal(journal, journalId, new JournalScanner() {
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
        for (Journal journal : getJournals()) {
            LogMark lastLogMark = journal.getLastLogMark().getCurMark();
            System.out.println("LastLogMark: Journal Id - " + lastLogMark.getLogFileId() + "("
                    + Long.toHexString(lastLogMark.getLogFileId()) + ".txn), Pos - "
                    + lastLogMark.getLogFileOffset());
        }
    }

    /**
     * Format the entry into a readable format.
     *
     * @param entry
     *          ledgerentry to print
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(LedgerEntry entry, boolean printMsg) {
        long ledgerId = entry.getLedgerId();
        long entryId = entry.getEntryId();
        long entrySize = entry.getLength();
        System.out
                .println("--------- Lid=" + ledgerId + ", Eid=" + entryId + ", EntrySize=" + entrySize + " ---------");
        if (printMsg) {
            formatter.formatEntry(entry.getEntry());
        }
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

    private static boolean getOptionBooleanValue(CommandLine cmdLine, String option, boolean defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            return Boolean.parseBoolean(val);
        }
        return defaultVal;
    }

    private static boolean getOptionalValue(String optValue, String optName) {
        if (StringUtils.equals(optValue, optName)) {
            return true;
        }
        return false;
    }
}
