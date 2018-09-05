/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.bookie;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithLedgerManagerFactory;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithMetadataBookieDriver;
import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;
import static org.apache.bookkeeper.tools.cli.helpers.CommandHelpers.getBookieSocketAddrStringRepresentation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.bookkeeper.bookie.BookieException.CookieNotFoundException;
import org.apache.bookkeeper.bookie.BookieException.InvalidCookieException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.LocationsIndexRebuildOp;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.MetaStoreException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.UpdateLedgerOp;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.replication.AuditorElector;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.tools.cli.commands.bookie.LastMarkCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ListBookiesCommand;
import org.apache.bookkeeper.tools.cli.commands.client.SimpleTestCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bookie Shell is to provide utilities for users to administer a bookkeeper cluster.
 */
public class BookieShell implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(BookieShell.class);

    static final String CONF_OPT = "conf";
    static final String ENTRY_FORMATTER_OPT = "entryformat";
    static final String LEDGERID_FORMATTER_OPT = "ledgeridformat";

    static final String CMD_METAFORMAT = "metaformat";
    static final String CMD_INITBOOKIE = "initbookie";
    static final String CMD_INITNEWCLUSTER = "initnewcluster";
    static final String CMD_NUKEEXISTINGCLUSTER = "nukeexistingcluster";
    static final String CMD_BOOKIEFORMAT = "bookieformat";
    static final String CMD_RECOVER = "recover";
    static final String CMD_LEDGER = "ledger";
    static final String CMD_READ_LEDGER_ENTRIES = "readledger";
    static final String CMD_LISTLEDGERS = "listledgers";
    static final String CMD_LEDGERMETADATA = "ledgermetadata";
    static final String CMD_LISTUNDERREPLICATED = "listunderreplicated";
    static final String CMD_WHOISAUDITOR = "whoisauditor";
    static final String CMD_WHATISINSTANCEID = "whatisinstanceid";
    static final String CMD_SIMPLETEST = "simpletest";
    static final String CMD_BOOKIESANITYTEST = "bookiesanity";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READLOGMETADATA = "readlogmetadata";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_AUTORECOVERY = "autorecovery";
    static final String CMD_LISTBOOKIES = "listbookies";
    static final String CMD_LISTFILESONDISC = "listfilesondisc";
    static final String CMD_UPDATECOOKIE = "updatecookie";
    static final String CMD_UPDATELEDGER = "updateledgers";
    static final String CMD_DELETELEDGER = "deleteledger";
    static final String CMD_BOOKIEINFO = "bookieinfo";
    static final String CMD_DECOMMISSIONBOOKIE = "decommissionbookie";
    static final String CMD_LOSTBOOKIERECOVERYDELAY = "lostbookierecoverydelay";
    static final String CMD_TRIGGERAUDIT = "triggeraudit";
    static final String CMD_CONVERT_TO_DB_STORAGE = "convert-to-db-storage";
    static final String CMD_CONVERT_TO_INTERLEAVED_STORAGE = "convert-to-interleaved-storage";
    static final String CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX = "rebuild-db-ledger-locations-index";
    static final String CMD_REGENERATE_INTERLEAVED_STORAGE_INDEX_FILE = "regenerate-interleaved-storage-index-file";
    static final String CMD_HELP = "help";

    final ServerConfiguration bkConf = new ServerConfiguration();
    File[] indexDirectories;
    File[] ledgerDirectories;
    File[] journalDirectories;

    EntryLogger entryLogger = null;
    List<Journal> journals = null;
    EntryFormatter entryFormatter;
    LedgerIdFormatter ledgerIdFormatter;

    int pageSize;
    int entriesPerPage;

    public BookieShell() {
    }

    public BookieShell(LedgerIdFormatter ledgeridFormatter, EntryFormatter entryFormatter) {
        this.ledgerIdFormatter = ledgeridFormatter;
        this.entryFormatter = entryFormatter;
    }

    interface Command {
        int runCmd(String[] args) throws Exception;

        void printUsage();
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
     * Format the bookkeeper metadata present in zookeeper.
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
            return "Format bookkeeper metadata in zookeeper.";
        }

        @Override
        String getUsage() {
            return "metaformat   [-nonInteractive] [-force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            boolean result = BookKeeperAdmin.format(bkConf, interactive, force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Intializes new cluster by creating required znodes for the cluster. If
     * ledgersrootpath is already existing then it will error out. If for any
     * reason it errors out while creating znodes for the cluster, then before
     * running initnewcluster again, try nuking existing cluster by running
     * nukeexistingcluster. This is required because ledgersrootpath znode would
     * be created after verifying that it doesn't exist, hence during next retry
     * of initnewcluster it would complain saying that ledgersrootpath is
     * already existing.
     */
    class InitNewCluster extends MyCommand {
        Options opts = new Options();

        InitNewCluster() {
            super(CMD_INITNEWCLUSTER);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Initializes a new bookkeeper cluster. If initnewcluster fails then try nuking "
                    + "existing cluster by running nukeexistingcluster before running initnewcluster again";
        }

        @Override
        String getUsage() {
            return "initnewcluster";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean result = BookKeeperAdmin.initNewCluster(bkConf);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Nuke bookkeeper metadata of existing cluster in zookeeper.
     */
    class NukeExistingCluster extends MyCommand {
        Options opts = new Options();

        NukeExistingCluster() {
            super(CMD_NUKEEXISTINGCLUSTER);
            opts.addOption("p", "zkledgersrootpath", true, "zookeeper ledgers rootpath");
            opts.addOption("i", "instanceid", true, "instanceid");
            opts.addOption("f", "force", false,
                    "If instanceid is not specified, "
                    + "then whether to force nuke the metadata without validating instanceid");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Nuke bookkeeper cluster by deleting metadata";
        }

        @Override
        String getUsage() {
            return "nukeexistingcluster -zkledgersrootpath <zkledgersrootpath> [-instanceid <instanceid> | -force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean force = cmdLine.hasOption("f");
            String zkledgersrootpath = cmdLine.getOptionValue("zkledgersrootpath");
            String instanceid = cmdLine.getOptionValue("instanceid");

            /*
             * for NukeExistingCluster command 'zkledgersrootpath' should be provided and either force option or
             * instanceid should be provided.
             */
            if ((zkledgersrootpath == null) || (force == (instanceid != null))) {
                LOG.error(
                        "zkledgersrootpath should be specified and either force option "
                        + "or instanceid should be specified (but not both)");
                printUsage();
                return -1;
            }

            boolean result = BookKeeperAdmin.nukeExistingCluster(bkConf, zkledgersrootpath, instanceid, force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Formats the local data present in current bookie server.
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
            opts.addOption("d", "deleteCookie", false, "Delete its cookie on metadata store");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format the current server contents.";
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
                runFunctionWithRegistrationManager(bkConf, rm -> {
                    try {
                        Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, conf);
                        cookie.getValue().deleteFromRegistrationManager(rm, conf, cookie.getVersion());
                    } catch (CookieNotFoundException nne) {
                        LOG.warn("No cookie to remove : ", nne);
                    } catch (BookieException be) {
                        throw new UncheckedExecutionException(be.getMessage(), be);
                    }
                    return null;
                });
            }
            return (result) ? 0 : 1;
        }
    }

    /**
     * Initializes bookie, by making sure that the journalDir, ledgerDirs and
     * indexDirs are empty and there is no registered Bookie with this BookieId.
     */
    class InitBookieCmd extends MyCommand {
        Options opts = new Options();

        public InitBookieCmd() {
            super(CMD_INITBOOKIE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Initialize new Bookie";
        }

        @Override
        String getUsage() {
            return CMD_INITBOOKIE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = BookKeeperAdmin.initBookie(conf);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Recover command for ledger data recovery for failed bookie.
     */
    class RecoverCmd extends MyCommand {
        Options opts = new Options();

        public RecoverCmd() {
            super(CMD_RECOVER);
            opts.addOption("q", "query", false, "Query the ledgers that contain given bookies");
            opts.addOption("dr", "dryrun", false, "Printing the recovery plan w/o doing actual recovery");
            opts.addOption("f", "force", false, "Force recovery without confirmation");
            opts.addOption("l", "ledger", true, "Recover a specific ledger");
            opts.addOption("sk", "skipOpenLedgers", false, "Skip recovering open ledgers");
            opts.addOption("d", "deleteCookie", false, "Delete cookie node for the bookie.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Recover the ledger data for failed bookie.";
        }

        @Override
        String getUsage() {
            return "recover [-deleteCookie] <bookieSrc[:bookieSrc]>";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length < 1) {
                throw new MissingArgumentException(
                        "'bookieSrc' argument required");
            }
            if (args.length > 1) {
                System.err.println("The provided bookie dest " + args[1] + " will be ignored!");
            }
            boolean query = cmdLine.hasOption("q");
            boolean dryrun = cmdLine.hasOption("dr");
            boolean force = cmdLine.hasOption("f");
            boolean skipOpenLedgers = cmdLine.hasOption("sk");
            boolean removeCookies = !dryrun && cmdLine.hasOption("d");

            Long ledgerId = getOptionLedgerIdValue(cmdLine, "ledger", -1);

            // Get bookies list
            final String[] bookieStrs = args[0].split(",");
            final Set<BookieSocketAddress> bookieAddrs = new HashSet<>();
            for (String bookieStr : bookieStrs) {
                final String bookieStrParts[] = bookieStr.split(":");
                if (bookieStrParts.length != 2) {
                    System.err.println("BookieSrcs has invalid bookie address format (host:port expected) : "
                            + bookieStr);
                    return -1;
                }
                bookieAddrs.add(new BookieSocketAddress(bookieStrParts[0],
                        Integer.parseInt(bookieStrParts[1])));
            }

            if (!force) {
                System.err.println("Bookies : " + bookieAddrs);
                if (!IOUtils.confirmPrompt("Are you sure to recover them : (Y/N)")) {
                    System.err.println("Give up!");
                    return -1;
                }
            }

            LOG.info("Constructing admin");
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            LOG.info("Construct admin : {}", admin);
            try {
                if (query) {
                    return bkQuery(admin, bookieAddrs);
                }
                if (-1 != ledgerId) {
                    return bkRecoveryLedger(admin, ledgerId, bookieAddrs, dryrun, skipOpenLedgers, removeCookies);
                }
                return bkRecovery(admin, bookieAddrs, dryrun, skipOpenLedgers, removeCookies);
            } finally {
                admin.close();
            }
        }

        private int bkQuery(BookKeeperAdmin bkAdmin, Set<BookieSocketAddress> bookieAddrs)
                throws InterruptedException, BKException {
            SortedMap<Long, LedgerMetadata> ledgersContainBookies =
                    bkAdmin.getLedgersContainBookies(bookieAddrs);
            System.err.println("NOTE: Bookies in inspection list are marked with '*'.");
            for (Map.Entry<Long, LedgerMetadata> ledger : ledgersContainBookies.entrySet()) {
                System.out.println("ledger " + ledger.getKey() + " : " + ledger.getValue().getState());
                Map<Long, Integer> numBookiesToReplacePerEnsemble =
                        inspectLedger(ledger.getValue(), bookieAddrs);
                System.out.print("summary: [");
                for (Map.Entry<Long, Integer> entry : numBookiesToReplacePerEnsemble.entrySet()) {
                    System.out.print(entry.getKey() + "=" + entry.getValue() + ", ");
                }
                System.out.println("]");
                System.out.println();
            }
            System.err.println("Done");
            return 0;
        }

        private Map<Long, Integer> inspectLedger(LedgerMetadata metadata, Set<BookieSocketAddress> bookiesToInspect) {
            Map<Long, Integer> numBookiesToReplacePerEnsemble = new TreeMap<Long, Integer>();
            for (Map.Entry<Long, ? extends List<BookieSocketAddress>> ensemble : metadata.getEnsembles().entrySet()) {
                List<BookieSocketAddress> bookieList = ensemble.getValue();
                System.out.print(ensemble.getKey() + ":\t");
                int numBookiesToReplace = 0;
                for (BookieSocketAddress bookie : bookieList) {
                    System.out.print(bookie);
                    if (bookiesToInspect.contains(bookie)) {
                        System.out.print("*");
                        ++numBookiesToReplace;
                    } else {
                        System.out.print(" ");
                    }
                    System.out.print(" ");
                }
                System.out.println();
                numBookiesToReplacePerEnsemble.put(ensemble.getKey(), numBookiesToReplace);
            }
            return numBookiesToReplacePerEnsemble;
        }

        private int bkRecoveryLedger(BookKeeperAdmin bkAdmin,
                                     long lid,
                                     Set<BookieSocketAddress> bookieAddrs,
                                     boolean dryrun,
                                     boolean skipOpenLedgers,
                                     boolean removeCookies)
                throws InterruptedException, BKException, KeeperException {
            bkAdmin.recoverBookieData(lid, bookieAddrs, dryrun, skipOpenLedgers);
            if (removeCookies) {
                deleteCookies(bkAdmin.getConf(), bookieAddrs);
            }
            return 0;
        }

        private int bkRecovery(BookKeeperAdmin bkAdmin,
                               Set<BookieSocketAddress> bookieAddrs,
                               boolean dryrun,
                               boolean skipOpenLedgers,
                               boolean removeCookies)
                throws InterruptedException, BKException, KeeperException {
            bkAdmin.recoverBookieData(bookieAddrs, dryrun, skipOpenLedgers);
            if (removeCookies) {
                deleteCookies(bkAdmin.getConf(), bookieAddrs);
            }
            return 0;
        }

        private void deleteCookies(ClientConfiguration conf,
                                   Set<BookieSocketAddress> bookieAddrs) throws BKException {
            ServerConfiguration serverConf = new ServerConfiguration(conf);
            try {
                runFunctionWithRegistrationManager(serverConf, rm -> {
                    try {
                        for (BookieSocketAddress addr : bookieAddrs) {
                            deleteCookie(rm, addr);
                        }
                    } catch (Exception e) {
                        throw new UncheckedExecutionException(e);
                    }
                    return null;
                });
            } catch (Exception e) {
                Throwable cause = e;
                if (e instanceof UncheckedExecutionException) {
                    cause = e.getCause();
                }
                if (cause instanceof BKException) {
                    throw (BKException) cause;
                } else {
                    BKException bke = new MetaStoreException();
                    bke.initCause(bke);
                    throw bke;
                }
            }
        }

        private void deleteCookie(RegistrationManager rm,
                                  BookieSocketAddress bookieSrc) throws BookieException {
            try {
                Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bookieSrc);
                cookie.getValue().deleteFromRegistrationManager(rm, bookieSrc, cookie.getVersion());
            } catch (CookieNotFoundException nne) {
                LOG.warn("No cookie to remove for {} : ", bookieSrc, nne);
            }
        }

    }

    /**
     * Ledger Command Handles ledger related operations.
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
                ledgerId = ledgerIdFormatter.readLedgerId(leftArgs[0]);
            } catch (IllegalArgumentException iae) {
                System.err.println("ERROR: invalid ledger id " + leftArgs[0]);
                printUsage();
                return -1;
            }

            if (bkConf.getLedgerStorageClass().equals(DbLedgerStorage.class.getName())) {
                // dump ledger info
                try {
                    DbLedgerStorage.readLedgerIndexEntries(ledgerId, bkConf,
                            (currentEntry, entryLogId, position) -> System.out.println(
                                    "entry " + currentEntry + "\t:\t(log: " + entryLogId + ", pos: " + position + ")"));
                } catch (IOException e) {
                    System.err.printf("ERROR: initializing dbLedgerStorage %s", e.getMessage());
                    return -1;
                }
            } else {
                if (printMeta) {
                    // print meta
                    readLedgerMeta(ledgerId);
                }
                // dump ledger info
                readLedgerIndexEntries(ledgerId);
            }

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
     * Command for reading ledger entries.
     */
    class ReadLedgerEntriesCmd extends MyCommand {
        Options lOpts = new Options();

        ReadLedgerEntriesCmd() {
            super(CMD_READ_LEDGER_ENTRIES);
            lOpts.addOption("m", "msg", false, "Print message body");
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
            lOpts.addOption("fe", "firstentryid", true, "First EntryID");
            lOpts.addOption("le", "lastentryid", true, "Last EntryID");
            lOpts.addOption("r", "force-recovery", false,
                "Ensure the ledger is properly closed before reading");
            lOpts.addOption("b", "bookie", true, "Only read from a specific bookie");
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        String getDescription() {
            return "Read a range of entries from a ledger.";
        }

        @Override
        String getUsage() {
            return "readledger  [-bookie <address:port>]  [-msg] -ledgerid <ledgerid> "
                    + "[-firstentryid <firstentryid> [-lastentryid <lastentryid>]] "
                    + "[-force-recovery]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            final long ledgerId = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (ledgerId == -1) {
                System.err.println("Must specify a ledger id");
                return -1;
            }

            final long firstEntry = getOptionLongValue(cmdLine, "firstentryid", 0);
            long lastEntry = getOptionLongValue(cmdLine, "lastentryid", -1);

            boolean printMsg = cmdLine.hasOption("m");
            boolean forceRecovery = cmdLine.hasOption("r");
            final BookieSocketAddress bookie;
            if (cmdLine.hasOption("b")) {
                // A particular bookie was specified
                bookie = new BookieSocketAddress(cmdLine.getOptionValue("b"));
            } else {
                bookie = null;
            }

            ClientConfiguration conf = new ClientConfiguration();
            conf.addConfiguration(bkConf);

            try (BookKeeperAdmin bk = new BookKeeperAdmin(conf)) {
                if (forceRecovery) {
                    // Force the opening of the ledger to trigger recovery
                    try (LedgerHandle lh = bk.openLedger(ledgerId)) {
                        if (lastEntry == -1 || lastEntry > lh.getLastAddConfirmed()) {
                            lastEntry = lh.getLastAddConfirmed();
                        }
                    }
                }

                if (bookie == null) {
                    // No bookie was specified, use normal bk client
                    Iterator<LedgerEntry> entries = bk.readEntries(ledgerId, firstEntry, lastEntry).iterator();
                    while (entries.hasNext()) {
                        LedgerEntry entry = entries.next();
                        formatEntry(entry, printMsg);
                    }
                } else {
                    // Use BookieClient to target a specific bookie
                    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
                    OrderedExecutor executor = OrderedExecutor.newBuilder()
                        .numThreads(1)
                        .name("BookieClientScheduler")
                        .build();

                    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                        new DefaultThreadFactory("BookKeeperClientSchedulerPool"));

                    BookieClient bookieClient = new BookieClientImpl(conf, eventLoopGroup, executor,
                        scheduler, NullStatsLogger.INSTANCE);

                    LongStream.range(firstEntry, lastEntry).forEach(entryId -> {
                        CompletableFuture<Void> future = new CompletableFuture<>();

                        bookieClient.readEntry(bookie, ledgerId, entryId,
                            (rc, ledgerId1, entryId1, buffer, ctx) -> {
                                if (rc != BKException.Code.OK) {
                                    LOG.error("Failed to read entry {} -- {}", entryId1, BKException.getMessage(rc));
                                    future.completeExceptionally(BKException.create(rc));
                                    return;
                                }

                                System.out.println("--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId)
                                    + ", Eid=" + entryId + " ---------");
                                if (printMsg) {
                                    System.out.println("Data: " + ByteBufUtil.prettyHexDump(buffer));
                                }

                                buffer.release();
                                future.complete(null);
                                }, null, BookieProtocol.FLAG_NONE);

                        try {
                            future.get();
                        } catch (Exception e) {
                            LOG.error("Error future.get while reading entries from ledger {}", ledgerId, e);
                        }
                    });

                    eventLoopGroup.shutdownGracefully();
                    executor.shutdown();
                    bookieClient.close();
                }
            }

            return 0;
        }

    }

    /**
     * Command for listing underreplicated ledgers.
     */
    class ListUnderreplicatedCmd extends MyCommand {
        Options opts = new Options();

        public ListUnderreplicatedCmd() {
            super(CMD_LISTUNDERREPLICATED);
            opts.addOption("missingreplica", true, "Bookie Id of missing replica");
            opts.addOption("excludingmissingreplica", true, "Bookie Id of missing replica to ignore");
            opts.addOption("printmissingreplica", false, "Whether to print missingreplicas list?");
            opts.addOption("printreplicationworkerid", false, "Whether to print replicationworkerid?");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "List ledgers marked as underreplicated, with optional options to specify missingreplica"
                    + " (BookieId) and to exclude missingreplica.";
        }

        @Override
        String getUsage() {
            return "listunderreplicated [[-missingreplica <bookieaddress>]"
                    + " [-excludingmissingreplica <bookieaddress>]] [-printmissingreplica] [-printreplicationworkerid]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {

            final String includingBookieId = cmdLine.getOptionValue("missingreplica");
            final String excludingBookieId = cmdLine.getOptionValue("excludingmissingreplica");
            final boolean printMissingReplica = cmdLine.hasOption("printmissingreplica");
            final boolean printReplicationWorkerId = cmdLine.hasOption("printreplicationworkerid");

            final Predicate<List<String>> predicate;
            if (!StringUtils.isBlank(includingBookieId) && !StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> (replicasList.contains(includingBookieId)
                        && !replicasList.contains(excludingBookieId));
            } else if (!StringUtils.isBlank(includingBookieId)) {
                predicate = replicasList -> replicasList.contains(includingBookieId);
            } else if (!StringUtils.isBlank(excludingBookieId)) {
                predicate = replicasList -> !replicasList.contains(excludingBookieId);
            } else {
                predicate = null;
            }

            runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
                LedgerUnderreplicationManager underreplicationManager;
                try {
                    underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                } catch (KeeperException | CompatibilityException e) {
                    throw new UncheckedExecutionException("Failed to new ledger underreplicated manager", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new UncheckedExecutionException("Interrupted on newing ledger underreplicated manager", e);
                }
                Iterator<UnderreplicatedLedger> iter = underreplicationManager.listLedgersToRereplicate(predicate);
                while (iter.hasNext()) {
                    UnderreplicatedLedger underreplicatedLedger = iter.next();
                    long urLedgerId = underreplicatedLedger.getLedgerId();
                    System.out.println(ledgerIdFormatter.formatLedgerId(urLedgerId));
                    long ctime = underreplicatedLedger.getCtime();
                    if (ctime != UnderreplicatedLedger.UNASSIGNED_CTIME) {
                        System.out.println("\tCtime : " + ctime);
                    }
                    if (printMissingReplica) {
                        underreplicatedLedger.getReplicaList().forEach((missingReplica) -> {
                            System.out.println("\tMissingReplica : " + missingReplica);
                        });
                    }
                    if (printReplicationWorkerId) {
                        try {
                            String replicationWorkerId = underreplicationManager
                                    .getReplicationWorkerIdRereplicatingLedger(urLedgerId);
                            if (replicationWorkerId != null) {
                                System.out.println("\tReplicationWorkerId : " + replicationWorkerId);
                            }
                        } catch (UnavailableException e) {
                            LOG.error("Failed to get ReplicationWorkerId rereplicating ledger {} -- {}", urLedgerId,
                                    e.getMessage());
                        }
                    }
                }
                return null;
            });

            return 0;
        }
    }

    static final int LIST_BATCH_SIZE = 1000;

    /**
     * Command to list all ledgers in the cluster.
     */
    class ListLedgersCmd extends MyCommand {
        Options lOpts = new Options();

        ListLedgersCmd() {
            super(CMD_LISTLEDGERS);
            lOpts.addOption("m", "meta", false, "Print metadata");
            lOpts.addOption("bookieid", true, "List ledgers residing in this bookie");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final boolean printMeta = cmdLine.hasOption("m");
            final String bookieidToBePartOfEnsemble = cmdLine.getOptionValue("bookieid");
            final BookieSocketAddress bookieAddress = StringUtils.isBlank(bookieidToBePartOfEnsemble) ? null
                    : new BookieSocketAddress(bookieidToBePartOfEnsemble);

            runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
                try (LedgerManager ledgerManager = mFactory.newLedgerManager()) {

                    final AtomicInteger returnCode = new AtomicInteger(BKException.Code.OK);
                    final CountDownLatch processDone = new CountDownLatch(1);

                    Processor<Long> ledgerProcessor = new Processor<Long>() {
                        @Override
                        public void process(Long ledgerId, VoidCallback cb) {
                            if (!printMeta && (bookieAddress == null)) {
                                printLedgerMetadata(ledgerId, null, false);
                                cb.processResult(BKException.Code.OK, null, null);
                            } else {
                                GenericCallback<LedgerMetadata> gencb = new GenericCallback<LedgerMetadata>() {
                                    @Override
                                    public void operationComplete(int rc, LedgerMetadata ledgerMetadata) {
                                        if (rc == BKException.Code.OK) {
                                            if ((bookieAddress == null)
                                                    || BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId,
                                                            bookieAddress, ledgerMetadata)) {
                                                /*
                                                 * the print method has to be in
                                                 * synchronized scope, otherwise
                                                 * output of printLedgerMetadata
                                                 * could interleave since this
                                                 * callback for different
                                                 * ledgers can happen in
                                                 * different threads.
                                                 */
                                                synchronized (BookieShell.this) {
                                                    printLedgerMetadata(ledgerId, ledgerMetadata, printMeta);
                                                }
                                            }
                                        } else if (rc == BKException.Code.NoSuchLedgerExistsException) {
                                            rc = BKException.Code.OK;
                                        } else {
                                            LOG.error("Unable to read the ledger: " + ledgerId + " information");
                                        }
                                        cb.processResult(rc, null, null);
                                    }
                                };
                                ledgerManager.readLedgerMetadata(ledgerId, gencb);
                            }
                        }
                    };

                    ledgerManager.asyncProcessLedgers(ledgerProcessor, new AsyncCallback.VoidCallback() {
                        @Override
                        public void processResult(int rc, String s, Object obj) {
                            returnCode.set(rc);
                            processDone.countDown();
                        }
                    }, null, BKException.Code.OK, BKException.Code.ReadException);

                    processDone.await();
                    if (returnCode.get() != BKException.Code.OK) {
                        LOG.error("Received error return value while processing ledgers: {}", returnCode.get());
                        throw BKException.create(returnCode.get());
                    }

                } catch (Exception ioe) {
                    LOG.error("Received Exception while processing ledgers", ioe);
                    throw new UncheckedExecutionException(ioe);
                }
                return null;
            });

            return 0;
        }

        @Override
        String getDescription() {
            return "List all ledgers on the cluster (this may take a long time).";
        }

        @Override
        String getUsage() {
            return "listledgers  [-meta] [-bookieid <bookieaddress>]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    void printLedgerMetadata(long ledgerId, LedgerMetadata md, boolean printMeta) {
        System.out.println("ledgerID: " + ledgerIdFormatter.formatLedgerId(ledgerId));
        if (printMeta) {
            System.out.println(new String(md.serialize(), UTF_8));
        }
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

        @Override
        public void operationComplete(int rc, LedgerMetadata result) {
            if (rc != 0) {
                setException(BKException.create(rc));
            } else {
                set(result);
            }
        }
    }

    /**
     * Print the metadata for a ledger.
     */
    class LedgerMetadataCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerMetadataCmd() {
            super(CMD_LEDGERMETADATA);
            lOpts.addOption("l", "ledgerid", true, "Ledger ID");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final long lid = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (lid == -1) {
                System.err.println("Must specify a ledger id");
                return -1;
            }

            runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
                try (LedgerManager m = mFactory.newLedgerManager()) {
                    ReadMetadataCallback cb = new ReadMetadataCallback(lid);
                    m.readLedgerMetadata(lid, cb);
                    printLedgerMetadata(lid, cb.get(), true);
                } catch (Exception e) {
                    throw new UncheckedExecutionException(e);
                }
                return null;
            });

            return 0;
        }

        @Override
        String getDescription() {
            return "Print the metadata for a ledger.";
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
     * Simple test to create a ledger and write to it.
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
            int ensemble = getOptionIntValue(cmdLine, "ensemble", 3);
            int writeQuorum = getOptionIntValue(cmdLine, "writeQuorum", 2);
            int ackQuorum = getOptionIntValue(cmdLine, "ackQuorum", 2);
            int numEntries = getOptionIntValue(cmdLine, "numEntries", 1000);

            SimpleTestCommand.Flags flags = new SimpleTestCommand.Flags()
                .ensembleSize(ensemble)
                .writeQuorumSize(writeQuorum)
                .ackQuorumSize(ackQuorum)
                .numEntries(numEntries);

            SimpleTestCommand command = new SimpleTestCommand(flags);

            command.apply(bkConf, flags);
            return 0;
        }

        @Override
        String getDescription() {
            return "Simple test to create a ledger and write entries to it.";
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
     * Command to run a bookie sanity test.
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
            int timeoutSecs = getOptionIntValue(cmdLine, "timeout", 1);

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
            rlOpts.addOption("e", "entryid", true, "Entry ID");
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

            final long lId = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
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
            return "readlog      [-msg] <entry_log_id | entry_log_file_name> [-ledgerid <ledgerid> "
                    + "[-entryid <entryid>]] [-startpos <startEntryLogBytePos> [-endpos <endEntryLogBytePos>]]";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }

    /**
     * Command to print metadata of entrylog.
     */
    class ReadLogMetadataCmd extends MyCommand {
        Options rlOpts = new Options();

        ReadLogMetadataCmd() {
            super(CMD_READLOGMETADATA);
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                LOG.error("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
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
                    LOG.error("ERROR: invalid entry log file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                logId = Long.parseLong(idString, 16);
            }

            printEntryLogMetadata(logId);

            return 0;
        }

        @Override
        String getDescription() {
            return "Prints entrylog's metadata";
        }

        @Override
        String getUsage() {
            return "readlogmetadata <entry_log_id | entry_log_file_name>";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }

    /**
     * Command to read journal files.
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
     * Command to print last log mark.
     */
    class LastMarkCmd extends MyCommand {
        LastMarkCmd() {
            super(CMD_LASTMARK);
        }

        @Override
        public int runCmd(CommandLine c) throws Exception {
            LastMarkCommand command = new LastMarkCommand();
            command.apply(bkConf, new CliFlags());
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
     * List available bookies.
     */
    class ListBookiesCmd extends MyCommand {
        Options opts = new Options();

        ListBookiesCmd() {
            super(CMD_LISTBOOKIES);
            opts.addOption("rw", "readwrite", false, "Print readwrite bookies");
            opts.addOption("ro", "readonly", false, "Print readonly bookies");
            // @deprecated 'rw'/'ro' option print both hostname and ip, so this option is not needed anymore
            opts.addOption("h", "hostnames", false, "Also print hostname of the bookie");
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

            ListBookiesCommand.Flags flags = new ListBookiesCommand.Flags()
                .readwrite(readwrite)
                .readonly(readonly);

            ListBookiesCommand command = new ListBookiesCommand(flags);

            command.apply(bkConf, flags);
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
                    System.out.println(journalFile.getCanonicalPath());
                }
                System.out.println();
            }
            if (all || entrylog) {
                File[] ledgerDirs = bkConf.getLedgerDirs();
                List<File> ledgerFiles = listFilesAndSort(ledgerDirs, "log");
                System.out.println("--------- Printing the list of EntryLog/Ledger Files ---------");
                for (File ledgerFile : ledgerFiles) {
                    System.out.println(ledgerFile.getCanonicalPath());
                }
                System.out.println();
            }
            if (all || index) {
                File[] indexDirs = (bkConf.getIndexDirs() == null) ? bkConf.getLedgerDirs() : bkConf.getIndexDirs();
                List<File> indexFiles = listFilesAndSort(indexDirs, "idx");
                System.out.println("--------- Printing the list of Index Files ---------");
                for (File indexFile : indexFiles) {
                    System.out.println(indexFile.getCanonicalPath());
                }
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "List the files in JournalDirectory/LedgerDirectories/IndexDirectories.";
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
     * Command to print help message.
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
     * Command for administration of autorecovery.
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

            runFunctionWithLedgerManagerFactory(bkConf, mFactory -> {
                try {
                    try (LedgerUnderreplicationManager underreplicationManager =
                             mFactory.newLedgerUnderreplicationManager()) {
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
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new UncheckedExecutionException(e);
                } catch (KeeperException | ReplicationException e) {
                    throw new UncheckedExecutionException(e);
                }
                return null;
            });

            return 0;
        }
    }

    /**
     * Setter and Getter for LostBookieRecoveryDelay value (in seconds) in metadata store.
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
            return "Setter and Getter for LostBookieRecoveryDelay value (in seconds) in metadata store.";
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
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                if (getter) {
                    int lostBookieRecoveryDelay = admin.getLostBookieRecoveryDelay();
                    LOG.info("LostBookieRecoveryDelay value in ZK: {}", String.valueOf(lostBookieRecoveryDelay));
                } else {
                    int lostBookieRecoveryDelay = Integer.parseInt(cmdLine.getOptionValue("set"));
                    admin.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);
                    LOG.info("Successfully set LostBookieRecoveryDelay value in ZK: {}",
                            String.valueOf(lostBookieRecoveryDelay));
                }
            } finally {
                if (admin != null) {
                    admin.close();
                }
            }
            return 0;
        }
    }


    /**
     * Print which node has the auditor lock.
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
            return "Print the node which holds the auditor lock.";
        }

        @Override
        String getUsage() {
            return "whoisauditor";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            ZooKeeper zk = null;
            try {
                String metadataServiceUri = bkConf.getMetadataServiceUri();
                String zkServers = ZKMetadataDriverBase.getZKServersFromServiceUri(URI.create(metadataServiceUri));
                zk = ZooKeeperClient.newBuilder()
                        .connectString(zkServers)
                        .sessionTimeoutMs(bkConf.getZkTimeout())
                        .build();
                BookieSocketAddress bookieId = AuditorElector.getCurrentAuditor(bkConf, zk);
                if (bookieId == null) {
                    LOG.info("No auditor elected");
                    return -1;
                }
                LOG.info("Auditor: " + getBookieSocketAddrStringRepresentation(bookieId));
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    /**
     * Prints the instanceid of the cluster.
     */
    class WhatIsInstanceId extends MyCommand {
        Options opts = new Options();

        public WhatIsInstanceId() {
            super(CMD_WHATISINSTANCEID);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Print the instanceid of the cluster";
        }

        @Override
        String getUsage() {
            return "whatisinstanceid";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            runFunctionWithRegistrationManager(bkConf, rm -> {
                String readInstanceId = null;
                try {
                    readInstanceId = rm.getClusterInstanceId();
                } catch (BookieException e) {
                    throw new UncheckedExecutionException(e);
                }
                LOG.info("Metadata Service Uri: {} InstanceId: {}",
                    bkConf.getMetadataServiceUriUnchecked(), readInstanceId);
                return null;
            });
            return 0;
        }
    }

    /**
     * Update cookie command.
     */
    class UpdateCookieCmd extends MyCommand {
        Options opts = new Options();
        private static final String BOOKIEID = "bookieId";
        private static final String EXPANDSTORAGE = "expandstorage";
        private static final String LIST = "list";
        private static final String DELETE = "delete";
        private static final String HOSTNAME = "hostname";
        private static final String IP = "ip";
        private static final String FORCE = "force";

        UpdateCookieCmd() {
            super(CMD_UPDATECOOKIE);
            opts.addOption("b", BOOKIEID, true, "Bookie Id");
            opts.addOption("e", EXPANDSTORAGE, false, "Expand Storage");
            opts.addOption("l", LIST, false, "List paths of all the cookies present locally and on zookkeeper");
            @SuppressWarnings("static-access")
            Option deleteOption = OptionBuilder.withLongOpt(DELETE).hasOptionalArgs(1)
                    .withDescription("Delete cookie both locally and in ZooKeeper").create("d");
            opts.addOption(deleteOption);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Command to update cookie"
                    + "bookieId - Update bookie id in cookie\n"
                    + "expandstorage - Add new empty ledger/index directories."
                    + " Update the directories info in the conf file before running the command\n"
                    + "list - list the local cookie files path and ZK cookiePath "
                    + "delete - Delete cookies locally and in zookeeper";
        }

        @Override
        String getUsage() {
            return "updatecookie [-bookieId <hostname|ip>] [-expandstorage] [-list] [-delete <force>]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            int retValue = -1;
            Option[] options = cmdLine.getOptions();
            if (options.length != 1) {
                LOG.error("Invalid command!");
                this.printUsage();
                return -1;
            }
            Option thisCommandOption = options[0];
            if (thisCommandOption.getLongOpt().equals(BOOKIEID)) {
                final String bookieId = cmdLine.getOptionValue(BOOKIEID);
                if (StringUtils.isBlank(bookieId)) {
                    LOG.error("Invalid argument list!");
                    this.printUsage();
                    return -1;
                }
                if (!StringUtils.equals(bookieId, HOSTNAME) && !StringUtils.equals(bookieId, IP)) {
                    LOG.error("Invalid option value:" + bookieId);
                    this.printUsage();
                    return -1;
                }
                boolean useHostName = getOptionalValue(bookieId, HOSTNAME);
                if (!bkConf.getUseHostNameAsBookieID() && useHostName) {
                    LOG.error(
                            "Expects config useHostNameAsBookieID=true as the option value passed is 'hostname'");
                    return -1;
                } else if (bkConf.getUseHostNameAsBookieID() && !useHostName) {
                    LOG.error("Expects configuration useHostNameAsBookieID=false as the option value passed is 'ip'");
                    return -1;
                }
                retValue = updateBookieIdInCookie(bookieId, useHostName);
            } else if (thisCommandOption.getLongOpt().equals(EXPANDSTORAGE)) {
                bkConf.setAllowStorageExpansion(true);
                return expandStorage();
            } else if (thisCommandOption.getLongOpt().equals(LIST)) {
                return listOrDeleteCookies(false, false);
            } else if (thisCommandOption.getLongOpt().equals(DELETE)) {
                boolean force = false;
                String optionValue = thisCommandOption.getValue();
                if (!StringUtils.isEmpty(optionValue) && optionValue.equals(FORCE)) {
                    force = true;
                }
                return listOrDeleteCookies(true, force);
            } else {
                LOG.error("Invalid command!");
                this.printUsage();
                return -1;
            }
            return retValue;
        }

        private int updateBookieIdInCookie(final String bookieId, final boolean useHostname)
                throws Exception {
            return runFunctionWithRegistrationManager(bkConf, rm -> {
                try {
                    ServerConfiguration conf = new ServerConfiguration(bkConf);
                    String newBookieId = Bookie.getBookieAddress(conf).toString();
                    // read oldcookie
                    Versioned<Cookie> oldCookie = null;
                    try {
                        conf.setUseHostNameAsBookieID(!useHostname);
                        oldCookie = Cookie.readFromRegistrationManager(rm, conf);
                    } catch (CookieNotFoundException nne) {
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
                            Cookie.readFromRegistrationManager(rm, conf);
                            // since newcookie exists, just do cleanup of oldcookie and return
                            conf.setUseHostNameAsBookieID(!useHostname);
                            oldCookie.getValue().deleteFromRegistrationManager(rm, conf, oldCookie.getVersion());
                            return 0;
                        } catch (CookieNotFoundException nne) {
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
                    newCookie.writeToRegistrationManager(rm, conf, Version.NEW);

                    // delete oldcookie
                    conf.setUseHostNameAsBookieID(!useHostname);
                    oldCookie.getValue().deleteFromRegistrationManager(rm, conf, oldCookie.getVersion());
                    return 0;
                } catch (IOException | BookieException ioe) {
                    LOG.error("IOException during cookie updation!", ioe);
                    return -1;
                }
            });
        }

        private int expandStorage() throws Exception {
            return runFunctionWithMetadataBookieDriver(bkConf, driver -> {
                List<File> allLedgerDirs = Lists.newArrayList();
                allLedgerDirs.addAll(Arrays.asList(ledgerDirectories));
                if (indexDirectories != ledgerDirectories) {
                    allLedgerDirs.addAll(Arrays.asList(indexDirectories));
                }

                try {
                    Bookie.checkEnvironmentWithStorageExpansion(
                        bkConf, driver, Arrays.asList(journalDirectories), allLedgerDirs);
                    return 0;
                } catch (BookieException e) {
                    LOG.error("Exception while updating cookie for storage expansion", e);
                    return -1;
                }
            });
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

        private int listOrDeleteCookies(boolean delete, boolean force) throws Exception {
            BookieSocketAddress bookieAddress = Bookie.getBookieAddress(bkConf);
            File[] journalDirs = bkConf.getJournalDirs();
            File[] ledgerDirs = bkConf.getLedgerDirs();
            File[] indexDirs = bkConf.getIndexDirs();
            File[] allDirs = ArrayUtils.addAll(journalDirs, ledgerDirs);
            if (indexDirs != null) {
                allDirs = ArrayUtils.addAll(allDirs, indexDirs);
            }

            File[] allCurDirs = Bookie.getCurrentDirectories(allDirs);
            List<File> allVersionFiles = new LinkedList<File>();
            File versionFile;
            for (File curDir : allCurDirs) {
                versionFile = new File(curDir, BookKeeperConstants.VERSION_FILENAME);
                if (versionFile.exists()) {
                    allVersionFiles.add(versionFile);
                }
            }

            if (!allVersionFiles.isEmpty()) {
                if (delete) {
                    boolean confirm = force;
                    if (!confirm) {
                        confirm = IOUtils.confirmPrompt("Are you sure you want to delete Cookies locally?");
                    }
                    if (confirm) {
                        for (File verFile : allVersionFiles) {
                            if (!verFile.delete()) {
                                LOG.error(
                                        "Failed to delete Local cookie file {}. So aborting deletecookie of Bookie: {}",
                                        verFile, bookieAddress);
                                return -1;
                            }
                        }
                        LOG.info("Deleted Local Cookies of Bookie: {}", bookieAddress);
                    } else {
                        LOG.info("Skipping deleting local Cookies of Bookie: {}", bookieAddress);
                    }
                } else {
                    LOG.info("Listing local Cookie Files of Bookie: {}", bookieAddress);
                    for (File verFile : allVersionFiles) {
                        LOG.info(verFile.getCanonicalPath());
                    }
                }
            } else {
                LOG.info("No local cookies for Bookie: {}", bookieAddress);
            }

            return runFunctionWithRegistrationManager(bkConf, rm -> {
                try {
                    Versioned<Cookie> cookie = null;
                    try {
                        cookie = Cookie.readFromRegistrationManager(rm, bookieAddress);
                    } catch (CookieNotFoundException nne) {
                        LOG.info("No cookie for {} in metadata store", bookieAddress);
                        return 0;
                    }

                    if (delete) {
                        boolean confirm = force;
                        if (!confirm) {
                            confirm = IOUtils.confirmPrompt(
                                "Are you sure you want to delete Cookies from metadata store?");
                        }

                        if (confirm) {
                            cookie.getValue().deleteFromRegistrationManager(rm, bkConf, cookie.getVersion());
                            LOG.info("Deleted Cookie from metadata store for Bookie: {}", bookieAddress);
                        } else {
                            LOG.info("Skipping deleting cookie from metadata store for Bookie: {}", bookieAddress);
                        }
                    }
                } catch (BookieException | IOException e) {
                    return -1;
                }
                return 0;
            });
        }
    }

    /**
     * Update ledger command.
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
            return "Update bookie id in ledgers (this may take a long time).";
        }

        @Override
        String getUsage() {
            return "updateledgers -bookieId <hostname|ip> [-updatespersec N] [-limit N] [-verbose true/false] "
                    + "[-printprogress N]";
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
            } catch (IOException e) {
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
            final long lid = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (lid == -1) {
                System.err.println("Must specify a ledger id");
                return -1;
            }

            boolean force = cmdLine.hasOption("f");
            boolean confirm = false;
            if (!force) {
                confirm = IOUtils.confirmPrompt(
                        "Are you sure to delete Ledger : " + ledgerIdFormatter.formatLedgerId(lid) + "?");
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
            return "Delete a ledger.";
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
            return "Retrieve bookie info such as free and total disk space.";
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
            String unit[] = {"", "KB", "MB", "GB", "TB"};
            int cnt = 0;
            double d = val;
            while (d >= 1000 && cnt < unit.length - 1) {
                d = d / 1000;
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
            long totalFree = 0, total = 0;
            for (Map.Entry<BookieSocketAddress, BookieInfo> e : map.entrySet()) {
                BookieInfo bInfo = e.getValue();
                BookieSocketAddress bookieId = e.getKey();
                System.out.println(getBookieSocketAddrStringRepresentation(bookieId) + ":\tFree: "
                        + bInfo.getFreeDiskSpace() + getReadable(bInfo.getFreeDiskSpace()) + "\tTotal: "
                        + bInfo.getTotalDiskSpace() + getReadable(bInfo.getTotalDiskSpace()));
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
     * Command to trigger AuditTask by resetting lostBookieRecoveryDelay to its current value.
     */
    class TriggerAuditCmd extends MyCommand {
        Options opts = new Options();

        TriggerAuditCmd() {
            super(CMD_TRIGGERAUDIT);
        }

        @Override
        String getDescription() {
            return "Force trigger the Audit by resetting the lostBookieRecoveryDelay.";
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
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                admin.triggerAudit();
            } finally {
                if (admin != null) {
                    admin.close();
                }
            }
            return 0;
        }
    }

    /**
     * Command to trigger AuditTask by resetting lostBookieRecoveryDelay and
     * then make sure the ledgers stored in the bookie are properly replicated
     * and Cookie of the decommissioned bookie should be deleted from metadata
     * server.
     */
    class DecommissionBookieCmd extends MyCommand {
        Options lOpts = new Options();

        DecommissionBookieCmd() {
            super(CMD_DECOMMISSIONBOOKIE);
            lOpts.addOption("bookieid", true, "decommission a remote bookie");
        }

        @Override
        String getDescription() {
            return "Force trigger the Audittask and make sure all the ledgers stored in the decommissioning bookie"
                    + " are replicated and cookie of the decommissioned bookie is deleted from metadata server.";
        }

        @Override
        String getUsage() {
            return CMD_DECOMMISSIONBOOKIE + " [-bookieid <bookieaddress>]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                final String remoteBookieidToDecommission = cmdLine.getOptionValue("bookieid");
                final BookieSocketAddress bookieAddressToDecommission = (StringUtils
                        .isBlank(remoteBookieidToDecommission) ? Bookie.getBookieAddress(bkConf)
                                : new BookieSocketAddress(remoteBookieidToDecommission));
                admin.decommissionBookie(bookieAddressToDecommission);
                LOG.info("The ledgers stored in the given decommissioning bookie: {} are properly replicated",
                        bookieAddressToDecommission);
                runFunctionWithRegistrationManager(bkConf, rm -> {
                    try {
                        Versioned<Cookie> cookie = Cookie.readFromRegistrationManager(rm, bookieAddressToDecommission);
                        cookie.getValue().deleteFromRegistrationManager(rm, bookieAddressToDecommission,
                                cookie.getVersion());
                    } catch (CookieNotFoundException nne) {
                        LOG.warn("No cookie to remove for the decommissioning bookie: {}, it could be deleted already",
                                bookieAddressToDecommission, nne);
                    } catch (BookieException be) {
                        throw new UncheckedExecutionException(be.getMessage(), be);
                    }
                    return 0;
                });
                LOG.info("Cookie of the decommissioned bookie: {} is deleted successfully",
                        bookieAddressToDecommission);
                return 0;
            } catch (Exception e) {
                LOG.error("Received exception in DecommissionBookieCmd ", e);
                return -1;
            } finally {
                if (admin != null) {
                    admin.close();
                }
            }
        }
    }

    /**
     * A facility for reporting update ledger progress.
     */
    public interface UpdateLedgerNotifier {
        void progress(long updated, long issued);
    }


    /**
     * Convert bookie indexes from InterleavedStorage to DbLedgerStorage format.
     */
    class ConvertToDbStorageCmd extends MyCommand {
        Options opts = new Options();

        public ConvertToDbStorageCmd() {
            super(CMD_CONVERT_TO_DB_STORAGE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Convert bookie indexes from InterleavedStorage to DbLedgerStorage format";
        }

        @Override
        String getUsage() {
            return CMD_CONVERT_TO_DB_STORAGE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            LOG.info("=== Converting to DbLedgerStorage ===");
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(bkConf, bkConf.getLedgerDirs(),
                    new DiskChecker(bkConf.getDiskUsageThreshold(), bkConf.getDiskUsageWarnThreshold()));
            LedgerDirsManager ledgerIndexManager = new LedgerDirsManager(bkConf, bkConf.getLedgerDirs(),
                    new DiskChecker(bkConf.getDiskUsageThreshold(), bkConf.getDiskUsageWarnThreshold()));

            InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
            DbLedgerStorage dbStorage = new DbLedgerStorage();

            CheckpointSource checkpointSource = new CheckpointSource() {
                    @Override
                    public Checkpoint newCheckpoint() {
                        return Checkpoint.MAX;
                    }

                    @Override
                    public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                            throws IOException {
                    }
                };
            Checkpointer checkpointer = new Checkpointer() {
                @Override
                public void startCheckpoint(Checkpoint checkpoint) {
                    // No-op
                }

                @Override
                public void start() {
                    // no-op
                }
            };

            interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerIndexManager,
                    null, checkpointSource, checkpointer, NullStatsLogger.INSTANCE);
            dbStorage.initialize(conf, null, ledgerDirsManager, ledgerIndexManager, null,
                    checkpointSource, checkpointer, NullStatsLogger.INSTANCE);

            int convertedLedgers = 0;
            for (long ledgerId : interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Converting ledger {}", ledgerIdFormatter.formatLedgerId(ledgerId));
                }

                FileInfo fi = getFileInfo(ledgerId);

                Iterable<SortedMap<Long, Long>> entries = getLedgerIndexEntries(ledgerId);

                long numberOfEntries = dbStorage.addLedgerToIndex(ledgerId, fi.isFenced(), fi.getMasterKey(), entries);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("   -- done. fenced={} entries={}", fi.isFenced(), numberOfEntries);
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
            return 0;
        }
    }

    /**
     * Convert bookie indexes from DbLedgerStorage to InterleavedStorage format.
     */
    class ConvertToInterleavedStorageCmd extends MyCommand {
        Options opts = new Options();

        public ConvertToInterleavedStorageCmd() {
            super(CMD_CONVERT_TO_INTERLEAVED_STORAGE);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Convert bookie indexes from DbLedgerStorage to InterleavedStorage format";
        }

        @Override
        String getUsage() {
            return CMD_CONVERT_TO_INTERLEAVED_STORAGE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
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
                    public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                            throws IOException {
                    }
                };
            Checkpointer checkpointer = new Checkpointer() {
                @Override
                public void startCheckpoint(Checkpoint checkpoint) {
                    // No-op
                }

                @Override
                public void start() {
                    // no-op
                }
            };

            dbStorage.initialize(conf, null, ledgerDirsManager, ledgerIndexManager, null,
                        checkpointSource, checkpointer, NullStatsLogger.INSTANCE);
            interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerIndexManager,
                    null, checkpointSource, checkpointer, NullStatsLogger.INSTANCE);
            LedgerCache interleavedLedgerCache = interleavedStorage.ledgerCache;

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
            return 0;
        }
    }

    /**
     * Rebuild DbLedgerStorage locations index.
     */
    class RebuildDbLedgerLocationsIndexCmd extends MyCommand {
        Options opts = new Options();

        public RebuildDbLedgerLocationsIndexCmd() {
            super(CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX);
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Rebuild DbLedgerStorage locations index by scanning the entry logs";
        }

        @Override
        String getUsage() {
            return CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            LOG.info("=== Rebuilding bookie index ===");
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            new LocationsIndexRebuildOp(conf).initiate();
            LOG.info("-- Done rebuilding bookie index --");
            return 0;
        }
    }

    /**
     * Regenerate an index file for interleaved storage.
     */
    class RegenerateInterleavedStorageIndexFile extends MyCommand {
        Options opts = new Options();

        public RegenerateInterleavedStorageIndexFile() {
            super(CMD_REGENERATE_INTERLEAVED_STORAGE_INDEX_FILE);
            Option ledgerOption = new Option("l", "ledgerIds", true,
                                             "Ledger(s) whose index needs to be regenerated."
                                             + " Multiple can be specified, comma separated.");
            ledgerOption.setRequired(true);
            ledgerOption.setValueSeparator(',');
            ledgerOption.setArgs(Option.UNLIMITED_VALUES);

            opts.addOption(ledgerOption);
            opts.addOption("dryRun", false,
                           "Process the entryLogger, but don't write anything.");
            opts.addOption("password", true,
                           "The bookie stores the password in the index file, so we need it to regenerate. "
                           + "This must match the value in the ledger metadata.");
            opts.addOption("b64password", true,
                           "The password in base64 encoding, for cases where the password is not UTF-8.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Regenerate an interleaved storage index file, from available entrylogger files.";
        }

        @Override
        String getUsage() {
            return CMD_REGENERATE_INTERLEAVED_STORAGE_INDEX_FILE;
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            byte[] password;
            if (cmdLine.hasOption("password")) {
                password = cmdLine.getOptionValue("password").getBytes(UTF_8);
            } else if (cmdLine.hasOption("b64password")) {
                password = Base64.getDecoder().decode(cmdLine.getOptionValue("b64password"));
            } else {
                LOG.error("The password must be specified to regenerate the index file.");
                return 1;
            }
            Set<Long> ledgerIds = Arrays.stream(cmdLine.getOptionValues("ledgerIds"))
                .map((id) -> Long.parseLong(id)).collect(Collectors.toSet());
            boolean dryRun = cmdLine.hasOption("dryRun");

            LOG.info("=== Rebuilding index file for {} ===", ledgerIds);
            ServerConfiguration conf = new ServerConfiguration(bkConf);
            new InterleavedStorageRegenerateIndexOp(conf, ledgerIds, password).initiate(dryRun);
            LOG.info("-- Done rebuilding index file for {} --", ledgerIds);
            return 0;
        }
    }

    final Map<String, MyCommand> commands = new HashMap<String, MyCommand>();

    {
        commands.put(CMD_METAFORMAT, new MetaFormatCmd());
        commands.put(CMD_INITBOOKIE, new InitBookieCmd());
        commands.put(CMD_INITNEWCLUSTER, new InitNewCluster());
        commands.put(CMD_NUKEEXISTINGCLUSTER, new NukeExistingCluster());
        commands.put(CMD_BOOKIEFORMAT, new BookieFormatCmd());
        commands.put(CMD_RECOVER, new RecoverCmd());
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_READ_LEDGER_ENTRIES, new ReadLedgerEntriesCmd());
        commands.put(CMD_LISTLEDGERS, new ListLedgersCmd());
        commands.put(CMD_LISTUNDERREPLICATED, new ListUnderreplicatedCmd());
        commands.put(CMD_WHOISAUDITOR, new WhoIsAuditorCmd());
        commands.put(CMD_WHATISINSTANCEID, new WhatIsInstanceId());
        commands.put(CMD_LEDGERMETADATA, new LedgerMetadataCmd());
        commands.put(CMD_SIMPLETEST, new SimpleTestCmd());
        commands.put(CMD_BOOKIESANITYTEST, new BookieSanityTestCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READLOGMETADATA, new ReadLogMetadataCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_AUTORECOVERY, new AutoRecoveryCmd());
        commands.put(CMD_LISTBOOKIES, new ListBookiesCmd());
        commands.put(CMD_LISTFILESONDISC, new ListDiskFilesCmd());
        commands.put(CMD_UPDATECOOKIE, new UpdateCookieCmd());
        commands.put(CMD_UPDATELEDGER, new UpdateLedgerCmd());
        commands.put(CMD_DELETELEDGER, new DeleteLedgerCmd());
        commands.put(CMD_BOOKIEINFO, new BookieInfoCmd());
        commands.put(CMD_DECOMMISSIONBOOKIE, new DecommissionBookieCmd());
        commands.put(CMD_CONVERT_TO_DB_STORAGE, new ConvertToDbStorageCmd());
        commands.put(CMD_CONVERT_TO_INTERLEAVED_STORAGE, new ConvertToInterleavedStorageCmd());
        commands.put(CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX, new RebuildDbLedgerLocationsIndexCmd());
        commands.put(CMD_REGENERATE_INTERLEAVED_STORAGE_INDEX_FILE, new RegenerateInterleavedStorageIndexFile());
        commands.put(CMD_HELP, new HelpCmd());
        commands.put(CMD_LOSTBOOKIERECOVERYDELAY, new LostBookieRecoveryDelayCmd());
        commands.put(CMD_TRIGGERAUDIT, new TriggerAuditCmd());
    }

    @Override
    public void setConf(CompositeConfiguration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectories = Bookie.getCurrentDirectories(bkConf.getJournalDirs());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        if (null == bkConf.getIndexDirs()) {
            indexDirectories = ledgerDirectories;
        } else {
            indexDirectories = Bookie.getCurrentDirectories(bkConf.getIndexDirs());
        }
        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private void printShellUsage() {
        System.err.println("Usage: bookkeeper shell [-localbookie [<host:port>]] [-ledgeridformat <hex/long/uuid>] "
                + "[-entryformat <hex/string>] [-conf configuration] <command>");
        System.err.println("where command is one of:");
        List<String> commandNames = new ArrayList<String>();
        for (MyCommand c : commands.values()) {
            commandNames.add("       " + c.getUsage());
        }
        Collections.sort(commandNames);
        for (String s : commandNames) {
            System.err.println(s);
        }
    }

    @VisibleForTesting
    public int execute(String... args) throws Exception {
        return run(args);
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
    public static List<File> listFilesAndSort(File[] folderNames, String... extensions) {
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

        // handle some common options for multiple cmds
        Options opts = new Options();
        opts.addOption(CONF_OPT, true, "configuration file");
        opts.addOption(LEDGERID_FORMATTER_OPT, true, "format of ledgerId");
        opts.addOption(ENTRY_FORMATTER_OPT, true, "format of entries");
        BasicParser parser = new BasicParser();
        CommandLine cmdLine = parser.parse(opts, argv, true);

        // load configuration
        CompositeConfiguration conf = new CompositeConfiguration();
        if (cmdLine.hasOption(CONF_OPT)) {
            String val = cmdLine.getOptionValue(CONF_OPT);
            conf.addConfiguration(new PropertiesConfiguration(
                    new File(val).toURI().toURL()));
        }
        shell.setConf(conf);

        // ledgerid format
        if (cmdLine.hasOption(LEDGERID_FORMATTER_OPT)) {
            String val = cmdLine.getOptionValue(LEDGERID_FORMATTER_OPT);
            shell.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(val, shell.bkConf);
        } else {
            shell.ledgerIdFormatter = LedgerIdFormatter.newLedgerIdFormatter(shell.bkConf);
        }
        LOG.debug("Using ledgerIdFormatter {}", shell.ledgerIdFormatter.getClass());

        // entry format
        if (cmdLine.hasOption(ENTRY_FORMATTER_OPT)) {
            String val = cmdLine.getOptionValue(ENTRY_FORMATTER_OPT);
            shell.entryFormatter = EntryFormatter.newEntryFormatter(val, shell.bkConf);
        } else {
            shell.entryFormatter = EntryFormatter.newEntryFormatter(shell.bkConf);
        }
        LOG.debug("Using entry formatter {}", shell.entryFormatter.getClass());

        int res = shell.run(cmdLine.getArgs());
        System.exit(res);
    }

    ///
    /// Bookie File Operations
    ///

    /**
     * Get the ledger file of a specified ledger.
     *
     * @param ledgerId Ledger Id
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
     * @param ledgerId Ledger Id
     * @return read only file info instance
     */
    ReadOnlyFileInfo getFileInfo(long ledgerId) throws IOException {
        File ledgerFile = getLedgerFile(ledgerId);
        if (null == ledgerFile) {
            throw new FileNotFoundException("No index file found for ledger " + ledgerId
                    + ". It may be not flushed yet.");
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
     * Scan over entry log.
     *
     * @param logId Entry Log Id
     * @param scanner Entry Log Scanner
     */
    protected void scanEntryLog(long logId, EntryLogScanner scanner) throws IOException {
        initEntryLogger();
        entryLogger.scanEntryLog(logId, scanner);
    }

    private synchronized List<Journal> getJournals() throws IOException {
        if (null == journals) {
            journals = Lists.newArrayListWithCapacity(bkConf.getJournalDirs().length);
            int idx = 0;
            for (File journalDir : bkConf.getJournalDirs()) {
                journals.add(new Journal(idx++, new File(journalDir, BookKeeperConstants.CURRENT_DIR), bkConf,
                    new LedgerDirsManager(bkConf, bkConf.getLedgerDirs(),
                        new DiskChecker(bkConf.getDiskUsageThreshold(), bkConf.getDiskUsageWarnThreshold()))));
            }
        }
        return journals;
    }

    /**
     * Scan journal file.
     *
     * @param journalId Journal File Id
     * @param scanner Journal File Scanner
     */
    protected void scanJournal(Journal journal, long journalId, JournalScanner scanner) throws IOException {
        journal.scanJournal(journalId, 0L, scanner);
    }

    ///
    /// Bookie Shell Commands
    ///

    /**
     * Read ledger meta.
     *
     * @param ledgerId Ledger Id
     */
    protected void readLedgerMeta(long ledgerId) throws Exception {
        System.out.println("===== LEDGER: " + ledgerIdFormatter.formatLedgerId(ledgerId) + " =====");
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
     * Read ledger index entries.
     *
     * @param ledgerId Ledger Id
     * @throws IOException
     */
    protected void readLedgerIndexEntries(long ledgerId) throws IOException {
        System.out.println("===== LEDGER: " + ledgerIdFormatter.formatLedgerId(ledgerId) + " =====");
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
                for (int i = 0; i < entriesPerPage; i++) {
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
                System.out.println("Failed to read index page @ " + curSize + ", the index file may be corrupted : "
                        + ie.getMessage());
            } else {
                System.out.println("Failed to read last index page @ " + curSize + ", the index file may be corrupted "
                        + "or last index page is not fully flushed yet : " + ie.getMessage());
            }
        }
    }

    protected void printEntryLogMetadata(long logId) throws IOException {
        LOG.info("Print entryLogMetadata of entrylog {} ({}.log)", logId, Long.toHexString(logId));
        initEntryLogger();
        EntryLogMetadata entryLogMetadata = entryLogger.getEntryLogMetadata(logId);
        entryLogMetadata.getLedgersMap().forEach((ledgerId, size) -> {
            LOG.info("--------- Lid={}, TotalSizeOfEntriesOfLedger={}  ---------",
                    ledgerIdFormatter.formatLedgerId(ledgerId), size);
        });
    }

    /**
     * Get an iterable over pages of entries and locations for a ledger.
     *
     * @param ledgerId
     * @return
     * @throws IOException
     */
    protected Iterable<SortedMap<Long, Long>> getLedgerIndexEntries(final long ledgerId) throws IOException {
        final FileInfo fi = getFileInfo(ledgerId);
        final long size = fi.size();

        final LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
        lep.usePage();

        final Iterator<SortedMap<Long, Long>> iterator = new Iterator<SortedMap<Long, Long>>() {
            long curSize = 0;
            long curEntry = 0;

            @Override
            public boolean hasNext() {
                return curSize < size;
            }

            @Override
            public SortedMap<Long, Long> next() {
                SortedMap<Long, Long> entries = Maps.newTreeMap();
                lep.setLedgerAndFirstEntry(ledgerId, curEntry);
                try {
                    lep.readPage(fi);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // process a page
                for (int i = 0; i < entriesPerPage; i++) {
                    long offset = lep.getOffset(i * 8);
                    if (offset != 0) {
                        entries.put(curEntry, offset);
                    }
                    ++curEntry;
                }

                curSize += pageSize;
                return entries;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Cannot remove");
            }

        };

        return new Iterable<SortedMap<Long, Long>>() {
            @Override
            public Iterator<SortedMap<Long, Long>> iterator() {
                return iterator;
            }
        };
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
            public void process(long ledgerId, long startPos, ByteBuf entry) {
                formatEntry(startPos, entry, printMsg);
            }
        });
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
    protected void scanEntryLogForSpecificEntry(long logId, final long ledgerId, final long entryId,
                                                final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)" + " for LedgerId "
                + ledgerId + ((entryId == -1) ? "" : " for EntryId " + entryId));
        final MutableBoolean entryFound = new MutableBoolean(false);
        scanEntryLog(logId, new EntryLogScanner() {
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
                    formatEntry(startPos, entry, printMsg);
                }
            }
        });
        if (!entryFound.booleanValue()) {
            System.out.println("LedgerId " + ledgerId + ((entryId == -1) ? "" : " EntryId " + entryId)
                    + " is not available in the entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        }
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
    protected void scanEntryLogForPositionRange(long logId, final long rangeStartPos, final long rangeEndPos,
                                                final boolean printMsg) throws Exception {
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
            public void process(long ledgerId, long entryStartPos, ByteBuf entry) {
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
     * Scan a journal file.
     *
     * @param journalId Journal File Id
     * @param printMsg Whether printing the entry data.
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
                formatEntry(offset, Unpooled.wrappedBuffer(entry), printMsg);
            }
        });
    }

    /**
     * Print last log mark.
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
        System.out.println("--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId) + ", Eid=" + entryId
                + ", EntrySize=" + entrySize + " ---------");
        if (printMsg) {
            entryFormatter.formatEntry(entry.getEntry());
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
    private void formatEntry(long pos, ByteBuf recBuff, boolean printMsg) {
        int entrySize = recBuff.readableBytes();
        long ledgerId = recBuff.readLong();
        long entryId = recBuff.readLong();

        System.out.println("--------- Lid=" + ledgerIdFormatter.formatLedgerId(ledgerId) + ", Eid=" + entryId
                + ", ByteOffset=" + pos + ", EntrySize=" + entrySize + " ---------");
        if (entryId == Bookie.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.readInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.readBytes(masterKey);
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
        long lastAddConfirmed = recBuff.readLong();
        System.out.println("Type:           DATA");
        System.out.println("LastConfirmed:  " + lastAddConfirmed);
        if (!printMsg) {
            System.out.println();
            return;
        }
        // skip digest checking
        recBuff.skipBytes(8);
        System.out.println("Data:");
        System.out.println();
        try {
            byte[] ret = new byte[recBuff.readableBytes()];
            recBuff.readBytes(ret);
            entryFormatter.formatEntry(ret);
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

    private long getOptionLedgerIdValue(CommandLine cmdLine, String option, long defaultVal) {
        if (cmdLine.hasOption(option)) {
            String val = cmdLine.getOptionValue(option);
            try {
                return ledgerIdFormatter.readLedgerId(val);
            } catch (IllegalArgumentException iae) {
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
