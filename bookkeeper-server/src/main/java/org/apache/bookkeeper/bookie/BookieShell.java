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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Private;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.commands.autorecovery.ListUnderReplicatedCommand;
import org.apache.bookkeeper.tools.cli.commands.autorecovery.LostBookieRecoveryDelayCommand;
import org.apache.bookkeeper.tools.cli.commands.autorecovery.ToggleCommand;
import org.apache.bookkeeper.tools.cli.commands.autorecovery.TriggerAuditCommand;
import org.apache.bookkeeper.tools.cli.commands.autorecovery.WhoIsAuditorCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ConvertToDBStorageCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ConvertToInterleavedStorageCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.FlipBookieIdCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.FormatCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.InitCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.LastMarkCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.LedgerCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ListFilesOnDiscCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ListLedgersCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.LocalConsistencyCheckCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadJournalCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLedgerCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLogCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.ReadLogMetadataCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.RebuildDBLedgerLocationsIndexCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.RegenerateInterleavedStorageIndexFileCommand;
import org.apache.bookkeeper.tools.cli.commands.bookie.SanityTestCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.DecommissionCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.EndpointInfoCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.InfoCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.InstanceIdCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.ListBookiesCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.MetaFormatCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.NukeExistingClusterCommand;
import org.apache.bookkeeper.tools.cli.commands.bookies.NukeExistingClusterCommand.NukeExistingClusterFlags;
import org.apache.bookkeeper.tools.cli.commands.bookies.RecoverCommand;
import org.apache.bookkeeper.tools.cli.commands.client.DeleteLedgerCommand;
import org.apache.bookkeeper.tools.cli.commands.client.LedgerMetaDataCommand;
import org.apache.bookkeeper.tools.cli.commands.client.SimpleTestCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.AdminCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.CreateCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.DeleteCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.GenerateCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.GetCookieCommand;
import org.apache.bookkeeper.tools.cli.commands.cookie.UpdateCookieCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import org.apache.bookkeeper.util.Tool;
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
    static final String CMD_ENDPOINTINFO = "endpointinfo";
    static final String CMD_LOSTBOOKIERECOVERYDELAY = "lostbookierecoverydelay";
    static final String CMD_TRIGGERAUDIT = "triggeraudit";
    static final String CMD_CONVERT_TO_DB_STORAGE = "convert-to-db-storage";
    static final String CMD_CONVERT_TO_INTERLEAVED_STORAGE = "convert-to-interleaved-storage";
    static final String CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX = "rebuild-db-ledger-locations-index";
    static final String CMD_REGENERATE_INTERLEAVED_STORAGE_INDEX_FILE = "regenerate-interleaved-storage-index-file";

    // cookie commands
    static final String CMD_CREATE_COOKIE = "cookie_create";
    static final String CMD_DELETE_COOKIE = "cookie_delete";
    static final String CMD_UPDATE_COOKIE = "cookie_update";
    static final String CMD_GET_COOKIE = "cookie_get";
    static final String CMD_GENERATE_COOKIE = "cookie_generate";

    static final String CMD_HELP = "help";
    static final String CMD_LOCALCONSISTENCYCHECK = "localconsistencycheck";

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

    /**
     * BookieShell command.
     */
    @Private
    public interface Command {
        int runCmd(String[] args) throws Exception;

        String description();

        void printUsage();
    }

    void printInfoLine(String s) {
        System.out.println(s);
    }

    void printErrorLine(String s) {
        System.err.println(s);
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
        public String description() {
            // we used the string returned by `getUsage` as description in showing the list of commands
            return getUsage();
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

            MetaFormatCommand cmd = new MetaFormatCommand();
            MetaFormatCommand.MetaFormatFlags flags = new MetaFormatCommand.MetaFormatFlags()
                .interactive(interactive).force(force);
            boolean result = cmd.apply(bkConf, flags);
            return result ? 0 : 1;
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
            org.apache.bookkeeper.tools.cli.commands.bookies.InitCommand initCommand =
                new org.apache.bookkeeper.tools.cli.commands.bookies.InitCommand();
            boolean result = initCommand.apply(bkConf, new CliFlags());
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

            NukeExistingClusterCommand cmd = new NukeExistingClusterCommand();
            NukeExistingClusterFlags flags = new NukeExistingClusterFlags().force(force)
                                                                           .zkLedgersRootPath(zkledgersrootpath)
                                                                           .instandId(instanceid);
            boolean result = cmd.apply(bkConf, flags);
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
            boolean deletecookie = cmdLine.hasOption("d");

            FormatCommand.Flags flags = new FormatCommand.Flags()
                .nonInteractive(interactive)
                .force(force)
                .deleteCookie(deletecookie);
            FormatCommand command = new FormatCommand(flags);
            boolean result = command.apply(bkConf, flags);
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
            InitCommand initCommand = new InitCommand();
            boolean result = initCommand.apply(conf, new CliFlags());
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

            RecoverCommand cmd = new RecoverCommand();
            RecoverCommand.RecoverFlags flags = new RecoverCommand.RecoverFlags();
            flags.bookieAddress(args[0]);
            flags.deleteCookie(removeCookies);
            flags.dryRun(dryrun);
            flags.force(force);
            flags.ledger(ledgerId);
            flags.skipOpenLedgers(skipOpenLedgers);
            flags.query(query);
            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : -1;
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
            LedgerCommand cmd = new LedgerCommand(ledgerIdFormatter);
            cmd.setPrint(BookieShell.this::printInfoLine);
            LedgerCommand.LedgerFlags flags = new LedgerCommand.LedgerFlags();
            if (cmdLine.hasOption("m")) {
                flags.meta(true);
            }
            flags.ledgerId(Long.parseLong(cmdLine.getArgs()[0]));
            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : 1;
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
            final long firstEntry = getOptionLongValue(cmdLine, "firstentryid", 0);
            long lastEntry = getOptionLongValue(cmdLine, "lastentryid", -1);

            boolean printMsg = cmdLine.hasOption("m");
            boolean forceRecovery = cmdLine.hasOption("r");
            final BookieSocketAddress bookie;
            String bookieAddress;
            if (cmdLine.hasOption("b")) {
                // A particular bookie was specified
                bookieAddress = cmdLine.getOptionValue("b");
            } else {
                bookieAddress = null;
            }

            ReadLedgerCommand cmd = new ReadLedgerCommand(entryFormatter, ledgerIdFormatter);
            ReadLedgerCommand.ReadLedgerFlags flags = new ReadLedgerCommand.ReadLedgerFlags();
            flags.bookieAddresss(bookieAddress);
            flags.firstEntryId(firstEntry);
            flags.forceRecovery(forceRecovery);
            flags.lastEntryId(lastEntry);
            flags.ledgerId(ledgerId);
            flags.msg(printMsg);
            cmd.apply(bkConf, flags);
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

            ListUnderReplicatedCommand.LURFlags flags = new ListUnderReplicatedCommand.LURFlags()
                                                            .missingReplica(includingBookieId)
                                                            .excludingMissingReplica(excludingBookieId)
                                                            .printMissingReplica(printMissingReplica)
                                                            .printReplicationWorkerId(printReplicationWorkerId);
            ListUnderReplicatedCommand cmd = new ListUnderReplicatedCommand(ledgerIdFormatter);
            cmd.apply(bkConf, flags);
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

            ListLedgersCommand.ListLedgersFlags flags = new ListLedgersCommand.ListLedgersFlags()
                                                            .bookieId(bookieidToBePartOfEnsemble).meta(printMeta);
            ListLedgersCommand cmd = new ListLedgersCommand(ledgerIdFormatter);
            cmd.apply(bkConf, flags);

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
            System.out.println(md.toString());
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
            lOpts.addOption("dumptofile", true, "Dump metadata for ledger, to a file");
            lOpts.addOption("restorefromfile", true, "Restore metadata for ledger, from a file");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            final long ledgerId = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            if (ledgerId == -1) {
                System.err.println("Must specify a ledger id");
                return -1;
            }
            if (cmdLine.hasOption("dumptofile") && cmdLine.hasOption("restorefromefile")) {
                System.err.println("Only one of --dumptofile and --restorefromfile can be specified");
                return -2;
            }

            LedgerMetaDataCommand.LedgerMetadataFlag flag = new LedgerMetaDataCommand.LedgerMetadataFlag();
            flag.ledgerId(ledgerId);
            if (cmdLine.hasOption("dumptofile")) {
                flag.dumpToFile(cmdLine.getOptionValue("dumptofile"));
            }
            if (cmdLine.hasOption("restorefromfile")) {
                flag.restoreFromFile(cmdLine.getOptionValue("restorefromfile"));
            }

            LedgerMetaDataCommand cmd = new LedgerMetaDataCommand(ledgerIdFormatter);
            cmd.apply(bkConf, flag);
            return 0;
        }

        @Override
        String getDescription() {
            return "Print the metadata for a ledger, or optionally dump to a file.";
        }

        @Override
        String getUsage() {
            return "ledgermetadata -ledgerid <ledgerid> [--dump-to-file FILENAME|--restore-from-file FILENAME]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Check local storage for inconsistencies.
     */
    class LocalConsistencyCheck extends MyCommand {
        Options lOpts = new Options();

        LocalConsistencyCheck() {
            super(CMD_LOCALCONSISTENCYCHECK);
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            LocalConsistencyCheckCommand cmd = new LocalConsistencyCheckCommand();
            boolean result = cmd.apply(bkConf, new CliFlags());
            return (result) ? 0 : 1;
        }

        @Override
        String getDescription() {
            return "Validate Ledger Storage internal metadata";
        }

        @Override
        String getUsage() {
            return "localconsistencycheck";
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
            SanityTestCommand command = new SanityTestCommand();
            SanityTestCommand.SanityFlags flags = new SanityTestCommand.SanityFlags();
            boolean result = command.apply(bkConf, flags);
            return (result) ? 0 : -1;
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
            ReadLogCommand cmd = new ReadLogCommand(ledgerIdFormatter, entryFormatter);
            ReadLogCommand.ReadLogFlags flags = new ReadLogCommand.ReadLogFlags();

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
                flags.entryLogId(logId);
            } catch (NumberFormatException nfe) {
                // not a entry log id
                flags.filename(leftArgs[0]);
            }
            final long lId = getOptionLedgerIdValue(cmdLine, "ledgerid", -1);
            final long eId = getOptionLongValue(cmdLine, "entryid", -1);
            final long startpos = getOptionLongValue(cmdLine, "startpos", -1);
            final long endpos = getOptionLongValue(cmdLine, "endpos", -1);
            flags.endPos(endpos);
            flags.startPos(startpos);
            flags.entryId(eId);
            flags.ledgerId(lId);
            flags.msg(printMsg);
            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : -1;
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
            ReadLogMetadataCommand cmd = new ReadLogMetadataCommand(ledgerIdFormatter);
            ReadLogMetadataCommand.ReadLogMetadataFlags flags = new ReadLogMetadataCommand.ReadLogMetadataFlags();
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                LOG.error("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
            }

            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
                flags.logId(logId);
            } catch (NumberFormatException nfe) {
                flags.logFilename(leftArgs[0]);
            }
            cmd.apply(bkConf, flags);
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
            rjOpts.addOption("dir", true, "Journal directory (needed if more than one journal configured)");
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

            long journalId = -1L;
            String filename = "";
            try {
                journalId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                filename = leftArgs[0];
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }

            ReadJournalCommand.ReadJournalFlags flags = new ReadJournalCommand.ReadJournalFlags().msg(printMsg)
                                                            .fileName(filename).journalId(journalId)
                                                            .dir(cmdLine.getOptionValue("dir"));
            ReadJournalCommand cmd = new ReadJournalCommand(ledgerIdFormatter, entryFormatter);
            boolean result = cmd.apply(bkConf, flags);
            return result ? 0 : -1;
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
            opts.addOption("a", "all", false, "Print all bookies");
            // @deprecated 'rw'/'ro' option print both hostname and ip, so this option is not needed anymore
            opts.addOption("h", "hostnames", false, "Also print hostname of the bookie");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            int passedCommands = 0;

            boolean readwrite = cmdLine.hasOption("rw");
            if (readwrite) {
                passedCommands++;
            }
            boolean readonly = cmdLine.hasOption("ro");
            if (readonly) {
                passedCommands++;
            }
            boolean all = cmdLine.hasOption("a");
            if (all) {
                passedCommands++;
            }

            if (passedCommands != 1) {
                LOG.error("One and only one of -readwrite, -readonly and -all must be specified");
                printUsage();
                return 1;
            }

            ListBookiesCommand.Flags flags = new ListBookiesCommand.Flags()
                .readwrite(readwrite)
                .readonly(readonly)
                .all(all);

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
            return "listbookies  [-readwrite|-readonly|-all] [-hostnames]";
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

            ListFilesOnDiscCommand.LFODFlags flags = new ListFilesOnDiscCommand.LFODFlags().journal(journal)
                                                         .entrylog(entrylog).index(index);
            ListFilesOnDiscCommand cmd = new ListFilesOnDiscCommand(flags);
            cmd.apply(bkConf, flags);
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

            ToggleCommand.AutoRecoveryFlags flags = new ToggleCommand.AutoRecoveryFlags()
                .enable(enable).status(!disable && !enable);
            ToggleCommand cmd = new ToggleCommand();
            cmd.apply(bkConf, flags);

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
            int set = 0;
            if (setter) {
                set = Integer.parseInt(cmdLine.getOptionValue("set"));
            }

            LostBookieRecoveryDelayCommand.LBRDFlags flags = new LostBookieRecoveryDelayCommand.LBRDFlags()
                .get(getter).set(set);
            LostBookieRecoveryDelayCommand cmd = new LostBookieRecoveryDelayCommand();
            boolean result = cmd.apply(bkConf, flags);
            return result ? 0 : 1;
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
            CliFlags flags = new CliFlags();
            WhoIsAuditorCommand cmd = new WhoIsAuditorCommand();
            boolean result = cmd.apply(bkConf, flags);
            return result ? 0 : -1;
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
            InstanceIdCommand cmd = new InstanceIdCommand();
            cmd.apply(bkConf, new CliFlags());
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
            AdminCommand cmd = new AdminCommand();
            AdminCommand.AdminFlags flags = new AdminCommand.AdminFlags();
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
                flags.hostname(useHostName);
                flags.ip(!useHostName);
            }
            flags.expandstorage(thisCommandOption.getLongOpt().equals(EXPANDSTORAGE));
            flags.list(thisCommandOption.getLongOpt().equals(LIST));
            flags.delete(thisCommandOption.getLongOpt().equals(DELETE));
            if (thisCommandOption.getLongOpt().equals(DELETE)) {
                boolean force = false;
                String optionValue = thisCommandOption.getValue();
                if (!StringUtils.isEmpty(optionValue) && optionValue.equals(FORCE)) {
                    force = true;
                }
                flags.force(force);
            }
            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : -1;
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
            opts.addOption("r", "maxOutstandingReads", true, "Max outstanding reads (default: 5 * updatespersec)");
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
            return "updateledgers -bookieId <hostname|ip> [-updatespersec N] [-maxOutstandingReads N] [-limit N] "
                    + "[-verbose true/false] [-printprogress N]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            FlipBookieIdCommand cmd = new FlipBookieIdCommand();
            FlipBookieIdCommand.FlipBookieIdFlags flags = new FlipBookieIdCommand.FlipBookieIdFlags();

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
            final int rate = getOptionIntValue(cmdLine, "updatespersec", 5);
            final int maxOutstandingReads = getOptionIntValue(cmdLine, "maxOutstandingReads", (rate * 5));
            final int limit = getOptionIntValue(cmdLine, "limit", Integer.MIN_VALUE);
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
            flags.hostname(useHostName);
            flags.printProgress(printprogress);
            flags.limit(limit);
            flags.updatePerSec(rate);
            flags.maxOutstandingReads(maxOutstandingReads);
            flags.verbose(verbose);

            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : -1;
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

            boolean force = cmdLine.hasOption("f");
            DeleteLedgerCommand cmd = new DeleteLedgerCommand(ledgerIdFormatter);
            DeleteLedgerCommand.DeleteLedgerFlags flags = new DeleteLedgerCommand.DeleteLedgerFlags()
                .ledgerId(lid).force(force);
            cmd.apply(bkConf, flags);

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

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            InfoCommand cmd = new InfoCommand();
            cmd.apply(bkConf, new CliFlags());
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
            TriggerAuditCommand cmd = new TriggerAuditCommand();
            cmd.apply(bkConf, new CliFlags());
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
            DecommissionCommand cmd = new DecommissionCommand();
            DecommissionCommand.DecommissionFlags flags = new DecommissionCommand.DecommissionFlags();
            final String remoteBookieidToDecommission = cmdLine.getOptionValue("bookieid");
            flags.remoteBookieIdToDecommission(remoteBookieidToDecommission);
            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : -1;
        }
    }

    /**
     * Command to retrieve remote bookie endpoint information.
     */
    class EndpointInfoCmd extends MyCommand {
        Options lOpts = new Options();

        EndpointInfoCmd() {
            super(CMD_ENDPOINTINFO);
            lOpts.addOption("b", "bookieid", true, "Bookie Id");
        }

        @Override
        String getDescription() {
            return "Get info about a remote bookie with a specific bookie address (bookieid)";
        }

        @Override
        String getUsage() {
            return CMD_ENDPOINTINFO + " [-bookieid <bookieaddress>]";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            EndpointInfoCommand cmd = new EndpointInfoCommand();
            EndpointInfoCommand.EndpointInfoFlags flags = new EndpointInfoCommand.EndpointInfoFlags();
            final String bookieId = cmdLine.getOptionValue("bookieid");
            flags.bookie(bookieId);
            if (StringUtils.isBlank(bookieId)) {
                LOG.error("Invalid argument list!");
                this.printUsage();
                return -1;
            }

            boolean result = cmd.apply(bkConf, flags);
            return (result) ? 0 : -1;
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
            ConvertToDBStorageCommand cmd = new ConvertToDBStorageCommand();
            ConvertToDBStorageCommand.CTDBFlags flags = new ConvertToDBStorageCommand.CTDBFlags();
            cmd.setLedgerIdFormatter(ledgerIdFormatter);
            cmd.apply(bkConf, flags);
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
            ConvertToInterleavedStorageCommand cmd = new ConvertToInterleavedStorageCommand();
            ConvertToInterleavedStorageCommand.CTISFlags flags = new ConvertToInterleavedStorageCommand.CTISFlags();
            cmd.apply(bkConf, flags);
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
            RebuildDBLedgerLocationsIndexCommand cmd = new RebuildDBLedgerLocationsIndexCommand();
            cmd.apply(bkConf, new CliFlags());
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
            RegenerateInterleavedStorageIndexFileCommand cmd = new RegenerateInterleavedStorageIndexFileCommand();
            RegenerateInterleavedStorageIndexFileCommand.RISIFFlags
                flags = new RegenerateInterleavedStorageIndexFileCommand.RISIFFlags();
            List<Long> ledgerIds = Arrays.stream(cmdLine.getOptionValues("ledgerIds")).map((id) -> Long.parseLong(id))
                                         .collect(Collectors.toList());
            boolean dryRun = cmdLine.hasOption("dryRun");
            flags.ledgerIds(ledgerIds);
            if (cmdLine.hasOption("password")) {
                flags.password(cmdLine.getOptionValue("password"));
            } else if (cmdLine.hasOption("b64password")) {
                flags.b64Password(cmdLine.getOptionValue("b64password"));
            }
            flags.dryRun(dryRun);
            cmd.apply(bkConf, flags);
            return 0;
        }
    }

    final Map<String, Command> commands = new HashMap<>();

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
        commands.put(CMD_LOCALCONSISTENCYCHECK, new LocalConsistencyCheck());
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
        commands.put(CMD_ENDPOINTINFO, new EndpointInfoCmd());
        commands.put(CMD_CONVERT_TO_DB_STORAGE, new ConvertToDbStorageCmd());
        commands.put(CMD_CONVERT_TO_INTERLEAVED_STORAGE, new ConvertToInterleavedStorageCmd());
        commands.put(CMD_REBUILD_DB_LEDGER_LOCATIONS_INDEX, new RebuildDbLedgerLocationsIndexCmd());
        commands.put(CMD_REGENERATE_INTERLEAVED_STORAGE_INDEX_FILE, new RegenerateInterleavedStorageIndexFile());
        commands.put(CMD_HELP, new HelpCmd());
        commands.put(CMD_LOSTBOOKIERECOVERYDELAY, new LostBookieRecoveryDelayCmd());
        commands.put(CMD_TRIGGERAUDIT, new TriggerAuditCmd());
        // cookie related commands
        commands.put(CMD_CREATE_COOKIE,
            new CreateCookieCommand().asShellCommand(CMD_CREATE_COOKIE, bkConf));
        commands.put(CMD_DELETE_COOKIE,
            new DeleteCookieCommand().asShellCommand(CMD_DELETE_COOKIE, bkConf));
        commands.put(CMD_UPDATE_COOKIE,
            new UpdateCookieCommand().asShellCommand(CMD_UPDATE_COOKIE, bkConf));
        commands.put(CMD_GET_COOKIE,
            new GetCookieCommand().asShellCommand(CMD_GET_COOKIE, bkConf));
        commands.put(CMD_GENERATE_COOKIE,
            new GenerateCookieCommand().asShellCommand(CMD_GENERATE_COOKIE, bkConf));
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
        for (Command c : commands.values()) {
            commandNames.add("       " + c.description());
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

    public static void main(String[] argv) throws Exception {
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

    private synchronized void initEntryLogger() throws IOException {
        if (null == entryLogger) {
            // provide read only entry logger
            entryLogger = new ReadOnlyEntryLogger(bkConf);
        }
    }

    ///
    /// Bookie Shell Commands
    ///

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
        return StringUtils.equals(optValue, optName);
    }
}
