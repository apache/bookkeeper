/*
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

package org.apache.bookkeeper.server;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.UncheckedConfigurationException;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;

/**
 * A bookie server is a server that run bookie and serving rpc requests.
 *
 * <p>It is a rewritten server using {@link org.apache.bookkeeper.common.component.LifecycleComponent},
 * replacing the legacy server {@link org.apache.bookkeeper.proto.BookieServer}.
 */
@Slf4j
public class Main {
    static final Options BK_OPTS = new Options();
    static {
        BK_OPTS.addOption("c", "conf", true, "Configuration for Bookie Server");
        BK_OPTS.addOption("withAutoRecovery", false,
                "Start Autorecovery service Bookie server");
        BK_OPTS.addOption("r", "readOnly", false,
                "Force Start a ReadOnly Bookie server");
        BK_OPTS.addOption("z", "zkserver", true, "Zookeeper Server");
        BK_OPTS.addOption("m", "zkledgerpath", true, "Zookeeper ledgers root path");
        BK_OPTS.addOption("p", "bookieport", true, "bookie port exported");
        BK_OPTS.addOption("hp", "httpport", true, "bookie http port exported");
        BK_OPTS.addOption("j", "journal", true, "bookie journal directory");
        Option indexDirs = new Option ("i", "indexdirs", true, "bookie index directories");
        indexDirs.setArgs(10);
        BK_OPTS.addOption(indexDirs);
        Option ledgerDirs = new Option ("l", "ledgerdirs", true, "bookie ledgers directories");
        ledgerDirs.setArgs(10);
        BK_OPTS.addOption(ledgerDirs);
        BK_OPTS.addOption("h", "help", false, "Print help message");
    }

    /**
     * Print usage.
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        String header = "\n"
            + "BookieServer provide an interface to start a bookie with configuration file and/or arguments."
            + "The settings in configuration file will be overwrite by provided arguments.\n"
            + "Options including:\n";
        String footer = "Here is an example:\n"
            + "\tBookieServer -c bookie.conf -z localhost:2181 -m /bookkeeper/ledgers "
            + "-p 3181 -j /mnt/journal -i \"/mnt/index1 /mnt/index2\""
            + " -l \"/mnt/ledger1 /mnt/ledger2 /mnt/ledger3\"\n";
        hf.printHelp("BookieServer [options]\n", header, BK_OPTS, footer, true);
    }

    private static void loadConfFile(ServerConfiguration conf, String confFile)
        throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
            conf.validate();
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file: {}", confFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file: {}", confFile, e);
            throw new IllegalArgumentException();
        }
        log.info("Using configuration file {}", confFile);
    }

    @SuppressWarnings("deprecation")
    private static ServerConfiguration parseArgs(String[] args)
        throws IllegalArgumentException {
        try {
            BasicParser parser = new BasicParser();
            CommandLine cmdLine = parser.parse(BK_OPTS, args);

            ServerConfiguration conf = new ServerConfiguration();

            if (cmdLine.hasOption('h')) {
                conf.setProperty("help", true);
                return conf;
            }

            if (cmdLine.hasOption('c')) {
                String confFile = cmdLine.getOptionValue("c");
                loadConfFile(conf, confFile);
            }

            if (cmdLine.hasOption("withAutoRecovery")) {
                conf.setAutoRecoveryDaemonEnabled(true);
            }

            if (cmdLine.hasOption("r")) {
                conf.setForceReadOnlyBookie(true);
            }

            boolean overwriteMetadataServiceUri = false;
            String sZkLedgersRootPath = "/ledgers";
            if (cmdLine.hasOption('m')) {
                sZkLedgersRootPath = cmdLine.getOptionValue('m');
                log.info("Get cmdline zookeeper ledger path: {}", sZkLedgersRootPath);
                overwriteMetadataServiceUri = true;
            }


            String sZK = conf.getZkServers();
            if (cmdLine.hasOption('z')) {
                sZK = cmdLine.getOptionValue('z');
                log.info("Get cmdline zookeeper instance: {}", sZK);
                overwriteMetadataServiceUri = true;
            }

            // command line arguments overwrite settings in configuration file
            if (overwriteMetadataServiceUri) {
                String metadataServiceUri = "zk://" + sZK + sZkLedgersRootPath;
                conf.setMetadataServiceUri(metadataServiceUri);
                log.info("Overwritten service uri to {}", metadataServiceUri);
            }

            if (cmdLine.hasOption('p')) {
                String sPort = cmdLine.getOptionValue('p');
                log.info("Get cmdline bookie port: {}", sPort);
                conf.setBookiePort(Integer.parseInt(sPort));
            }

            if (cmdLine.hasOption("httpport")) {
                String sPort = cmdLine.getOptionValue("httpport");
                log.info("Get cmdline http port: {}", sPort);
                Integer iPort = Integer.parseInt(sPort);
                conf.setHttpServerPort(iPort.intValue());
            }

            if (cmdLine.hasOption('j')) {
                String sJournalDir = cmdLine.getOptionValue('j');
                log.info("Get cmdline journal dir: {}", sJournalDir);
                conf.setJournalDirName(sJournalDir);
            }

            if (cmdLine.hasOption('i')) {
                String[] sIndexDirs = cmdLine.getOptionValues('i');
                log.info("Get cmdline index dirs: ");
                for (String index : sIndexDirs) {
                    log.info("indexDir : {}", index);
                }
                conf.setIndexDirName(sIndexDirs);
            }

            if (cmdLine.hasOption('l')) {
                String[] sLedgerDirs = cmdLine.getOptionValues('l');
                log.info("Get cmdline ledger dirs: ");
                for (String ledger : sLedgerDirs) {
                    log.info("ledgerdir : {}", ledger);
                }
                conf.setLedgerDirNames(sLedgerDirs);
            }

            return conf;
        } catch (ParseException e) {
            log.error("Error parsing command line arguments : ", e);
            throw new IllegalArgumentException(e);
        }
    }

    public static void main(String[] args) {
        int retCode = doMain(args);
        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) {
        ServerConfiguration conf;

        // 0. parse command line
        try {
            conf = parseCommandLine(args);
        } catch (IllegalArgumentException iae) {
            return ExitCode.INVALID_CONF;
        }

        if (conf.getBoolean("help", false)) {
            printUsage();
            return ExitCode.OK;
        }

        // 1. building the component stack:
        LifecycleComponent server;
        try {
            server = buildBookieServer(new BookieConfiguration(conf));
        } catch (Exception e) {
            log.error("Failed to build bookie server", e);
            System.err.println(e.getMessage());
            return ExitCode.SERVER_EXCEPTION;
        }

        // 2. start the server
        try {
            ComponentStarter.startComponent(server).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // the server is interrupted
            log.info("Bookie server is interrupted. Exiting ...");
            System.err.println(ie.getMessage());
        } catch (ExecutionException ee) {
            log.error("Error in bookie shutdown", ee.getCause());
            System.err.println(ee.getMessage());
            return ExitCode.SERVER_EXCEPTION;
        }
        return ExitCode.OK;
    }

    private static ServerConfiguration parseCommandLine(String[] args)
            throws IllegalArgumentException, UncheckedConfigurationException {
        ServerConfiguration conf;
        try {
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            log.error("Error parsing command line arguments : ", iae);
            System.err.println(iae.getMessage());
            printUsage();
            throw iae;
        }

        if (conf.getBoolean("help", false)) {
            return conf;
        }

        String hello = String.format(
            "Hello, I'm your bookie, bookieId is %1$s, listening on port %2$s. Metadata service uri is %3$s."
                + " Journals are in %4$s. Ledgers are stored in %5$s. Indexes are stored in %6$s.",
            conf.getBookieId() != null ? conf.getBookieId() : "<not-set>",
            conf.getBookiePort(),
            conf.getMetadataServiceUriUnchecked(),
            Arrays.asList(conf.getJournalDirNames()),
            Arrays.asList(conf.getLedgerDirNames()),
            Arrays.asList(conf.getIndexDirNames() != null ? conf.getIndexDirNames() : conf.getLedgerDirNames()));
        log.info(hello);

        return conf;
    }

    /**
     * Build the bookie server.
     *
     * <p>The sequence of the components is:
     *
     * <pre>
     * - stats provider
     * - bookie server
     * - autorecovery daemon
     * - http service
     * </pre>
     *
     * @param conf bookie server configuration
     * @return lifecycle stack
     */
    public static LifecycleComponentStack buildBookieServer(BookieConfiguration conf) throws Exception {
        return EmbeddedServer.builder(conf).build().getLifecycleComponentStack();
    }

    public static List<File> storageDirectoriesFromConf(ServerConfiguration conf) throws IOException {
        List<File> dirs = new ArrayList<>();

        File[] journalDirs = conf.getJournalDirs();
        if (journalDirs != null) {
            for (File j : journalDirs) {
                File cur = BookieImpl.getCurrentDirectory(j);
                if (!dirs.stream().anyMatch(f -> f.equals(cur))) {
                    BookieImpl.checkDirectoryStructure(cur);
                    dirs.add(cur);
                }
            }
        }

        File[] ledgerDirs = conf.getLedgerDirs();
        if (ledgerDirs != null) {
            for (File l : ledgerDirs) {
                File cur = BookieImpl.getCurrentDirectory(l);
                if (!dirs.stream().anyMatch(f -> f.equals(cur))) {
                    BookieImpl.checkDirectoryStructure(cur);
                    dirs.add(cur);
                }
            }
        }
        File[] indexDirs = conf.getIndexDirs();
        if (indexDirs != null) {
            for (File i : indexDirs) {
                File cur = BookieImpl.getCurrentDirectory(i);
                if (!dirs.stream().anyMatch(f -> f.equals(cur))) {
                    BookieImpl.checkDirectoryStructure(cur);
                    dirs.add(cur);
                }
            }
        }
        return dirs;
    }

}
