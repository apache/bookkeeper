/**
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.lang.Integer;
import java.util.Arrays;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SERVER_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_SCOPE;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
public class BookieServer {
    final ServerConfiguration conf;
    BookieNettyServer nettyServer;
    private volatile boolean running = false;
    Bookie bookie;
    DeathWatcher deathWatcher;
    private final static Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    int exitCode = ExitCode.OK;

    // operation stats
    AutoRecoveryMain autoRecoveryMain = null;
    private boolean isAutoRecoveryDaemonEnabled;

    // request processor
    private final RequestProcessor requestProcessor;

    // Expose Stats
    private final StatsLogger statsLogger;

    public BookieServer(ServerConfiguration conf) throws IOException,
            KeeperException, InterruptedException, BookieException,
            UnavailableException, CompatibilityException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public BookieServer(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, KeeperException, InterruptedException,
            BookieException, UnavailableException, CompatibilityException {
        this.conf = conf;
        this.statsLogger = statsLogger;
        this.nettyServer = new BookieNettyServer(this.conf, null);
        this.bookie = newBookie(conf);
        this.requestProcessor = new BookieRequestProcessor(conf, bookie,
                statsLogger.scope(SERVER_SCOPE));
        this.nettyServer.setRequestProcessor(this.requestProcessor);
        isAutoRecoveryDaemonEnabled = conf.isAutoRecoveryDaemonEnabled();
        if (isAutoRecoveryDaemonEnabled) {
            this.autoRecoveryMain = new AutoRecoveryMain(conf, statsLogger.scope(REPLICATION_SCOPE));
        }
    }

    protected Bookie newBookie(ServerConfiguration conf)
        throws IOException, KeeperException, InterruptedException, BookieException {
        return conf.isForceReadOnlyBookie() ?
                new ReadOnlyBookie(conf, statsLogger.scope(BOOKIE_SCOPE)) :
                new Bookie(conf, statsLogger.scope(BOOKIE_SCOPE));
    }

    public void start() throws IOException, UnavailableException {
        this.bookie.start();
        // fail fast, when bookie startup is not successful
        if (!this.bookie.isRunning()) {
            exitCode = bookie.getExitCode();
            return;
        }
        if (isAutoRecoveryDaemonEnabled && this.autoRecoveryMain != null) {
            this.autoRecoveryMain.start();
        }
        this.nettyServer.start();

        running = true;
        deathWatcher = new DeathWatcher(conf);
        deathWatcher.start();
    }

    @VisibleForTesting
    public BookieSocketAddress getLocalAddress() throws UnknownHostException {
        return Bookie.getBookieAddress(conf);
    }

    @VisibleForTesting
    public Bookie getBookie() {
        return bookie;
    }

    /**
     * Suspend processing of requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void suspendProcessing() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Suspending bookie server, port is {}", conf.getBookiePort());
        }
        nettyServer.suspendProcessing();
    }

    /**
     * Resume processing requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void resumeProcessing() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resuming bookie server, port is {}", conf.getBookiePort());
        }
        nettyServer.resumeProcessing();
    }

    public synchronized void shutdown() {
        LOG.info("Shutting down BookieServer");
        this.nettyServer.shutdown();
        if (!running) {
            return;
        }
        exitCode = bookie.shutdown();
        if (isAutoRecoveryDaemonEnabled && this.autoRecoveryMain != null) {
            this.autoRecoveryMain.shutdown();
        }
        this.requestProcessor.close();
        running = false;
    }

    public boolean isRunning() {
        return bookie.isRunning() && nettyServer.isRunning() && running;
    }

    /**
     * Whether bookie is running?
     *
     * @return true if bookie is running, otherwise return false
     */
    public boolean isBookieRunning() {
        return bookie.isRunning();
    }

    /**
     * Whether auto-recovery service running with Bookie?
     *
     * @return true if auto-recovery service is running, otherwise return false
     */
    public boolean isAutoRecoveryRunning() {
        return this.autoRecoveryMain != null
                && this.autoRecoveryMain.isAutoRecoveryRunning();
    }

    public void join() throws InterruptedException {
        bookie.join();
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * A thread to watch whether bookie & nioserver is still alive
     */
    private class DeathWatcher extends BookieCriticalThread {

        private final int watchInterval;

        DeathWatcher(ServerConfiguration conf) {
            super("BookieDeathWatcher-" + conf.getBookiePort());
            watchInterval = conf.getDeathWatchInterval();
        }

        @Override
        public void run() {
            while(true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    // do nothing
                }
                if (!isBookieRunning()) {
                    shutdown();
                    break;
                }
                if (isAutoRecoveryDaemonEnabled && !isAutoRecoveryRunning()) {
                    LOG.error("Autorecovery daemon has stopped. Please check the logs");
                    isAutoRecoveryDaemonEnabled = false; // to avoid spamming the logs
                }
            }
            LOG.info("BookieDeathWatcher exited loop!");
        }
    }

    static final Options bkOpts = new Options();
    static {
        bkOpts.addOption("c", "conf", true, "Configuration for Bookie Server");
        bkOpts.addOption("withAutoRecovery", false,
                "Start Autorecovery service Bookie server");
        bkOpts.addOption("readOnly", false,
                "Force Start a ReadOnly Bookie server");
        bkOpts.addOption("z", "zkserver", true, "Zookeeper Server");
        bkOpts.addOption("m", "zkledgerpath", true, "Zookeeper ledgers root path");
        bkOpts.addOption("p", "bookieport", true, "bookie port exported");
        bkOpts.addOption("j", "journal", true, "bookie journal directory");
        Option indexDirs = new Option ("i", "indexdirs", true, "bookie index directories");
        indexDirs.setArgs(10);
        bkOpts.addOption(indexDirs);
        Option ledgerDirs = new Option ("l", "ledgerdirs", true, "bookie ledgers directories");
        ledgerDirs.setArgs(10);
        bkOpts.addOption(ledgerDirs);
        bkOpts.addOption("h", "help", false, "Print help message");
    }

    /**
     * Print usage
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        String header = "\n"
                        + "BookieServer provide an interface to start a bookie with configuration file and/or arguments."
                        + "The settings in configuration file will be overwrite by provided arguments.\n"
                        + "Options including:\n";
        String footer = "Here is an example:\n" +
                        "\tBookieServer -c bookie.conf -z localhost:2181 -m /bookkeeper/ledgers " +
                        "-p 3181 -j /mnt/journal -i \"/mnt/index1 /mnt/index2\" -l \"/mnt/ledger1 /mnt/ledger2 /mnt/ledger3\"\n";
        hf.printHelp("BookieServer [options]\n", header,  bkOpts, footer, true);
    }

    private static void loadConfFile(ServerConfiguration conf, String confFile)
        throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
            conf.validate();
        } catch (MalformedURLException e) {
            LOG.error("Could not open configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            LOG.error("Malformed configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        }
        LOG.info("Using configuration file " + confFile);
    }

    private static ServerConfiguration parseArgs(String[] args)
        throws IllegalArgumentException {
        try {
            BasicParser parser = new BasicParser();
            CommandLine cmdLine = parser.parse(bkOpts, args);

            if (cmdLine.hasOption('h')) {
                throw new IllegalArgumentException();
            }

            ServerConfiguration conf = new ServerConfiguration();

            if (cmdLine.hasOption('c')) {
                String confFile = cmdLine.getOptionValue("c");
                loadConfFile(conf, confFile);
            }

            if (cmdLine.hasOption("withAutoRecovery")) {
                conf.setAutoRecoveryDaemonEnabled(true);
            }

            if (cmdLine.hasOption("readOnly")) {
                conf.setForceReadOnlyBookie(true);
            }


            // command line arguments overwrite settings in configuration file
            if (cmdLine.hasOption('z')) {
                String sZK = cmdLine.getOptionValue('z');
                LOG.info("Get cmdline zookeeper instance: " + sZK);
                conf.setZkServers(sZK);
            }

            if (cmdLine.hasOption('m')) {
                String sZkLedgersRootPath = cmdLine.getOptionValue('m');
                LOG.info("Get cmdline zookeeper ledger path: " + sZkLedgersRootPath);
                conf.setZkLedgersRootPath(sZkLedgersRootPath);
            }

            if (cmdLine.hasOption('p')) {
                String sPort = cmdLine.getOptionValue('p');
                LOG.info("Get cmdline bookie port: " + sPort);
                Integer iPort = Integer.parseInt(sPort);
                conf.setBookiePort(iPort.intValue());
            }

            if (cmdLine.hasOption('j')) {
                String sJournalDir = cmdLine.getOptionValue('j');
                LOG.info("Get cmdline journal dir: " + sJournalDir);
                conf.setJournalDirName(sJournalDir);
            }

            if (cmdLine.hasOption('i')) {
                String[] sIndexDirs = cmdLine.getOptionValues('i');
                LOG.info("Get cmdline index dirs: ");
                for(String index : sIndexDirs) {
                    LOG.info("indexDir : " + index);
                }
                conf.setIndexDirName(sIndexDirs);
            }

            if (cmdLine.hasOption('l')) {
                String[] sLedgerDirs = cmdLine.getOptionValues('l');
                LOG.info("Get cmdline ledger dirs: ");
                for(String ledger : sLedgerDirs) {
                    LOG.info("ledgerdir : " + ledger);
                }
                conf.setLedgerDirNames(sLedgerDirs);
            }

            return conf;
        } catch (ParseException e) {
            LOG.error("Error parsing command line arguments : ", e);
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) {
        ServerConfiguration conf = null;
        try {
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            LOG.error("Error parsing command line arguments : ", iae);
            System.err.println(iae.getMessage());
            printUsage();
            System.exit(ExitCode.INVALID_CONF);
        }

        StringBuilder sb = new StringBuilder();
        String[] ledgerDirNames = conf.getLedgerDirNames();
        for (int i = 0; i < ledgerDirNames.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(ledgerDirNames[i]);
        }

        String hello = String.format(
                           "Hello, I'm your bookie, listening on port %1$s. ZKServers are on %2$s. Journals are in %3$s. Ledgers are stored in %4$s.",
                           conf.getBookiePort(), conf.getZkServers(),
                           Arrays.asList(conf.getJournalDirNames()), sb);
        LOG.info(hello);
        try {
            // Initialize Stats Provider
            Class<? extends StatsProvider> statsProviderClass =
                    conf.getStatsProviderClass();
            final StatsProvider statsProvider = ReflectionUtils.newInstance(statsProviderClass);
            statsProvider.start(conf);

            final BookieServer bs = new BookieServer(conf, statsProvider.getStatsLogger(""));
            bs.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    bs.shutdown();
                    LOG.info("Shut down bookie server successfully");
                }
            });
            LOG.info("Register shutdown hook successfully");
            bs.join();

            statsProvider.stop();
            LOG.info("Stop stats provider");
            bs.shutdown();
            System.exit(bs.getExitCode());
        } catch (Exception e) {
            LOG.error("Exception running bookie server : ", e);
            System.exit(ExitCode.SERVER_EXCEPTION);
        }
    }
}
