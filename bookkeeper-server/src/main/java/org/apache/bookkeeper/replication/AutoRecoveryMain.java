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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.server.service.AutoRecoveryService;
import org.apache.bookkeeper.server.service.HttpService;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to start/stop the AutoRecovery daemons Auditor and ReplicationWorker.
 *
 * <p>TODO: eliminate the direct usage of zookeeper here {@link https://github.com/apache/bookkeeper/issues/1332}
 */
public class AutoRecoveryMain {
    private static final Logger LOG = LoggerFactory
            .getLogger(AutoRecoveryMain.class);

    private final ServerConfiguration conf;
    final BookKeeper bkc;
    final AuditorElector auditorElector;
    final ReplicationWorker replicationWorker;
    final AutoRecoveryDeathWatcher deathWatcher;
    int exitCode;
    private volatile boolean shuttingDown = false;
    private volatile boolean running = false;

    // Exception handler
    private volatile UncaughtExceptionHandler uncaughtExceptionHandler = null;

    public AutoRecoveryMain(ServerConfiguration conf) throws IOException,
            InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public AutoRecoveryMain(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        this.conf = conf;
        this.bkc = Auditor.createBookKeeperClient(conf, statsLogger.scope(BookKeeperClientStats.CLIENT_SCOPE));
        MetadataClientDriver metadataClientDriver = bkc.getMetadataClientDriver();
        metadataClientDriver.setSessionStateListener(() -> {
            LOG.error("Client connection to the Metadata server has expired, so shutting down AutoRecoveryMain!");
            shutdown(ExitCode.ZK_EXPIRED);
        });

        auditorElector = new AuditorElector(
            Bookie.getBookieAddress(conf).toString(),
            conf,
            bkc,
            statsLogger.scope(AUDITOR_SCOPE),
            false);
        replicationWorker = new ReplicationWorker(
            conf,
            bkc,
            false,
            statsLogger.scope(REPLICATION_WORKER_SCOPE));
        deathWatcher = new AutoRecoveryDeathWatcher(this);
    }

    /*
     * Start daemons
     */
    public void start() {
        auditorElector.start();
        replicationWorker.start();
        if (null != uncaughtExceptionHandler) {
            deathWatcher.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        deathWatcher.start();
        running = true;
    }

    /*
     * Waits till all daemons joins
     */
    public void join() throws InterruptedException {
        deathWatcher.join();
    }

    /*
     * Shutdown all daemons gracefully
     */
    public void shutdown() {
        shutdown(ExitCode.OK);
    }

    private void shutdown(int exitCode) {
        LOG.info("Shutting down auto recovery: {}", exitCode);
        if (shuttingDown) {
            return;
        }
        LOG.info("Shutting down AutoRecovery");
        shuttingDown = true;
        running = false;
        this.exitCode = exitCode;

        try {
            auditorElector.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted shutting down auditor elector", e);
        }
        replicationWorker.shutdown();
        try {
            bkc.close();
        } catch (BKException e) {
            LOG.warn("Failed to close bookkeeper client for auto recovery", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted closing bookkeeper client for auto recovery", e);
        }
    }

    private int getExitCode() {
        return exitCode;
    }

    /**
     * Currently the uncaught exception handler is used for DeathWatcher to notify
     * lifecycle management that a bookie is dead for some reasons.
     *
     * <p>in future, we can register this <tt>exceptionHandler</tt> to critical threads
     * so when those threads are dead, it will automatically trigger lifecycle management
     * to shutdown the process.
     */
    public void setExceptionHandler(UncaughtExceptionHandler exceptionHandler) {
        this.uncaughtExceptionHandler = exceptionHandler;
    }

    @VisibleForTesting
    public Auditor getAuditor() {
        return auditorElector.getAuditor();
    }

    /** Is auto-recovery service running? */
    public boolean isAutoRecoveryRunning() {
        return running;
    }

    /*
     * DeathWatcher for AutoRecovery daemons.
     */
    private class AutoRecoveryDeathWatcher extends BookieCriticalThread {
        private int watchInterval;
        private AutoRecoveryMain autoRecoveryMain;

        public AutoRecoveryDeathWatcher(AutoRecoveryMain autoRecoveryMain) {
            super("AutoRecoveryDeathWatcher-"
                    + autoRecoveryMain.conf.getBookiePort());
            this.autoRecoveryMain = autoRecoveryMain;
            watchInterval = autoRecoveryMain.conf.getDeathWatchInterval();
            // set a default uncaught exception handler to shutdown the AutoRecovery
            // when it notices the AutoRecovery is not running any more.
            setUncaughtExceptionHandler((thread, cause) -> {
                LOG.info("AutoRecoveryDeathWatcher exited loop due to uncaught exception from thread {}",
                    thread.getName(), cause);
                shutdown();
            });
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                // If any one service not running, then shutdown peer.
                if (!autoRecoveryMain.auditorElector.isRunning() || !autoRecoveryMain.replicationWorker.isRunning()) {
                    LOG.info(
                            "AutoRecoveryDeathWatcher noticed the AutoRecovery is not running any more,"
                            + "exiting the watch loop!");
                    /*
                     * death watcher has noticed that AutoRecovery is not
                     * running any more throw an exception to fail the death
                     * watcher thread and it will trigger the uncaught exception
                     * handler to handle this "AutoRecovery not running"
                     * situation.
                     */
                    throw new RuntimeException("AutoRecovery is not running any more");
                }
            }
        }
    }

    private static final Options opts = new Options();
    static {
        opts.addOption("c", "conf", true, "Bookie server configuration");
        opts.addOption("h", "help", false, "Print help message");
    }

    /*
     * Print usage
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("AutoRecoveryMain [options]\n", opts);
    }

    /*
     * load configurations from file.
     */
    private static void loadConfFile(ServerConfiguration conf, String confFile)
            throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
        } catch (MalformedURLException e) {
            LOG.error("Could not open configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            LOG.error("Malformed configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        }
        LOG.info("Using configuration file " + confFile);
    }

    /*
     * Parse console args
     */
    private static ServerConfiguration parseArgs(String[] args)
            throws IllegalArgumentException {
        try {
            BasicParser parser = new BasicParser();
            CommandLine cmdLine = parser.parse(opts, args);

            if (cmdLine.hasOption('h')) {
                throw new IllegalArgumentException();
            }

            ServerConfiguration conf = new ServerConfiguration();
            String[] leftArgs = cmdLine.getArgs();

            if (cmdLine.hasOption('c')) {
                if (null != leftArgs && leftArgs.length > 0) {
                    throw new IllegalArgumentException();
                }
                String confFile = cmdLine.getOptionValue("c");
                loadConfFile(conf, confFile);
            }

            if (null != leftArgs && leftArgs.length > 0) {
                throw new IllegalArgumentException();
            }
            return conf;
        } catch (ParseException e) {
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
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            return ExitCode.INVALID_CONF;
        }

        // 1. building the component stack:
        LifecycleComponent server;
        try {
            server = buildAutoRecoveryServer(new BookieConfiguration(conf));
        } catch (Exception e) {
            LOG.error("Failed to build AutoRecovery Server", e);
            return ExitCode.SERVER_EXCEPTION;
        }

        // 2. start the server
        try {
            ComponentStarter.startComponent(server).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // the server is interrupted
            LOG.info("AutoRecovery server is interrupted. Exiting ...");
        } catch (ExecutionException ee) {
            LOG.error("Error in bookie shutdown", ee.getCause());
            return ExitCode.SERVER_EXCEPTION;
        }
        return ExitCode.OK;
    }

    public static LifecycleComponentStack buildAutoRecoveryServer(BookieConfiguration conf) throws Exception {
        LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack.newBuilder()
                .withName("autorecovery-server");

        // 1. build stats provider
        StatsProviderService statsProviderService = new StatsProviderService(conf);
        StatsLogger rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");

        serverBuilder.addComponent(statsProviderService);
        LOG.info("Load lifecycle component : {}", StatsProviderService.class.getName());

        // 2. build AutoRecovery server
        AutoRecoveryService autoRecoveryService = new AutoRecoveryService(conf, rootStatsLogger);

        serverBuilder.addComponent(autoRecoveryService);
        LOG.info("Load lifecycle component : {}", AutoRecoveryService.class.getName());

        // 4. build http service
        if (conf.getServerConf().isHttpServerEnabled()) {
            BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                    .setAutoRecovery(autoRecoveryService.getAutoRecoveryServer())
                    .setServerConfiguration(conf.getServerConf())
                    .setStatsProvider(statsProviderService.getStatsProvider()).build();
            HttpService httpService = new HttpService(provider, conf, rootStatsLogger);

            serverBuilder.addComponent(httpService);
            LOG.info("Load lifecycle component : {}", HttpService.class.getName());
        }

        return serverBuilder.build();
    }
}
