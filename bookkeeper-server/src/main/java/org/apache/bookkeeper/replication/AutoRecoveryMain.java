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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServerLoader;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;

/**
 * Class to start/stop the AutoRecovery daemons Auditor and ReplicationWorker
 */
public class AutoRecoveryMain {
    private static final Logger LOG = LoggerFactory
            .getLogger(AutoRecoveryMain.class);

    private ServerConfiguration conf;
    ZooKeeper zk;
    AuditorElector auditorElector;
    ReplicationWorker replicationWorker;
    private AutoRecoveryDeathWatcher deathWatcher;
    private int exitCode;
    private volatile boolean shuttingDown = false;
    private volatile boolean running = false;

    public AutoRecoveryMain(ServerConfiguration conf) throws IOException,
            InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public AutoRecoveryMain(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, InterruptedException, KeeperException, UnavailableException,
            CompatibilityException {
        this.conf = conf;
        Set<Watcher> watchers = new HashSet<Watcher>();
        // TODO: better session handling for auto recovery daemon  https://issues.apache.org/jira/browse/BOOKKEEPER-594
        //       since {@link org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager}
        //       use Watcher, need to ensure the logic works correctly after recreating
        //       a new zookeeper client when session expired.
        //       for now just shutdown it.
        watchers.add(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Check for expired connection.
                if (event.getState().equals(Watcher.Event.KeeperState.Expired)) {
                    LOG.error("ZK client connection to the ZK server has expired!");
                    shutdown(ExitCode.ZK_EXPIRED);
                }
            }
        });
        zk = ZooKeeperClient.newBuilder()
                .connectString(conf.getZkServers())
                .sessionTimeoutMs(conf.getZkTimeout())
                .watchers(watchers)
                .build();
        auditorElector = new AuditorElector(Bookie.getBookieAddress(conf).toString(), conf,
                zk, statsLogger.scope(AUDITOR_SCOPE));
        replicationWorker = new ReplicationWorker(zk, conf, statsLogger.scope(REPLICATION_WORKER_SCOPE));
        deathWatcher = new AutoRecoveryDeathWatcher(this);
    }

    public AutoRecoveryMain(ServerConfiguration conf, ZooKeeper zk) throws IOException, InterruptedException, KeeperException,
            UnavailableException, CompatibilityException {
        this.conf = conf;
        this.zk = zk;
        auditorElector = new AuditorElector(Bookie.getBookieAddress(conf).toString(), conf, zk);
        replicationWorker = new ReplicationWorker(zk, conf);
        deathWatcher = new AutoRecoveryDeathWatcher(this);
    }

    /*
     * Start daemons
     */
    public void start() throws UnavailableException {
        auditorElector.start();
        replicationWorker.start();
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
            deathWatcher.interrupt();
            deathWatcher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted shutting down auto recovery", e);
        }

        try {
            auditorElector.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted shutting down auditor elector", e);
        }
        replicationWorker.shutdown();
        try {
            zk.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted shutting down auto recovery", e);
        }
    }

    private int getExitCode() {
        return exitCode;
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
    private static class AutoRecoveryDeathWatcher extends BookieCriticalThread {
        private int watchInterval;
        private AutoRecoveryMain autoRecoveryMain;

        public AutoRecoveryDeathWatcher(AutoRecoveryMain autoRecoveryMain) {
            super("AutoRecoveryDeathWatcher-"
                    + autoRecoveryMain.conf.getBookiePort());
            this.autoRecoveryMain = autoRecoveryMain;
            watchInterval = autoRecoveryMain.conf.getDeathWatchInterval();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    break;
                }
                // If any one service not running, then shutdown peer.
                if (!autoRecoveryMain.auditorElector.isRunning()
                    || !autoRecoveryMain.replicationWorker.isRunning()) {
                    autoRecoveryMain.shutdown(ExitCode.SERVER_EXCEPTION);
                    break;
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
        ServerConfiguration conf = null;
        try {
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            LOG.error("Error parsing command line arguments : ", iae);
            System.err.println(iae.getMessage());
            printUsage();
            System.exit(ExitCode.INVALID_CONF);
        }

        try {
            final AutoRecoveryMain autoRecoveryMain = new AutoRecoveryMain(conf);
            autoRecoveryMain.start();
            HttpServerLoader.loadHttpServer(conf);
            final HttpServer httpServer = HttpServerLoader.get();
            if (conf.isHttpServerEnabled() && httpServer != null) {
                BKHttpServiceProvider serviceProvider = new BKHttpServiceProvider.Builder()
                    .setAutoRecovery(autoRecoveryMain)
                    .setServerConfiguration(conf)
                    .build();
                httpServer.initialize(serviceProvider);
                httpServer.startServer(conf.getHttpServerPort());
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    autoRecoveryMain.shutdown();
                    if (httpServer != null && httpServer.isRunning()) {
                        httpServer.stopServer();
                    }
                    LOG.info("Shutdown AutoRecoveryMain successfully");
                }
            });
            LOG.info("Register shutdown hook successfully");
            autoRecoveryMain.join();
            System.exit(autoRecoveryMain.getExitCode());
        } catch (Exception e) {
            LOG.error("Exception running AutoRecoveryMain : ", e);
            System.exit(ExitCode.SERVER_EXCEPTION);
        }
    }
}
