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

import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_SCOPE;
import static org.apache.bookkeeper.server.component.ServerLifecycleComponent.loadServerComponents;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.bookie.ScrubberStats;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.UncheckedConfigurationException;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.BookieServiceInfo.Endpoint;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.server.service.AutoRecoveryService;
import org.apache.bookkeeper.server.service.BookieService;
import org.apache.bookkeeper.server.service.HttpService;
import org.apache.bookkeeper.server.service.ScrubberService;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

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
                Integer iPort = Integer.parseInt(sPort);
                conf.setBookiePort(iPort.intValue());
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

        // 1. building the component stack:
        LifecycleComponent server;
        try {
            server = buildBookieServer(new BookieConfiguration(conf));
        } catch (Exception e) {
            log.error("Failed to build bookie server", e);
            return ExitCode.SERVER_EXCEPTION;
        }

        // 2. start the server
        try {
            ComponentStarter.startComponent(server).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // the server is interrupted
            log.info("Bookie server is interrupted. Exiting ...");
        } catch (ExecutionException ee) {
            log.error("Error in bookie shutdown", ee.getCause());
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

        StringBuilder sb = new StringBuilder();
        String[] ledgerDirNames = conf.getLedgerDirNames();
        for (int i = 0; i < ledgerDirNames.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(ledgerDirNames[i]);
        }

        String hello = String.format(
            "Hello, I'm your bookie, listening on port %1$s. Metadata service uri is %2$s."
                + " Journals are in %3$s. Ledgers are stored in %4$s.",
            conf.getBookiePort(),
            conf.getMetadataServiceUriUnchecked(),
            Arrays.asList(conf.getJournalDirNames()),
            sb);
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

        final ComponentInfoPublisher componentInfoPublisher = new ComponentInfoPublisher();

        final Supplier<BookieServiceInfo> bookieServiceInfoProvider =
                () -> buildBookieServiceInfo(componentInfoPublisher);
        LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack
                .newBuilder()
                .withComponentInfoPublisher(componentInfoPublisher)
                .withName("bookie-server");

        // 1. build stats provider
        StatsProviderService statsProviderService =
            new StatsProviderService(conf);
        StatsLogger rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");
        serverBuilder.addComponent(statsProviderService);
        log.info("Load lifecycle component : {}", StatsProviderService.class.getName());

        // 2. build bookie server
        BookieService bookieService =
            new BookieService(conf, rootStatsLogger, bookieServiceInfoProvider);

        serverBuilder.addComponent(bookieService);
        log.info("Load lifecycle component : {}", BookieService.class.getName());

        if (conf.getServerConf().isLocalScrubEnabled()) {
            serverBuilder.addComponent(
                    new ScrubberService(
                            rootStatsLogger.scope(ScrubberStats.SCOPE),
                    conf, bookieService.getServer().getBookie().getLedgerStorage()));
        }

        // 3. build auto recovery
        if (conf.getServerConf().isAutoRecoveryDaemonEnabled()) {
            AutoRecoveryService autoRecoveryService =
                new AutoRecoveryService(conf, rootStatsLogger.scope(REPLICATION_SCOPE));

            serverBuilder.addComponent(autoRecoveryService);
            log.info("Load lifecycle component : {}", AutoRecoveryService.class.getName());
        }

        // 4. build http service
        if (conf.getServerConf().isHttpServerEnabled()) {
            BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                .setBookieServer(bookieService.getServer())
                .setServerConfiguration(conf.getServerConf())
                .setStatsProvider(statsProviderService.getStatsProvider())
                .build();
            HttpService httpService =
                new HttpService(provider, conf, rootStatsLogger);
            serverBuilder.addComponent(httpService);
            log.info("Load lifecycle component : {}", HttpService.class.getName());
        }

        // 5. build extra services
        String[] extraComponents = conf.getServerConf().getExtraServerComponents();
        if (null != extraComponents) {
            try {
                List<ServerLifecycleComponent> components = loadServerComponents(
                    extraComponents,
                    conf,
                    rootStatsLogger);
                for (ServerLifecycleComponent component : components) {
                    serverBuilder.addComponent(component);
                    log.info("Load lifecycle component : {}", component.getClass().getName());
                }
            } catch (Exception e) {
                if (conf.getServerConf().getIgnoreExtraServerComponentsStartupFailures()) {
                    log.info("Failed to load extra components '{}' - {}. Continuing without those components.",
                        StringUtils.join(extraComponents), e.getMessage());
                } else {
                    throw e;
                }
            }
        }

        return serverBuilder.build();
    }

    /**
     * Create the {@link BookieServiceInfo} starting from the published endpoints.
     *
     * @see ComponentInfoPublisher
     * @param componentInfoPublisher the endpoint publisher
     * @return the created bookie service info
     */
    private static BookieServiceInfo buildBookieServiceInfo(ComponentInfoPublisher componentInfoPublisher) {
        List<Endpoint> endpoints = componentInfoPublisher.getEndpoints().values()
                .stream().map(e -> {
                    return new Endpoint(
                            e.getId(),
                            e.getPort(),
                            e.getHost(),
                            e.getProtocol(),
                            e.getAuth(),
                            e.getExtensions()
                    );
                }).collect(Collectors.toList());
        return new BookieServiceInfo(componentInfoPublisher.getProperties(), endpoints);
    }

}
