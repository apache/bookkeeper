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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_SCOPE;
import static org.apache.bookkeeper.server.component.ServerLifecycleComponent.loadServerComponents;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.CookieValidation;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.bookie.ScrubberStats;
import org.apache.bookkeeper.bookie.UncleanShutdownDetection;
import org.apache.bookkeeper.bookie.UncleanShutdownDetectionImpl;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityCheck;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityCheckImpl;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityCookieValidation;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityService;
import org.apache.bookkeeper.bookie.datainteg.EntryCopier;
import org.apache.bookkeeper.bookie.datainteg.EntryCopierImpl;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.common.component.AutoCloseableLifecycleComponent;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.common.component.RxSchedulerLifecycleComponent;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.UncheckedConfigurationException;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.BookieServiceInfo.Endpoint;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.server.service.AutoRecoveryService;
import org.apache.bookkeeper.server.service.BookieService;
import org.apache.bookkeeper.server.service.HttpService;
import org.apache.bookkeeper.server.service.ScrubberService;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
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
                conf.setBookiePort(Integer.parseInt(sPort));
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
        String hello = String.format(
            "Hello, I'm your bookie, bookieId is %1$s, listening on port %2$s. Metadata service uri is %3$s."
                + " Journals are in %4$s. Ledgers are stored in %5$s.",
            conf.getBookieId() != null ? conf.getBookieId() : "<not-set>",
            conf.getBookiePort(),
            conf.getMetadataServiceUriUnchecked(),
            Arrays.asList(conf.getJournalDirNames()),
            Arrays.asList(conf.getLedgerDirNames()));
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

        // 2. Build metadata driver
        MetadataBookieDriver metadataDriver = BookieResources.createMetadataDriver(
                conf.getServerConf(), rootStatsLogger);
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("metadataDriver", metadataDriver));
        RegistrationManager rm = metadataDriver.createRegistrationManager();
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("registrationManager", rm));

        // 3. Build ledger manager
        LedgerManagerFactory lmFactory = metadataDriver.getLedgerManagerFactory();
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("lmFactory", lmFactory));
        LedgerManager ledgerManager = lmFactory.newLedgerManager();
        serverBuilder.addComponent(new AutoCloseableLifecycleComponent("ledgerManager", ledgerManager));

        // 4. Build bookie
        StatsLogger bookieStats = rootStatsLogger.scope(BOOKIE_SCOPE);
        DiskChecker diskChecker = BookieResources.createDiskChecker(conf.getServerConf());
        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                conf.getServerConf(), diskChecker, bookieStats.scope(LD_LEDGER_SCOPE));
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                conf.getServerConf(), diskChecker, bookieStats.scope(LD_INDEX_SCOPE), ledgerDirsManager);

        ByteBufAllocatorWithOomHandler allocator = BookieResources.createAllocator(conf.getServerConf());

        UncleanShutdownDetection uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
        if (uncleanShutdownDetection.lastShutdownWasUnclean()) {
            log.info("Unclean shutdown detected. The bookie did not register a graceful shutdown prior to this boot.");
        }

        // bookie takes ownership of storage, so shuts it down
        LedgerStorage storage = null;
        DataIntegrityCheck integCheck = null;

        if (conf.getServerConf().isDataIntegrityCheckingEnabled()) {
            StatsLogger clientStats = bookieStats.scope(CLIENT_SCOPE);
            ClientConfiguration clientConfiguration = new ClientConfiguration(conf.getServerConf());
            clientConfiguration.setClientRole(ClientConfiguration.CLIENT_ROLE_SYSTEM);
            BookKeeper bkc = BookKeeper.forConfig(clientConfiguration).statsLogger(clientStats).build();
            serverBuilder.addComponent(new AutoCloseableLifecycleComponent("bkc", bkc));

            BookieId bookieId = BookieImpl.getBookieId(conf.getServerConf());
            ExecutorService rxExecutor = Executors.newFixedThreadPool(
                    2, new ThreadFactoryBuilder().setNameFormat("rx-schedule-%d")
                    .setUncaughtExceptionHandler(
                            (t, ex) -> log.error("Uncaught exception on thread {}", t.getName(), ex))
                    .build());
            Scheduler rxScheduler = Schedulers.from(rxExecutor);
            serverBuilder.addComponent(
                    new RxSchedulerLifecycleComponent("rx-scheduler", conf, bookieStats,
                            rxScheduler, rxExecutor));

            storage = BookieResources.createLedgerStorage(
                    conf.getServerConf(), ledgerManager, ledgerDirsManager, indexDirsManager, bookieStats, allocator);

            EntryCopier copier = new EntryCopierImpl(bookieId,
                    ((org.apache.bookkeeper.client.BookKeeper) bkc).getClientCtx().getBookieClient(),
                    storage, Ticker.systemTicker());

            integCheck = new DataIntegrityCheckImpl(bookieId,
                                                    ledgerManager, storage, copier,
                                                    new BookKeeperAdmin(bkc, clientStats, clientConfiguration),
                                                    rxScheduler);

            // if we're running with journal writes disabled and an unclean shutdown occurred then
            // run the preboot check to protect against data loss and to perform data repair
            if (!conf.getServerConf().getJournalWriteData()
                    && uncleanShutdownDetection.lastShutdownWasUnclean()) {
                integCheck.runPreBootCheck("UNCLEAN_SHUTDOWN");
            }
            CookieValidation cookieValidation = new DataIntegrityCookieValidation(conf.getServerConf(),
                                                                 rm, integCheck);
            cookieValidation.checkCookies(storageDirectoriesFromConf(conf.getServerConf()));
        } else {
            CookieValidation cookieValidation = new LegacyCookieValidation(conf.getServerConf(), rm);
            cookieValidation.checkCookies(storageDirectoriesFromConf(conf.getServerConf()));
            storage = BookieResources.createLedgerStorage(
                    conf.getServerConf(), ledgerManager, ledgerDirsManager, indexDirsManager, bookieStats, allocator);
        }

        Bookie bookie;
        if (conf.getServerConf().isForceReadOnlyBookie()) {
            bookie = new ReadOnlyBookie(conf.getServerConf(), rm, storage,
                                        diskChecker,
                                        ledgerDirsManager, indexDirsManager,
                                        bookieStats, allocator,
                                        bookieServiceInfoProvider);
        } else {
            bookie = new BookieImpl(conf.getServerConf(), rm, storage,
                                    diskChecker,
                                    ledgerDirsManager, indexDirsManager,
                                    bookieStats, allocator,
                                    bookieServiceInfoProvider);
        }

        // 5. build bookie server
        BookieService bookieService =
            new BookieService(conf, bookie, rootStatsLogger, allocator, uncleanShutdownDetection);

        serverBuilder.addComponent(bookieService);
        log.info("Load lifecycle component : {}", BookieService.class.getName());

        if (conf.getServerConf().isLocalScrubEnabled()) {
            serverBuilder.addComponent(
                    new ScrubberService(
                            rootStatsLogger.scope(ScrubberStats.SCOPE),
                    conf, bookieService.getServer().getBookie().getLedgerStorage()));
        }

        // 6. build auto recovery
        if (conf.getServerConf().isAutoRecoveryDaemonEnabled()) {
            AutoRecoveryService autoRecoveryService =
                new AutoRecoveryService(conf, rootStatsLogger.scope(REPLICATION_SCOPE));

            serverBuilder.addComponent(autoRecoveryService);
            log.info("Load lifecycle component : {}", AutoRecoveryService.class.getName());
        }

        // 7. build data integrity check service
        if (conf.getServerConf().isDataIntegrityCheckingEnabled()) {
            checkNotNull(integCheck,
                    "integCheck should have been initialized with the cookie validation");
            DataIntegrityService dataIntegrityService =
                    new DataIntegrityService(conf, rootStatsLogger.scope(REPLICATION_SCOPE),
                    integCheck);
            serverBuilder.addComponent(dataIntegrityService);
            log.info("Load lifecycle component : {}", DataIntegrityService.class.getName());
        }

        // 8. build http service
        if (conf.getServerConf().isHttpServerEnabled()) {
            BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                .setBookieServer(bookieService.getServer())
                .setServerConfiguration(conf.getServerConf())
                .setStatsProvider(statsProviderService.getStatsProvider())
                .setLedgerManagerFactory(metadataDriver.getLedgerManagerFactory())
                .build();
            HttpService httpService =
                new HttpService(provider, conf, rootStatsLogger);
            serverBuilder.addComponent(httpService);
            log.info("Load lifecycle component : {}", HttpService.class.getName());
        }

        // 9. build extra services
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
