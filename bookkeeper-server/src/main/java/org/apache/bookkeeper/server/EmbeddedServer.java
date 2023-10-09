/*
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

package org.apache.bookkeeper.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_SCOPE;
import static org.apache.bookkeeper.server.Main.storageDirectoriesFromConf;
import static org.apache.bookkeeper.server.component.ServerLifecycleComponent.loadServerComponents;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.CookieValidation;
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
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.common.component.RxSchedulerLifecycleComponent;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
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
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.lang3.StringUtils;

/**
 * An embedded server is a server that run bookie and serving rpc requests.
 *
 * <p>
 * It is a rewritten server using {@link org.apache.bookkeeper.common.component.LifecycleComponent}, replacing the
 * legacy server {@link org.apache.bookkeeper.proto.BookieServer}.
 */
public class EmbeddedServer {

    private final LifecycleComponentStack lifecycleComponentStack;

    private final StatsProvider statsProvider;

    private final RegistrationManager registrationManager;

    private final LedgerManagerFactory ledgerManagerFactory;

    private final DiskChecker diskChecker;
    private final LedgerDirsManager ledgerDirsManager;
    private final LedgerDirsManager indexDirsManager;

    private final BookieService bookieService;
    private final AutoRecoveryService autoRecoveryService;
    private final DataIntegrityService dataIntegrityService;
    private final HttpService httpService;

    private EmbeddedServer(LifecycleComponentStack lifecycleComponentStack, StatsProvider statsProvider,
                           RegistrationManager registrationManager, LedgerManagerFactory ledgerManagerFactory,
                           DiskChecker diskChecker, LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager, BookieService bookieService,
                           AutoRecoveryService autoRecoveryService, DataIntegrityService dataIntegrityService,
                           HttpService httpService) {
        this.lifecycleComponentStack = lifecycleComponentStack;
        this.statsProvider = statsProvider;
        this.registrationManager = registrationManager;
        this.ledgerManagerFactory = ledgerManagerFactory;
        this.diskChecker = diskChecker;
        this.ledgerDirsManager = ledgerDirsManager;
        this.indexDirsManager = indexDirsManager;
        this.bookieService = bookieService;
        this.autoRecoveryService = autoRecoveryService;
        this.dataIntegrityService = dataIntegrityService;
        this.httpService = httpService;
    }

    public LifecycleComponentStack getLifecycleComponentStack() {
        return lifecycleComponentStack;
    }

    public StatsProvider getStatsProvider() {
        return statsProvider;
    }

    public RegistrationManager getRegistrationManager() {
        return registrationManager;
    }

    public LedgerManagerFactory getLedgerManagerFactory() {
        return ledgerManagerFactory;
    }

    public DiskChecker getDiskChecker() {
        return diskChecker;
    }

    public LedgerDirsManager getLedgerDirsManager() {
        return ledgerDirsManager;
    }

    public LedgerDirsManager getIndexDirsManager() {
        return indexDirsManager;
    }

    public BookieService getBookieService() {
        return bookieService;
    }

    public AutoRecoveryService getAutoRecoveryService() {
        return autoRecoveryService;
    }

    public DataIntegrityService getDataIntegrityService() {
        return dataIntegrityService;
    }

    public HttpService getHttpService() {
        return httpService;
    }

    /**
     * Create a new builder from given configuration. Actual services implementations can be provided to the builder and
     * will override ones defined in the configuration.
     * <p>
     * Invoker is responsible to start and stop provided services implementations, components from
     * {@link EmbeddedServer#getLifecycleComponentStack()} will reflect only those created from provided configuration.
     *
     * @param conf bookie configuration
     * @return a new embedded server builder
     */
    public static final Builder builder(BookieConfiguration conf) {
        return new Builder(conf);
    }

    @Slf4j
    public static class Builder {

        private BookieConfiguration conf;

        private StatsProvider statsProvider;

        private MetadataBookieDriver metadataDriver;

        private RegistrationManager registrationManager;

        private LedgerManagerFactory ledgerManagerFactory;

        private DiskChecker diskChecker;
        private LedgerDirsManager ledgerDirsManager;
        private LedgerDirsManager indexDirsManager;

        private ByteBufAllocator allocator;
        private UncleanShutdownDetection uncleanShutdownDetection;

        private Builder(BookieConfiguration conf) {
            checkNotNull(conf, "bookieConfiguration cannot be null");

            this.conf = conf;
        }

        public Builder statsProvider(StatsProvider statsProvider) {
            this.statsProvider = statsProvider;
            return this;
        }

        public Builder metadataDriver(MetadataBookieDriver metadataDriver) {
            this.metadataDriver = metadataDriver;
            return this;
        }

        public Builder registrationManager(RegistrationManager registrationManager) {
            this.registrationManager = registrationManager;
            return this;
        }

        public Builder ledgerManagerFactory(LedgerManagerFactory ledgerManagerFactory) {
            this.ledgerManagerFactory = ledgerManagerFactory;
            return this;
        }

        public Builder diskChecker(DiskChecker diskChecker) {
            this.diskChecker = diskChecker;
            return this;
        }

        public Builder ledgerDirsManager(LedgerDirsManager ledgerDirsManager) {
            this.ledgerDirsManager = ledgerDirsManager;
            return this;
        }

        public Builder indexDirsManager(LedgerDirsManager indexDirsManager) {
            this.indexDirsManager = indexDirsManager;
            return this;
        }

        public Builder allocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
            return this;
        }

        public Builder uncleanShutdownDetection(UncleanShutdownDetection uncleanShutdownDetection) {
            this.uncleanShutdownDetection = uncleanShutdownDetection;
            return this;
        }

        /**
         * Build the bookie server.
         *
         * <p>
         * The sequence of the components is:
         *
         * <pre>
         * - stats provider
         * - bookie server
         * - autorecovery daemon
         * - http service
         * </pre>
         *
         * @return lifecycle stack
         * @throws java.lang.Exception
         */
        public EmbeddedServer build() throws Exception {

            final ComponentInfoPublisher componentInfoPublisher = new ComponentInfoPublisher();

            final Supplier<BookieServiceInfo> bookieServiceInfoProvider =
                    () -> buildBookieServiceInfo(componentInfoPublisher);

            LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack
                    .newBuilder()
                    .withComponentInfoPublisher(componentInfoPublisher)
                    .withName("bookie-server");

            // 1. build stats provider
            if (statsProvider == null) {
                StatsProviderService statsProviderService = new StatsProviderService(conf);
                statsProvider = statsProviderService.getStatsProvider();
                serverBuilder.addComponent(statsProviderService);
                log.info("Load lifecycle component : {}", statsProviderService.getName());
            }

            StatsLogger rootStatsLogger = statsProvider.getStatsLogger("");

            // 2. Build metadata driver
            if (metadataDriver == null) {
                if (ledgerManagerFactory == null || registrationManager == null) {
                    metadataDriver = BookieResources.createMetadataDriver(conf.getServerConf(), rootStatsLogger);
                    serverBuilder.addComponent(new AutoCloseableLifecycleComponent("metadataDriver", metadataDriver));
                }
            }

            if (registrationManager == null) {
                registrationManager = metadataDriver.createRegistrationManager();
                serverBuilder.addComponent(
                        new AutoCloseableLifecycleComponent("registrationManager", registrationManager));
            }

            // 3. Build ledger manager
            if (ledgerManagerFactory == null) {
                ledgerManagerFactory = metadataDriver.getLedgerManagerFactory();
                serverBuilder.addComponent(new AutoCloseableLifecycleComponent("lmFactory", ledgerManagerFactory));
            }
            LedgerManager ledgerManager = ledgerManagerFactory.newLedgerManager();
            serverBuilder.addComponent(new AutoCloseableLifecycleComponent("ledgerManager", ledgerManager));

            // 4. Build bookie
            StatsLogger bookieStats = rootStatsLogger.scope(BOOKIE_SCOPE);

            if (diskChecker == null) {
                diskChecker = BookieResources.createDiskChecker(conf.getServerConf());
            }

            if (ledgerDirsManager == null) {
                ledgerDirsManager = BookieResources.createLedgerDirsManager(
                        conf.getServerConf(), diskChecker, bookieStats.scope(LD_LEDGER_SCOPE));
            }

            if (indexDirsManager == null) {
                indexDirsManager = BookieResources.createIndexDirsManager(
                        conf.getServerConf(), diskChecker, bookieStats.scope(LD_INDEX_SCOPE), ledgerDirsManager);
            }

            ByteBufAllocatorWithOomHandler allocatorWithOomHandler;
            if (allocator == null) {
                allocatorWithOomHandler = BookieResources.createAllocator(conf.getServerConf());
                allocator = allocatorWithOomHandler;
            } else {
                if (allocator instanceof ByteBufAllocatorWithOomHandler) {
                    allocatorWithOomHandler = (ByteBufAllocatorWithOomHandler) allocator;
                } else {
                    allocatorWithOomHandler = new ByteBuffAllocatorWrapper(allocator);
                }
            }

            if (uncleanShutdownDetection == null) {
                uncleanShutdownDetection = new UncleanShutdownDetectionImpl(ledgerDirsManager);
            }
            if (uncleanShutdownDetection.lastShutdownWasUnclean()) {
                log.info("Unclean shutdown detected. "
                        + "The bookie did not register a graceful shutdown prior to this boot.");
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

                storage = BookieResources.createLedgerStorage(conf.getServerConf(), ledgerManager,
                        ledgerDirsManager, indexDirsManager, bookieStats, allocator);

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
                        registrationManager, integCheck);
                cookieValidation.checkCookies(storageDirectoriesFromConf(conf.getServerConf()));
            } else {
                CookieValidation cookieValidation =
                        new LegacyCookieValidation(conf.getServerConf(), registrationManager);
                cookieValidation.checkCookies(storageDirectoriesFromConf(conf.getServerConf()));
                // storage should be created after legacy validation or it will fail (it would find ledger dirs)
                storage = BookieResources.createLedgerStorage(conf.getServerConf(), ledgerManager,
                        ledgerDirsManager, indexDirsManager, bookieStats, allocator);
            }

            Bookie bookie;
            if (conf.getServerConf().isForceReadOnlyBookie()) {
                bookie = new ReadOnlyBookie(conf.getServerConf(), registrationManager, storage,
                        diskChecker,
                        ledgerDirsManager, indexDirsManager,
                        bookieStats, allocator,
                        bookieServiceInfoProvider);
            } else {
                bookie = new BookieImpl(conf.getServerConf(), registrationManager, storage,
                        diskChecker,
                        ledgerDirsManager, indexDirsManager,
                        bookieStats, allocator,
                        bookieServiceInfoProvider);
            }

            // 5. build bookie server
            BookieService bookieService =
                    new BookieService(conf, bookie, rootStatsLogger, allocatorWithOomHandler, uncleanShutdownDetection);

            serverBuilder.addComponent(bookieService);
            log.info("Load lifecycle component : {}", bookieService.getName());

            if (conf.getServerConf().isLocalScrubEnabled()) {
                serverBuilder.addComponent(
                        new ScrubberService(
                                rootStatsLogger.scope(ScrubberStats.SCOPE),
                                conf, bookieService.getServer().getBookie().getLedgerStorage()));
            }

            // 6. build auto recovery
            AutoRecoveryService autoRecoveryService = null;
            if (conf.getServerConf().isAutoRecoveryDaemonEnabled()) {
                autoRecoveryService = new AutoRecoveryService(conf, rootStatsLogger.scope(REPLICATION_SCOPE));

                serverBuilder.addComponent(autoRecoveryService);
                log.info("Load lifecycle component : {}", autoRecoveryService.getName());
            }

            // 7. build data integrity check service
            DataIntegrityService dataIntegrityService = null;
            if (conf.getServerConf().isDataIntegrityCheckingEnabled()) {
                checkNotNull(integCheck, "integCheck should have been initialized with the cookie validation");
                dataIntegrityService =
                        new DataIntegrityService(conf, rootStatsLogger.scope(REPLICATION_SCOPE), integCheck);
                serverBuilder.addComponent(dataIntegrityService);
                log.info("Load lifecycle component : {}", dataIntegrityService.getName());
            }

            // 8. build http service
            HttpService httpService = null;
            if (conf.getServerConf().isHttpServerEnabled()) {
                BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                        .setBookieServer(bookieService.getServer())
                        .setServerConfiguration(conf.getServerConf())
                        .setStatsProvider(statsProvider)
                        .setLedgerManagerFactory(ledgerManagerFactory)
                        .build();
                httpService = new HttpService(provider, conf, rootStatsLogger);
                serverBuilder.addComponent(httpService);
                log.info("Load lifecycle component : {}", httpService.getName());
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
                        log.info("Load lifecycle component : {}", component.getName());
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

            return new EmbeddedServer(serverBuilder.build(), statsProvider, registrationManager, ledgerManagerFactory,
                    diskChecker, ledgerDirsManager, indexDirsManager, bookieService, autoRecoveryService,
                    dataIntegrityService, httpService);

        }

        /**
         * Create the {@link BookieServiceInfo} starting from the published endpoints.
         *
         * @see ComponentInfoPublisher
         * @param componentInfoPublisher the endpoint publisher
         * @return the created bookie service info
         */
        private static BookieServiceInfo buildBookieServiceInfo(ComponentInfoPublisher componentInfoPublisher) {
            List<BookieServiceInfo.Endpoint> endpoints = componentInfoPublisher.getEndpoints().values()
                    .stream().map(e -> {
                        return new BookieServiceInfo.Endpoint(
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

    private static final class ByteBuffAllocatorWrapper implements ByteBufAllocatorWithOomHandler {

        private final ByteBufAllocator allocator;

        @Override
        public ByteBuf buffer() {
            return allocator.buffer();
        }

        @Override
        public ByteBuf buffer(int i) {
            return allocator.buffer(i);
        }

        @Override
        public ByteBuf buffer(int i, int i1) {
            return allocator.buffer(i, i1);
        }

        @Override
        public ByteBuf ioBuffer() {
            return allocator.ioBuffer();
        }

        @Override
        public ByteBuf ioBuffer(int i) {
            return allocator.ioBuffer(i);
        }

        @Override
        public ByteBuf ioBuffer(int i, int i1) {
            return allocator.ioBuffer(i, i1);
        }

        @Override
        public ByteBuf heapBuffer() {
            return allocator.heapBuffer();
        }

        @Override
        public ByteBuf heapBuffer(int i) {
            return allocator.heapBuffer(i);
        }

        @Override
        public ByteBuf heapBuffer(int i, int i1) {
            return allocator.heapBuffer(i, i1);
        }

        @Override
        public ByteBuf directBuffer() {
            return allocator.directBuffer();
        }

        @Override
        public ByteBuf directBuffer(int i) {
            return allocator.directBuffer(i);
        }

        @Override
        public ByteBuf directBuffer(int i, int i1) {
            return allocator.directBuffer(i, i1);
        }

        @Override
        public CompositeByteBuf compositeBuffer() {
            return allocator.compositeBuffer();
        }

        @Override
        public CompositeByteBuf compositeBuffer(int i) {
            return allocator.compositeBuffer(i);
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer() {
            return allocator.compositeHeapBuffer();
        }

        @Override
        public CompositeByteBuf compositeHeapBuffer(int i) {
            return allocator.compositeHeapBuffer(i);
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer() {
            return allocator.compositeDirectBuffer();
        }

        @Override
        public CompositeByteBuf compositeDirectBuffer(int i) {
            return allocator.compositeDirectBuffer(i);
        }

        @Override
        public boolean isDirectBufferPooled() {
            return allocator.isDirectBufferPooled();
        }

        @Override
        public int calculateNewCapacity(int i, int i1) {
            return allocator.calculateNewCapacity(i, i1);
        }

        public ByteBuffAllocatorWrapper(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public void setOomHandler(Consumer<OutOfMemoryError> handler) {
            // NOP
        }
    }

}
