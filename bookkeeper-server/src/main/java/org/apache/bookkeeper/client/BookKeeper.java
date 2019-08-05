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
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WATCHER_SCOPE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.IsClosedCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCreateAdvCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncCreateCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncDeleteCallback;
import org.apache.bookkeeper.client.SyncCallbackUtils.SyncOpenCallback;
import org.apache.bookkeeper.client.api.BookKeeperBuilder;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.DeleteBuilder;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.meta.CleanupLedgerManager;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataClientDriver;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieClientImpl;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BookKeeper client.
 *
 * <p>We assume there is one single writer to a ledger at any time.
 *
 * <p>There are four possible operations: start a new ledger, write to a ledger,
 * read from a ledger and delete a ledger.
 *
 * <p>The exceptions resulting from synchronous calls and error code resulting from
 * asynchronous calls can be found in the class {@link BKException}.
 */
public class BookKeeper implements org.apache.bookkeeper.client.api.BookKeeper {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeper.class);


    final EventLoopGroup eventLoopGroup;
    private final ByteBufAllocator allocator;

    // The stats logger for this client.
    private final StatsLogger statsLogger;
    private final BookKeeperClientStats clientStats;

    // whether the event loop group is one we created, or is owned by whoever
    // instantiated us
    boolean ownEventLoopGroup = false;

    final BookieClient bookieClient;
    final BookieWatcherImpl bookieWatcher;

    final OrderedExecutor mainWorkerPool;
    final OrderedScheduler scheduler;
    final HashedWheelTimer requestTimer;
    final boolean ownTimer;
    final FeatureProvider featureProvider;
    final ScheduledExecutorService bookieInfoScheduler;

    final MetadataClientDriver metadataDriver;
    // Ledger manager responsible for how to store ledger meta data
    final LedgerManagerFactory ledgerManagerFactory;
    final LedgerManager ledgerManager;
    final LedgerIdGenerator ledgerIdGenerator;

    // Ensemble Placement Policy
    final EnsemblePlacementPolicy placementPolicy;
    BookieInfoReader bookieInfoReader;

    final ClientConfiguration conf;
    final ClientInternalConf internalConf;

    // Close State
    boolean closed = false;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    /**
     * BookKeeper Client Builder to build client instances.
     *
     * @see BookKeeperBuilder
     */
    public static class Builder {
        final ClientConfiguration conf;

        ZooKeeper zk = null;
        EventLoopGroup eventLoopGroup = null;
        ByteBufAllocator allocator = null;
        StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        DNSToSwitchMapping dnsResolver = null;
        HashedWheelTimer requestTimer = null;
        FeatureProvider featureProvider = null;

        Builder(ClientConfiguration conf) {
            this.conf = conf;
        }

        /**
         * Configure the bookkeeper client with a provided {@link EventLoopGroup}.
         *
         * @param f an external {@link EventLoopGroup} to use by the bookkeeper client.
         * @return client builder.
         * @deprecated since 4.5, use {@link #eventLoopGroup(EventLoopGroup)}
         * @see #eventLoopGroup(EventLoopGroup)
         */
        @Deprecated
        public Builder setEventLoopGroup(EventLoopGroup f) {
            eventLoopGroup = f;
            return this;
        }

        /**
         * Configure the bookkeeper client with a provided {@link ZooKeeper} client.
         *
         * @param zk an external {@link ZooKeeper} client to use by the bookkeeper client.
         * @return client builder.
         * @deprecated since 4.5, use {@link #zk(ZooKeeper)}
         * @see #zk(ZooKeeper)
         */
        @Deprecated
        public Builder setZookeeper(ZooKeeper zk) {
            this.zk = zk;
            return this;
        }

        /**
         * Configure the bookkeeper client with a provided {@link StatsLogger}.
         *
         * @param statsLogger an {@link StatsLogger} to use by the bookkeeper client to collect stats generated
         *                    by the client.
         * @return client builder.
         * @deprecated since 4.5, use {@link #statsLogger(StatsLogger)}
         * @see #statsLogger(StatsLogger)
         */
        @Deprecated
        public Builder setStatsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        /**
         * Configure the bookkeeper client with a provided {@link EventLoopGroup}.
         *
         * @param f an external {@link EventLoopGroup} to use by the bookkeeper client.
         * @return client builder.
         * @since 4.5
         */
        public Builder eventLoopGroup(EventLoopGroup f) {
            eventLoopGroup = f;
            return this;
        }

        /**
         * Configure the bookkeeper client with a provided {@link ByteBufAllocator}.
         *
         * @param allocator an external {@link ByteBufAllocator} to use by the bookkeeper client.
         * @return client builder.
         * @since 4.9
         */
        public Builder allocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
            return this;
        }

        /**
         * Configure the bookkeeper client with a provided {@link ZooKeeper} client.
         *
         * @param zk an external {@link ZooKeeper} client to use by the bookkeeper client.
         * @return client builder.
         * @since 4.5
         */
        @Deprecated
        public Builder zk(ZooKeeper zk) {
            this.zk = zk;
            return this;
        }

        /**
         * Configure the bookkeeper client with a provided {@link StatsLogger}.
         *
         * @param statsLogger an {@link StatsLogger} to use by the bookkeeper client to collect stats generated
         *                    by the client.
         * @return client builder.
         * @since 4.5
         */
        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        /**
         * Configure the bookkeeper client to use the provided dns resolver {@link DNSToSwitchMapping}.
         *
         * @param dnsResolver dns resolver for placement policy to use for resolving network locations.
         * @return client builder
         * @since 4.5
         */
        public Builder dnsResolver(DNSToSwitchMapping dnsResolver) {
            this.dnsResolver = dnsResolver;
            return this;
        }

        /**
         * Configure the bookkeeper client to use a provided {@link HashedWheelTimer}.
         *
         * @param requestTimer request timer for client to manage timer related tasks.
         * @return client builder
         * @since 4.5
         */
        public Builder requestTimer(HashedWheelTimer requestTimer) {
            this.requestTimer = requestTimer;
            return this;
        }

        /**
         * Feature Provider.
         *
         * @param featureProvider
         * @return
         */
        public Builder featureProvider(FeatureProvider featureProvider) {
            this.featureProvider = featureProvider;
            return this;
        }

        public BookKeeper build() throws IOException, InterruptedException, BKException {
            checkNotNull(statsLogger, "No stats logger provided");
            return new BookKeeper(conf, zk, eventLoopGroup, allocator, statsLogger, dnsResolver, requestTimer,
                    featureProvider);
        }
    }

    public static Builder forConfig(final ClientConfiguration conf) {
        return new Builder(conf);
    }

    /**
     * Create a bookkeeper client. A zookeeper client and a client event loop group
     * will be instantiated as part of this constructor.
     *
     * @param servers
     *          A list of one of more servers on which zookeeper is running. The
     *          client assumes that the running bookies have been registered with
     *          zookeeper under the path
     *          {@link AbstractConfiguration#getZkAvailableBookiesPath()}
     * @throws IOException
     * @throws InterruptedException
     */
    public BookKeeper(String servers) throws IOException, InterruptedException,
        BKException {
        this(new ClientConfiguration().setMetadataServiceUri("zk+null://" + servers + "/ledgers"));
    }

    /**
     * Create a bookkeeper client using a configuration object.
     * A zookeeper client and a client event loop group will be
     * instantiated as part of this constructor.
     *
     * @param conf
     *          Client Configuration object
     * @throws IOException
     * @throws InterruptedException
     */
    public BookKeeper(final ClientConfiguration conf)
            throws IOException, InterruptedException, BKException {
        this(conf, null, null, null, NullStatsLogger.INSTANCE,
                null, null, null);
    }

    private static ZooKeeper validateZooKeeper(ZooKeeper zk) throws NullPointerException, IOException {
        checkNotNull(zk, "No zookeeper instance provided");
        if (!zk.getState().isConnected()) {
            LOG.error("Unconnected zookeeper handle passed to bookkeeper");
            throw new IOException(KeeperException.create(KeeperException.Code.CONNECTIONLOSS));
        }
        return zk;
    }

    private static EventLoopGroup validateEventLoopGroup(EventLoopGroup eventLoopGroup)
            throws NullPointerException {
        checkNotNull(eventLoopGroup, "No Event Loop Group provided");
        return eventLoopGroup;
    }

    /**
     * Create a bookkeeper client but use the passed in zookeeper client instead
     * of instantiating one.
     *
     * @param conf
     *          Client Configuration object
     *          {@link ClientConfiguration}
     * @param zk
     *          Zookeeper client instance connected to the zookeeper with which
     *          the bookies have registered
     * @throws IOException
     * @throws InterruptedException
     */
    public BookKeeper(ClientConfiguration conf, ZooKeeper zk)
            throws IOException, InterruptedException, BKException {
        this(conf, validateZooKeeper(zk), null, null, NullStatsLogger.INSTANCE, null, null, null);
    }

    /**
     * Create a bookkeeper client but use the passed in zookeeper client and
     * client event loop group instead of instantiating those.
     *
     * @param conf
     *          Client Configuration Object
     *          {@link ClientConfiguration}
     * @param zk
     *          Zookeeper client instance connected to the zookeeper with which
     *          the bookies have registered. The ZooKeeper client must be connected
     *          before it is passed to BookKeeper. Otherwise a KeeperException is thrown.
     * @param eventLoopGroup
     *          An event loop group that will be used to create connections to the bookies
     * @throws IOException
     * @throws InterruptedException
     * @throws BKException in the event of a bookkeeper connection error
     */
    public BookKeeper(ClientConfiguration conf, ZooKeeper zk, EventLoopGroup eventLoopGroup)
            throws IOException, InterruptedException, BKException {
        this(conf, validateZooKeeper(zk), validateEventLoopGroup(eventLoopGroup), null, NullStatsLogger.INSTANCE,
                null, null, null);
    }

    /**
     * Constructor for use with the builder. Other constructors also use it.
     */
    @SuppressWarnings("deprecation")
    @VisibleForTesting
    BookKeeper(ClientConfiguration conf,
                       ZooKeeper zkc,
                       EventLoopGroup eventLoopGroup,
                       ByteBufAllocator byteBufAllocator,
                       StatsLogger rootStatsLogger,
                       DNSToSwitchMapping dnsResolver,
                       HashedWheelTimer requestTimer,
                       FeatureProvider featureProvider)
            throws IOException, InterruptedException, BKException {
        this.conf = conf;
        // initialize feature provider
        if (null == featureProvider) {
            this.featureProvider = SettableFeatureProvider.DISABLE_ALL;
        } else {
            this.featureProvider = featureProvider;
        }

        this.internalConf = ClientInternalConf.fromConfigAndFeatureProvider(conf, this.featureProvider);

        // initialize resources
        this.scheduler = OrderedScheduler.newSchedulerBuilder().numThreads(1).name("BookKeeperClientScheduler").build();
        this.mainWorkerPool = OrderedExecutor.newBuilder()
                .name("BookKeeperClientWorker")
                .numThreads(conf.getNumWorkerThreads())
                .statsLogger(rootStatsLogger)
                .traceTaskExecution(conf.getEnableTaskExecutionStats())
                .preserveMdcForTaskExecution(conf.getPreserveMdcForTaskExecution())
                .traceTaskWarnTimeMicroSec(conf.getTaskExecutionWarnTimeMicros())
                .enableBusyWait(conf.isBusyWaitEnabled())
                .build();

        // initialize stats logger
        this.statsLogger = rootStatsLogger.scope(BookKeeperClientStats.CLIENT_SCOPE);
        this.clientStats = BookKeeperClientStats.newInstance(this.statsLogger);

        // initialize metadata driver
        try {
            String metadataServiceUriStr = conf.getMetadataServiceUri();
            if (null != metadataServiceUriStr) {
                this.metadataDriver = MetadataDrivers.getClientDriver(URI.create(metadataServiceUriStr));
            } else {
                checkNotNull(zkc, "No external zookeeper provided when no metadata service uri is found");
                this.metadataDriver = MetadataDrivers.getClientDriver("zk");
            }
            this.metadataDriver.initialize(
                conf,
                scheduler,
                rootStatsLogger,
                java.util.Optional.ofNullable(zkc));
        } catch (ConfigurationException ce) {
            LOG.error("Failed to initialize metadata client driver using invalid metadata service uri", ce);
            throw new IOException("Failed to initialize metadata client driver", ce);
        } catch (MetadataException me) {
            LOG.error("Encountered metadata exceptions on initializing metadata client driver", me);
            throw new IOException("Failed to initialize metadata client driver", me);
        }

        // initialize event loop group
        if (null == eventLoopGroup) {
            this.eventLoopGroup = EventLoopUtil.getClientEventLoopGroup(conf,
                    new DefaultThreadFactory("bookkeeper-io"));
            this.ownEventLoopGroup = true;
        } else {
            this.eventLoopGroup = eventLoopGroup;
            this.ownEventLoopGroup = false;
        }

        if (byteBufAllocator != null) {
            this.allocator = byteBufAllocator;
        } else {
            this.allocator = ByteBufAllocatorBuilder.create()
                    .poolingPolicy(conf.getAllocatorPoolingPolicy())
                    .poolingConcurrency(conf.getAllocatorPoolingConcurrency())
                    .outOfMemoryPolicy(conf.getAllocatorOutOfMemoryPolicy())
                    .leakDetectionPolicy(conf.getAllocatorLeakDetectionPolicy())
                    .build();
        }

        // initialize bookie client
        this.bookieClient = new BookieClientImpl(conf, this.eventLoopGroup, this.allocator, this.mainWorkerPool,
                scheduler, rootStatsLogger);

        if (null == requestTimer) {
            this.requestTimer = new HashedWheelTimer(
                    new ThreadFactoryBuilder().setNameFormat("BookieClientTimer-%d").build(),
                    conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                    conf.getTimeoutTimerNumTicks());
            this.ownTimer = true;
        } else {
            this.requestTimer = requestTimer;
            this.ownTimer = false;
        }

        // initialize the ensemble placement
        this.placementPolicy = initializeEnsemblePlacementPolicy(conf,
                dnsResolver, this.requestTimer, this.featureProvider, this.statsLogger);


        this.bookieWatcher = new BookieWatcherImpl(
                conf, this.placementPolicy, metadataDriver.getRegistrationClient(),
                this.statsLogger.scope(WATCHER_SCOPE));
        if (conf.getDiskWeightBasedPlacementEnabled()) {
            LOG.info("Weighted ledger placement enabled");
            ThreadFactoryBuilder tFBuilder = new ThreadFactoryBuilder()
                    .setNameFormat("BKClientMetaDataPollScheduler-%d");
            this.bookieInfoScheduler = Executors.newSingleThreadScheduledExecutor(tFBuilder.build());
            this.bookieInfoReader = new BookieInfoReader(this, conf, this.bookieInfoScheduler);
            this.bookieWatcher.initialBlockingBookieRead();
            this.bookieInfoReader.start();
        } else {
            LOG.info("Weighted ledger placement is not enabled");
            this.bookieInfoScheduler = null;
            this.bookieInfoReader = new BookieInfoReader(this, conf, null);
            this.bookieWatcher.initialBlockingBookieRead();
        }

        // initialize ledger manager
        try {
            this.ledgerManagerFactory =
                this.metadataDriver.getLedgerManagerFactory();
        } catch (MetadataException e) {
            throw new IOException("Failed to initialize ledger manager factory", e);
        }
        this.ledgerManager = new CleanupLedgerManager(ledgerManagerFactory.newLedgerManager());
        this.ledgerIdGenerator = ledgerManagerFactory.newLedgerIdGenerator();

        scheduleBookieHealthCheckIfEnabled(conf);
    }

    /**
     * Allow to extend BookKeeper for mocking in unit tests.
     */
    @VisibleForTesting
    BookKeeper() {
        conf = new ClientConfiguration();
        internalConf = ClientInternalConf.fromConfig(conf);
        statsLogger = NullStatsLogger.INSTANCE;
        clientStats = BookKeeperClientStats.newInstance(statsLogger);
        scheduler = null;
        requestTimer = null;
        metadataDriver = null;
        placementPolicy = null;
        ownTimer = false;
        mainWorkerPool = null;
        ledgerManagerFactory = null;
        ledgerManager = null;
        ledgerIdGenerator = null;
        featureProvider = null;
        eventLoopGroup = null;
        bookieWatcher = null;
        bookieInfoScheduler = null;
        bookieClient = null;
        allocator = UnpooledByteBufAllocator.DEFAULT;
    }

    private EnsemblePlacementPolicy initializeEnsemblePlacementPolicy(ClientConfiguration conf,
                                                                      DNSToSwitchMapping dnsResolver,
                                                                      HashedWheelTimer timer,
                                                                      FeatureProvider featureProvider,
                                                                      StatsLogger statsLogger)
        throws IOException {
        try {
            Class<? extends EnsemblePlacementPolicy> policyCls = conf.getEnsemblePlacementPolicy();
            return ReflectionUtils.newInstance(policyCls).initialize(conf, java.util.Optional.ofNullable(dnsResolver),
                    timer, featureProvider, statsLogger);
        } catch (ConfigurationException e) {
            throw new IOException("Failed to initialize ensemble placement policy : ", e);
        }
    }

    int getReturnRc(int rc) {
        return getReturnRc(bookieClient, rc);
    }

    static int getReturnRc(BookieClient bookieClient, int rc) {
        if (BKException.Code.OK == rc) {
            return rc;
        } else {
            if (bookieClient.isClosed()) {
                return BKException.Code.ClientClosedException;
            } else {
                return rc;
            }
        }
    }

    void scheduleBookieHealthCheckIfEnabled(ClientConfiguration conf) {
        if (conf.isBookieHealthCheckEnabled()) {
            scheduler.scheduleAtFixedRate(new SafeRunnable() {

                @Override
                public void safeRun() {
                    checkForFaultyBookies();
                }
                    }, conf.getBookieHealthCheckIntervalSeconds(), conf.getBookieHealthCheckIntervalSeconds(),
                    TimeUnit.SECONDS);
        }
    }

    void checkForFaultyBookies() {
        List<BookieSocketAddress> faultyBookies = bookieClient.getFaultyBookies();
        for (BookieSocketAddress faultyBookie : faultyBookies) {
            bookieWatcher.quarantineBookie(faultyBookie);
        }
    }

    /**
     * Returns ref to speculative read counter, needed in PendingReadOp.
     */
    @VisibleForTesting
    public LedgerManager getLedgerManager() {
        return ledgerManager;
    }

    @VisibleForTesting
    LedgerManager getUnderlyingLedgerManager() {
        return ((CleanupLedgerManager) ledgerManager).getUnderlying();
    }

    @VisibleForTesting
    LedgerIdGenerator getLedgerIdGenerator() {
        return ledgerIdGenerator;
    }

    @VisibleForTesting
    ReentrantReadWriteLock getCloseLock() {
        return closeLock;
    }

    @VisibleForTesting
    boolean isClosed() {
        return closed;
    }

    @VisibleForTesting
    BookieWatcher getBookieWatcher() {
        return bookieWatcher;
    }

    public OrderedExecutor getMainWorkerPool() {
        return mainWorkerPool;
    }

    @VisibleForTesting
    OrderedScheduler getScheduler() {
        return scheduler;
    }

    @VisibleForTesting
    EnsemblePlacementPolicy getPlacementPolicy() {
        return placementPolicy;
    }

    @VisibleForTesting
    public MetadataClientDriver getMetadataClientDriver() {
        return metadataDriver;
    }

    /**
     * There are 3 digest types that can be used for verification. The CRC32 is
     * cheap to compute but does not protect against byzantine bookies (i.e., a
     * bookie might report fake bytes and a matching CRC32). The MAC code is more
     * expensive to compute, but is protected by a password, i.e., a bookie can't
     * report fake bytes with a mathching MAC unless it knows the password.
     * The CRC32C, which use SSE processor instruction, has better performance than CRC32.
     * Legacy DigestType for backward compatibility. If we want to add new DigestType,
     * we should add it in here, client.api.DigestType and DigestType in DataFormats.proto.
     * If the digest type is set/passed in as DUMMY, a dummy digest is added/checked.
     * This DUMMY digest is mostly for test purposes or in situations/use-cases
     * where digest is considered a overhead.
     */
    public enum DigestType {
        MAC, CRC32, CRC32C, DUMMY;

        public static DigestType fromApiDigestType(org.apache.bookkeeper.client.api.DigestType digestType) {
            switch (digestType) {
                case MAC:
                    return DigestType.MAC;
                case CRC32:
                    return DigestType.CRC32;
                case CRC32C:
                    return DigestType.CRC32C;
                case DUMMY:
                    return DigestType.DUMMY;
                default:
                    throw new IllegalArgumentException("Unable to convert digest type " + digestType);
            }
        }
        public static DataFormats.LedgerMetadataFormat.DigestType toProtoDigestType(DigestType digestType) {
            switch (digestType) {
                case MAC:
                    return DataFormats.LedgerMetadataFormat.DigestType.HMAC;
                case CRC32:
                    return DataFormats.LedgerMetadataFormat.DigestType.CRC32;
                case CRC32C:
                    return DataFormats.LedgerMetadataFormat.DigestType.CRC32C;
                case DUMMY:
                    return DataFormats.LedgerMetadataFormat.DigestType.DUMMY;
                default:
                    throw new IllegalArgumentException("Unable to convert digest type " + digestType);
            }
        }
        public org.apache.bookkeeper.client.api.DigestType toApiDigestType() {
            switch (this) {
                case MAC:
                    return org.apache.bookkeeper.client.api.DigestType.MAC;
                case CRC32:
                    return org.apache.bookkeeper.client.api.DigestType.CRC32;
                case CRC32C:
                    return org.apache.bookkeeper.client.api.DigestType.CRC32C;
                case DUMMY:
                    return org.apache.bookkeeper.client.api.DigestType.DUMMY;
                default:
                    throw new IllegalArgumentException("Unable to convert digest type " + this);
            }
        }
    }

    ZooKeeper getZkHandle() {
        return ((ZKMetadataClientDriver) metadataDriver).getZk();
    }

    protected ClientConfiguration getConf() {
        return conf;
    }

    StatsLogger getStatsLogger() {
        return statsLogger;
    }

    /**
     * Get the BookieClient, currently used for doing bookie recovery.
     *
     * @return BookieClient for the BookKeeper instance.
     */
    BookieClient getBookieClient() {
        return bookieClient;
    }

    /**
     * Retrieves BookieInfo from all the bookies in the cluster. It sends requests
     * to all the bookies in parallel and returns the info from the bookies that responded.
     * If there was an error in reading from any bookie, nothing will be returned for
     * that bookie in the map.
     * @return map
     *             A map of bookieSocketAddress to its BookiInfo
     * @throws BKException
     * @throws InterruptedException
     */
    public Map<BookieSocketAddress, BookieInfo> getBookieInfo() throws BKException, InterruptedException {
        return bookieInfoReader.getBookieInfo();
    }

    /**
     * Creates a new ledger asynchronously. To create a ledger, we need to specify
     * the ensemble size, the quorum size, the digest type, a password, a callback
     * implementation, and an optional control object. The ensemble size is how
     * many bookies the entries should be striped among and the quorum size is the
     * degree of replication of each entry. The digest type is either a MAC or a
     * CRC. Note that the CRC option is not able to protect a client against a
     * bookie that replaces an entry. The password is used not only to
     * authenticate access to a ledger, but also to verify entries in ledgers.
     *
     * @param ensSize
     *          number of bookies over which to stripe entries
     * @param writeQuorumSize
     *          number of bookies each entry will be written to. each of these bookies
     *          must acknowledge the entry before the call is completed.
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @param cb
     *          createCallback implementation
     * @param ctx
     *          optional control object
     */
    public void asyncCreateLedger(final int ensSize,
                                  final int writeQuorumSize,
                                  final DigestType digestType,
                                  final byte[] passwd, final CreateCallback cb, final Object ctx) {
        asyncCreateLedger(ensSize, writeQuorumSize, writeQuorumSize,
                          digestType, passwd, cb, ctx, Collections.emptyMap());
    }

    /**
     * Creates a new ledger asynchronously. Ledgers created with this call have
     * a separate write quorum and ack quorum size. The write quorum must be larger than
     * the ack quorum.
     *
     * <p>Separating the write and the ack quorum allows the BookKeeper client to continue
     * writing when a bookie has failed but the failure has not yet been detected. Detecting
     * a bookie has failed can take a number of seconds, as configured by the read timeout
     * {@link ClientConfiguration#getReadTimeout()}. Once the bookie failure is detected,
     * that bookie will be removed from the ensemble.
     *
     * <p>The other parameters match those of {@link #asyncCreateLedger(int, int, DigestType, byte[],
     *                                      AsyncCallback.CreateCallback, Object)}
     *
     * @param ensSize
     *          number of bookies over which to stripe entries
     * @param writeQuorumSize
     *          number of bookies each entry will be written to
     * @param ackQuorumSize
     *          number of bookies which must acknowledge an entry before the call is completed
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @param cb
     *          createCallback implementation
     * @param ctx
     *          optional control object
     * @param customMetadata
     *          optional customMetadata that holds user specified metadata
     */

    public void asyncCreateLedger(final int ensSize, final int writeQuorumSize, final int ackQuorumSize,
                                  final DigestType digestType, final byte[] passwd,
                                  final CreateCallback cb, final Object ctx, final Map<String, byte[]> customMetadata) {
        if (writeQuorumSize < ackQuorumSize) {
            throw new IllegalArgumentException("Write quorum must be larger than ack quorum");
        }
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                return;
            }
            new LedgerCreateOp(BookKeeper.this, ensSize, writeQuorumSize,
                               ackQuorumSize, digestType, passwd, cb, ctx,
                               customMetadata, WriteFlag.NONE, clientStats)
                .initiate();
        } finally {
            closeLock.readLock().unlock();
        }
    }


    /**
     * Creates a new ledger. Default of 3 servers, and quorum of 2 servers.
     *
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedger(DigestType digestType, byte[] passwd)
            throws BKException, InterruptedException {
        return createLedger(3, 2, digestType, passwd);
    }

    /**
     * Synchronous call to create ledger. Parameters match those of
     * {@link #asyncCreateLedger(int, int, DigestType, byte[],
     *                           AsyncCallback.CreateCallback, Object)}
     *
     * @param ensSize
     * @param qSize
     * @param digestType
     * @param passwd
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedger(int ensSize, int qSize,
                                     DigestType digestType, byte[] passwd)
            throws InterruptedException, BKException {
        return createLedger(ensSize, qSize, qSize, digestType, passwd, Collections.emptyMap());
    }

    /**
     * Synchronous call to create ledger. Parameters match those of
     * {@link #asyncCreateLedger(int, int, DigestType, byte[],
     *                           AsyncCallback.CreateCallback, Object)}
     *
     * @param ensSize
     * @param writeQuorumSize
     * @param ackQuorumSize
     * @param digestType
     * @param passwd
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize,
            DigestType digestType, byte[] passwd)
            throws InterruptedException, BKException {
        return createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, Collections.emptyMap());
    }

    /**
     * Synchronous call to create ledger. Parameters match those of asyncCreateLedger
     *
     * @param ensSize
     * @param writeQuorumSize
     * @param ackQuorumSize
     * @param digestType
     * @param passwd
     * @param customMetadata
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize,
                                     DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata)
            throws InterruptedException, BKException {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        SyncCreateCallback result = new SyncCreateCallback(future);

        /*
         * Calls asynchronous version
         */
        asyncCreateLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd,
                          result, null, customMetadata);

        LedgerHandle lh = SyncCallbackUtils.waitForResult(future);
        if (lh == null) {
            LOG.error("Unexpected condition : no ledger handle returned for a success ledger creation");
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }
        return lh;
    }

    /**
     * Synchronous call to create ledger.
     * Creates a new ledger asynchronously and returns {@link LedgerHandleAdv} which can accept entryId.
     * Parameters must match those of asyncCreateLedgerAdv
     *
     * @param ensSize
     * @param writeQuorumSize
     * @param ackQuorumSize
     * @param digestType
     * @param passwd
     *
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedgerAdv(int ensSize, int writeQuorumSize, int ackQuorumSize,
                                        DigestType digestType, byte[] passwd)
            throws InterruptedException, BKException {
        return createLedgerAdv(ensSize, writeQuorumSize, ackQuorumSize,
                               digestType, passwd, Collections.emptyMap());
    }

    /**
     * Synchronous call to create ledger.
     * Creates a new ledger asynchronously and returns {@link LedgerHandleAdv} which can accept entryId.
     * Parameters must match those of asyncCreateLedgerAdv
     *
     * @param ensSize
     * @param writeQuorumSize
     * @param ackQuorumSize
     * @param digestType
     * @param passwd
     * @param customMetadata
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedgerAdv(int ensSize, int writeQuorumSize, int ackQuorumSize,
                                        DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata)
            throws InterruptedException, BKException {
        CompletableFuture<LedgerHandleAdv> future = new CompletableFuture<>();
        SyncCreateAdvCallback result = new SyncCreateAdvCallback(future);

        /*
         * Calls asynchronous version
         */
        asyncCreateLedgerAdv(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd,
                             result, null, customMetadata);

        LedgerHandle lh = SyncCallbackUtils.waitForResult(future);
        if (lh == null) {
            LOG.error("Unexpected condition : no ledger handle returned for a success ledger creation");
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }
        return lh;
    }

    /**
     * Creates a new ledger asynchronously and returns {@link LedgerHandleAdv}
     * which can accept entryId.  Ledgers created with this call have ability to accept
     * a separate write quorum and ack quorum size. The write quorum must be larger than
     * the ack quorum.
     *
     * <p>Separating the write and the ack quorum allows the BookKeeper client to continue
     * writing when a bookie has failed but the failure has not yet been detected. Detecting
     * a bookie has failed can take a number of seconds, as configured by the read timeout
     * {@link ClientConfiguration#getReadTimeout()}. Once the bookie failure is detected,
     * that bookie will be removed from the ensemble.
     *
     * <p>The other parameters match those of {@link #asyncCreateLedger(int, int, DigestType, byte[],
     *                                      AsyncCallback.CreateCallback, Object)}
     *
     * @param ensSize
     *          number of bookies over which to stripe entries
     * @param writeQuorumSize
     *          number of bookies each entry will be written to
     * @param ackQuorumSize
     *          number of bookies which must acknowledge an entry before the call is completed
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @param cb
     *          createCallback implementation
     * @param ctx
     *          optional control object
     * @param customMetadata
     *          optional customMetadata that holds user specified metadata
     */
    public void asyncCreateLedgerAdv(final int ensSize, final int writeQuorumSize, final int ackQuorumSize,
            final DigestType digestType, final byte[] passwd, final CreateCallback cb, final Object ctx,
            final Map<String, byte[]> customMetadata) {
        if (writeQuorumSize < ackQuorumSize) {
            throw new IllegalArgumentException("Write quorum must be larger than ack quorum");
        }
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                return;
            }
            new LedgerCreateOp(BookKeeper.this, ensSize, writeQuorumSize,
                               ackQuorumSize, digestType, passwd, cb, ctx,
                               customMetadata, WriteFlag.NONE, clientStats)
                                       .initiateAdv(-1L);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * Synchronously creates a new ledger using the interface which accepts a ledgerId as input.
     * This method returns {@link LedgerHandleAdv} which can accept entryId.
     * Parameters must match those of asyncCreateLedgerAdvWithLedgerId
     * @param ledgerId
     * @param ensSize
     * @param writeQuorumSize
     * @param ackQuorumSize
     * @param digestType
     * @param passwd
     * @param customMetadata
     * @return a handle to the newly created ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle createLedgerAdv(final long ledgerId,
                                        int ensSize,
                                        int writeQuorumSize,
                                        int ackQuorumSize,
                                        DigestType digestType,
                                        byte[] passwd,
                                        final Map<String, byte[]> customMetadata)
            throws InterruptedException, BKException {
        CompletableFuture<LedgerHandleAdv> future = new CompletableFuture<>();
        SyncCreateAdvCallback result = new SyncCreateAdvCallback(future);

        /*
         * Calls asynchronous version
         */
        asyncCreateLedgerAdv(ledgerId, ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd,
                             result, null, customMetadata);

        LedgerHandle lh = SyncCallbackUtils.waitForResult(future);
        if (lh == null) {
            LOG.error("Unexpected condition : no ledger handle returned for a success ledger creation");
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        } else if (ledgerId != lh.getId()) {
            LOG.error("Unexpected condition : Expected ledgerId: {} but got: {}", ledgerId, lh.getId());
            throw BKException.create(BKException.Code.UnexpectedConditionException);
        }

        LOG.info("Ensemble: {} for ledger: {}", lh.getLedgerMetadata().getEnsembleAt(0L), lh.getId());

        return lh;
    }

    /**
     * Asynchronously creates a new ledger using the interface which accepts a ledgerId as input.
     * This method returns {@link LedgerHandleAdv} which can accept entryId.
     * Ledgers created with this call have ability to accept
     * a separate write quorum and ack quorum size. The write quorum must be larger than
     * the ack quorum.
     *
     * <p>Separating the write and the ack quorum allows the BookKeeper client to continue
     * writing when a bookie has failed but the failure has not yet been detected. Detecting
     * a bookie has failed can take a number of seconds, as configured by the read timeout
     * {@link ClientConfiguration#getReadTimeout()}. Once the bookie failure is detected,
     * that bookie will be removed from the ensemble.
     *
     * <p>The other parameters match those of asyncCreateLedger</p>
     *
     * @param ledgerId
     *          ledger Id to use for the newly created ledger
     * @param ensSize
     *          number of bookies over which to stripe entries
     * @param writeQuorumSize
     *          number of bookies each entry will be written to
     * @param ackQuorumSize
     *          number of bookies which must acknowledge an entry before the call is completed
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @param cb
     *          createCallback implementation
     * @param ctx
     *          optional control object
     * @param customMetadata
     *          optional customMetadata that holds user specified metadata
     */
    public void asyncCreateLedgerAdv(final long ledgerId,
                                     final int ensSize,
                                     final int writeQuorumSize,
                                     final int ackQuorumSize,
                                     final DigestType digestType,
                                     final byte[] passwd,
                                     final CreateCallback cb,
                                     final Object ctx,
                                     final Map<String, byte[]> customMetadata) {
        if (writeQuorumSize < ackQuorumSize) {
            throw new IllegalArgumentException("Write quorum must be larger than ack quorum");
        }
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.createComplete(BKException.Code.ClientClosedException, null, ctx);
                return;
            }
            new LedgerCreateOp(BookKeeper.this, ensSize, writeQuorumSize,
                               ackQuorumSize, digestType, passwd, cb, ctx,
                               customMetadata, WriteFlag.NONE, clientStats)
                    .initiateAdv(ledgerId);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * Open existing ledger asynchronously for reading.
     *
     * <p>Opening a ledger with this method invokes fencing and recovery on the ledger
     * if the ledger has not been closed. Fencing will block all other clients from
     * writing to the ledger. Recovery will make sure that the ledger is closed
     * before reading from it.
     *
     * <p>Recovery also makes sure that any entries which reached one bookie, but not a
     * quorum, will be replicated to a quorum of bookies. This occurs in cases were
     * the writer of a ledger crashes after sending a write request to one bookie but
     * before being able to send it to the rest of the bookies in the quorum.
     *
     * <p>If the ledger is already closed, neither fencing nor recovery will be applied.
     *
     * @see LedgerHandle#asyncClose
     *
     * @param lId
     *          ledger identifier
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @param ctx
     *          optional control object
     */
    public void asyncOpenLedger(final long lId, final DigestType digestType, final byte[] passwd,
                                final OpenCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.openComplete(BKException.Code.ClientClosedException, null, ctx);
                return;
            }
            new LedgerOpenOp(BookKeeper.this, clientStats,
                             lId, digestType, passwd, cb, ctx).initiate();
        } finally {
            closeLock.readLock().unlock();
        }
    }

    /**
     * Open existing ledger asynchronously for reading, but it does not try to
     * recover the ledger if it is not yet closed. The application needs to use
     * it carefully, since the writer might have crashed and ledger will remain
     * unsealed forever if there is no external mechanism to detect the failure
     * of the writer and the ledger is not open in a safe manner, invoking the
     * recovery procedure.
     *
     * <p>Opening a ledger without recovery does not fence the ledger. As such, other
     * clients can continue to write to the ledger.
     *
     * <p>This method returns a read only ledger handle. It will not be possible
     * to add entries to the ledger. Any attempt to add entries will throw an
     * exception.
     *
     * <p>Reads from the returned ledger will be able to read entries up until
     * the lastConfirmedEntry at the point in time at which the ledger was opened.
     * If an attempt is made to read beyond the ledger handle's LAC, an attempt is made
     * to get the latest LAC from bookies or metadata, and if the entry_id of the read request
     * is less than or equal to the new LAC, read will be allowed to proceed.
     *
     * @param lId
     *          ledger identifier
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @param ctx
     *          optional control object
     */
    public void asyncOpenLedgerNoRecovery(final long lId, final DigestType digestType, final byte[] passwd,
                                          final OpenCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.openComplete(BKException.Code.ClientClosedException, null, ctx);
                return;
            }
            new LedgerOpenOp(BookKeeper.this, clientStats,
                             lId, digestType, passwd, cb, ctx).initiateWithoutRecovery();
        } finally {
            closeLock.readLock().unlock();
        }
    }


    /**
     * Synchronous open ledger call.
     *
     * @see #asyncOpenLedger
     * @param lId
     *          ledger identifier
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @return a handle to the open ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle openLedger(long lId, DigestType digestType, byte[] passwd)
            throws BKException, InterruptedException {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        SyncOpenCallback result = new SyncOpenCallback(future);

        /*
         * Calls async open ledger
         */
        asyncOpenLedger(lId, digestType, passwd, result, null);

        return SyncCallbackUtils.waitForResult(future);
    }

    /**
     * Synchronous, unsafe open ledger call.
     *
     * @see #asyncOpenLedgerNoRecovery
     * @param lId
     *          ledger identifier
     * @param digestType
     *          digest type, either MAC or CRC32
     * @param passwd
     *          password
     * @return a handle to the open ledger
     * @throws InterruptedException
     * @throws BKException
     */
    public LedgerHandle openLedgerNoRecovery(long lId, DigestType digestType, byte[] passwd)
            throws BKException, InterruptedException {
        CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        SyncOpenCallback result = new SyncOpenCallback(future);

        /*
         * Calls async open ledger
         */
        asyncOpenLedgerNoRecovery(lId, digestType, passwd,
                                  result, null);

        return SyncCallbackUtils.waitForResult(future);
    }

    /**
     * Deletes a ledger asynchronously.
     *
     * @param lId
     *            ledger Id
     * @param cb
     *            deleteCallback implementation
     * @param ctx
     *            optional control object
     */
    public void asyncDeleteLedger(final long lId, final DeleteCallback cb, final Object ctx) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                cb.deleteComplete(BKException.Code.ClientClosedException, ctx);
                return;
            }
            new LedgerDeleteOp(BookKeeper.this, clientStats, lId, cb, ctx).initiate();
        } finally {
            closeLock.readLock().unlock();
        }
    }


    /**
     * Synchronous call to delete a ledger. Parameters match those of
     * {@link #asyncDeleteLedger(long, AsyncCallback.DeleteCallback, Object)}
     *
     * @param lId
     *            ledgerId
     * @throws InterruptedException
     * @throws BKException
     */
    public void deleteLedger(long lId) throws InterruptedException, BKException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        SyncDeleteCallback result = new SyncDeleteCallback(future);
        // Call asynchronous version
        asyncDeleteLedger(lId, result, null);

        SyncCallbackUtils.waitForResult(future);
    }

    /**
     * Check asynchronously whether the ledger with identifier <i>lId</i>
     * has been closed.
     *
     * @param lId   ledger identifier
     * @param cb    callback method
     */
    public void asyncIsClosed(long lId, final IsClosedCallback cb, final Object ctx){
        ledgerManager.readLedgerMetadata(lId).whenComplete((metadata, exception) -> {
                if (exception == null) {
                    cb.isClosedComplete(BKException.Code.OK, metadata.getValue().isClosed(), ctx);
                } else {
                    cb.isClosedComplete(BKException.getExceptionCode(exception), false, ctx);
                }
            });
    }

    /**
     * Check whether the ledger with identifier <i>lId</i>
     * has been closed.
     *
     * @param lId
     * @return boolean true if ledger has been closed
     * @throws BKException
     */
    public boolean isClosed(long lId)
    throws BKException, InterruptedException {
        final class Result {
            int rc;
            boolean isClosed;
            final CountDownLatch notifier = new CountDownLatch(1);
        }

        final Result result = new Result();

        final IsClosedCallback cb = new IsClosedCallback(){
            @Override
            public void isClosedComplete(int rc, boolean isClosed, Object ctx){
                    result.isClosed = isClosed;
                    result.rc = rc;
                    result.notifier.countDown();
            }
        };

        /*
         * Call asynchronous version of isClosed
         */
        asyncIsClosed(lId, cb, null);

        /*
         * Wait for callback
         */
        result.notifier.await();

        if (result.rc != BKException.Code.OK) {
            throw BKException.create(result.rc);
        }

        return result.isClosed;
    }

    /**
     * Shuts down client.
     *
     */
    @Override
    public void close() throws BKException, InterruptedException {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }

        // Close bookie client so all pending bookie requests would be failed
        // which will reject any incoming bookie requests.
        bookieClient.close();
        try {
            // Close ledger manage so all pending metadata requests would be failed
            // which will reject any incoming metadata requests.
            ledgerManager.close();
            ledgerIdGenerator.close();
        } catch (IOException ie) {
            LOG.error("Failed to close ledger manager : ", ie);
        }

        // Close the scheduler
        scheduler.shutdown();
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.warn("The scheduler did not shutdown cleanly");
        }
        mainWorkerPool.shutdown();
        if (!mainWorkerPool.awaitTermination(10, TimeUnit.SECONDS)) {
            LOG.warn("The mainWorkerPool did not shutdown cleanly");
        }
        if (this.bookieInfoScheduler != null) {
            this.bookieInfoScheduler.shutdown();
            if (!bookieInfoScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.warn("The bookieInfoScheduler did not shutdown cleanly");
            }
        }

        if (ownTimer) {
            requestTimer.stop();
        }
        if (ownEventLoopGroup) {
            eventLoopGroup.shutdownGracefully();
        }
        this.metadataDriver.close();
    }

    @Override
    public CreateBuilder newCreateLedgerOp() {
        return new LedgerCreateOp.CreateBuilderImpl(this);
    }

    @Override
    public OpenBuilder newOpenLedgerOp() {
        return new LedgerOpenOp.OpenBuilderImpl(this);
    }

    @Override
    public DeleteBuilder newDeleteLedgerOp() {
        return new LedgerDeleteOp.DeleteBuilderImpl(this);
    }

    private final ClientContext clientCtx = new ClientContext() {
            @Override
            public ClientInternalConf getConf() {
                return internalConf;
            }

            @Override
            public LedgerManager getLedgerManager() {
                return BookKeeper.this.getLedgerManager();
            }

            @Override
            public BookieWatcher getBookieWatcher() {
                return BookKeeper.this.getBookieWatcher();
            }

            @Override
            public EnsemblePlacementPolicy getPlacementPolicy() {
                return BookKeeper.this.getPlacementPolicy();
            }

            @Override
            public BookieClient getBookieClient() {
                return BookKeeper.this.getBookieClient();
            }

            @Override
            public OrderedExecutor getMainWorkerPool() {
                return BookKeeper.this.getMainWorkerPool();
            }

            @Override
            public OrderedScheduler getScheduler() {
                return BookKeeper.this.getScheduler();
            }

            @Override
            public BookKeeperClientStats getClientStats() {
                return clientStats;
            }

            @Override
            public boolean isClientClosed() {
                return BookKeeper.this.isClosed();
            }

            @Override
            public ByteBufAllocator getByteBufAllocator() {
                return allocator;
            }
        };

    ClientContext getClientCtx() {
        return clientCtx;
    }
}
