/**
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
package org.apache.distributedlog.impl;

import static org.apache.distributedlog.util.DLUtils.isReservedStreamName;
import static org.apache.distributedlog.util.DLUtils.validateAndNormalizeName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.commons.lang.SystemUtils;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.BookKeeperClientBuilder;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.ZooKeeperClientBuilder;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.acl.DefaultAccessControlManager;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.bk.LedgerAllocator;
import org.apache.distributedlog.bk.LedgerAllocatorUtils;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.AlreadyClosedException;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.impl.acl.ZKAccessControlManager;
import org.apache.distributedlog.impl.federated.FederatedZKLogMetadataStore;
import org.apache.distributedlog.impl.logsegment.BKLogSegmentEntryStore;
import org.apache.distributedlog.impl.metadata.BKDLConfig;
import org.apache.distributedlog.impl.metadata.ZKLogStreamMetadataStore;
import org.apache.distributedlog.impl.subscription.ZKSubscriptionsStore;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.metadata.LogMetadataForReader;
import org.apache.distributedlog.metadata.LogMetadataStore;
import org.apache.distributedlog.metadata.LogStreamMetadataStore;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.namespace.NamespaceDriverManager;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for ZooKeeper/BookKeeper based namespace.
 */
public class BKNamespaceDriver implements NamespaceDriver {

    private static Logger LOG = LoggerFactory.getLogger(BKNamespaceDriver.class);

    // register itself
    static {
        NamespaceDriverManager.registerDriver(DistributedLogConstants.BACKEND_BK, BKNamespaceDriver.class);
    }

    /**
     * Extract zk servers fro dl <i>namespace</i>.
     *
     * @param uri dl namespace
     * @return zk servers
     */
    public static String getZKServersFromDLUri(URI uri) {
        return uri.getAuthority().replace(";", ",");
    }

    // resources (passed from initialization)
    private DistributedLogConfiguration conf;
    private DynamicDistributedLogConfiguration dynConf;
    private URI namespace;
    private OrderedScheduler scheduler;
    private FeatureProvider featureProvider;
    private AsyncFailureInjector failureInjector;
    private StatsLogger statsLogger;
    private StatsLogger perLogStatsLogger;
    private String clientId;
    private int regionId;

    //
    // resources (created internally and initialized at #initialize())
    //

    // namespace binding
    private BKDLConfig bkdlConfig;

    // zookeeper clients
    // NOTE: The actual zookeeper client is initialized lazily when it is referenced by
    //       {@link org.apache.distributedlog.ZooKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private ZooKeeperClientBuilder sharedWriterZKCBuilder;
    private ZooKeeperClient writerZKC;
    private ZooKeeperClientBuilder sharedReaderZKCBuilder;
    private ZooKeeperClient readerZKC;
    // NOTE: The actual bookkeeper client is initialized lazily when it is referenced by
    //       {@link org.apache.distributedlog.BookKeeperClient#get()}. So it is safe to
    //       keep builders and their client wrappers here, as they will be used when
    //       instantiating readers or writers.
    private EventLoopGroup eventLoopGroup;
    private HashedWheelTimer requestTimer;
    private BookKeeperClientBuilder sharedWriterBKCBuilder;
    private BookKeeperClient writerBKC;
    private BookKeeperClientBuilder sharedReaderBKCBuilder;
    private BookKeeperClient readerBKC;

    // log stream metadata store
    private LogMetadataStore metadataStore;
    private LogStreamMetadataStore writerStreamMetadataStore;
    private LogStreamMetadataStore readerStreamMetadataStore;

    //
    // resources (lazily initialized)
    //

    // ledger allocator
    private LedgerAllocator allocator;

    // log segment entry stores
    private LogSegmentEntryStore writerEntryStore;
    private LogSegmentEntryStore readerEntryStore;

    // access control manager
    private AccessControlManager accessControlManager;

    //
    // states
    //
    protected boolean initialized = false;
    protected AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Public constructor for reflection.
     */
    public BKNamespaceDriver() {
    }

    @Override
    public synchronized NamespaceDriver initialize(DistributedLogConfiguration conf,
                                                   DynamicDistributedLogConfiguration dynConf,
                                                   URI namespace,
                                                   OrderedScheduler scheduler,
                                                   FeatureProvider featureProvider,
                                                   AsyncFailureInjector failureInjector,
                                                   StatsLogger statsLogger,
                                                   StatsLogger perLogStatsLogger,
                                                   String clientId,
                                                   int regionId) throws IOException {
        if (initialized) {
            return this;
        }
        // validate the namespace
        if ((null == namespace) || (null == namespace.getAuthority()) || (null == namespace.getPath())) {
            throw new IOException("Incorrect distributedlog namespace : " + namespace);
        }

        // initialize the resources
        this.conf = conf;
        this.dynConf = dynConf;
        this.namespace = namespace;
        this.scheduler = scheduler;
        this.featureProvider = featureProvider;
        this.failureInjector = failureInjector;
        this.statsLogger = statsLogger;
        this.perLogStatsLogger = perLogStatsLogger;
        this.clientId = clientId;
        this.regionId = regionId;

        // initialize the zookeeper clients
        initializeZooKeeperClients();

        // initialize the bookkeeper clients
        initializeBookKeeperClients();

        // propagate bkdlConfig to configuration
        BKDLConfig.propagateConfiguration(bkdlConfig, conf);

        // initialize the log metadata & stream metadata store
        initializeLogStreamMetadataStores();

        // initialize other resources
        initializeOtherResources();

        initialized = true;

        LOG.info("Initialized BK namespace driver: clientId = {}, regionId = {}, federated = {}.",
            clientId, regionId, bkdlConfig.isFederatedNamespace());
        return this;
    }

    private void initializeZooKeeperClients() throws IOException {
        // Build the namespace zookeeper client
        this.sharedWriterZKCBuilder = createZKClientBuilder(
                String.format("dlzk:%s:factory_writer_shared", namespace),
                conf,
                getZKServersFromDLUri(namespace),
                statsLogger.scope("dlzk_factory_writer_shared"));
        this.writerZKC = sharedWriterZKCBuilder.build();

        // Resolve namespace binding
        this.bkdlConfig = BKDLConfig.resolveDLConfig(writerZKC, namespace);

        // Build zookeeper client for readers
        if (bkdlConfig.getDlZkServersForWriter().equals(bkdlConfig.getDlZkServersForReader())) {
            this.sharedReaderZKCBuilder = this.sharedWriterZKCBuilder;
        } else {
            this.sharedReaderZKCBuilder = createZKClientBuilder(
                    String.format("dlzk:%s:factory_reader_shared", namespace),
                    conf,
                    bkdlConfig.getDlZkServersForReader(),
                    statsLogger.scope("dlzk_factory_reader_shared"));
        }
        this.readerZKC = this.sharedReaderZKCBuilder.build();
    }

    private synchronized BKDLConfig getBkdlConfig() {
        return bkdlConfig;
    }

    static EventLoopGroup getDefaultEventLoopGroup(int numThreads) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("DL-io-%s").build();
        if (SystemUtils.IS_OS_LINUX) {
            try {
                return new EpollEventLoopGroup(numThreads, threadFactory);
            } catch (Throwable t) {
                LOG.warn("Could not use Netty Epoll event loop for bookie server:", t);
                return new NioEventLoopGroup(numThreads, threadFactory);
            }
        } else {
            return new NioEventLoopGroup(numThreads, threadFactory);
        }
    }

    private void initializeBookKeeperClients() throws IOException {
        this.eventLoopGroup = getDefaultEventLoopGroup(conf.getBKClientNumberIOThreads());
        this.requestTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("DLFactoryTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());
        // Build bookkeeper client for writers
        this.sharedWriterBKCBuilder = createBKCBuilder(
                String.format("bk:%s:factory_writer_shared", namespace),
                conf,
                bkdlConfig.getBkZkServersForWriter(),
                bkdlConfig.getBkLedgersPath(),
                eventLoopGroup,
                requestTimer,
                Optional.of(featureProvider.scope("bkc")),
                statsLogger);
        this.writerBKC = this.sharedWriterBKCBuilder.build();

        // Build bookkeeper client for readers
        if (bkdlConfig.getBkZkServersForWriter().equals(bkdlConfig.getBkZkServersForReader())) {
            this.sharedReaderBKCBuilder = this.sharedWriterBKCBuilder;
        } else {
            this.sharedReaderBKCBuilder = createBKCBuilder(
                    String.format("bk:%s:factory_reader_shared", namespace),
                    conf,
                    bkdlConfig.getBkZkServersForReader(),
                    bkdlConfig.getBkLedgersPath(),
                    eventLoopGroup,
                    requestTimer,
                    Optional.<FeatureProvider>empty(),
                    statsLogger);
        }
        this.readerBKC = this.sharedReaderBKCBuilder.build();
    }

    private void initializeLogStreamMetadataStores() throws IOException {
        // log metadata store
        if (bkdlConfig.isFederatedNamespace() || conf.isFederatedNamespaceEnabled()) {
            this.metadataStore = new FederatedZKLogMetadataStore(conf, namespace, readerZKC, scheduler);
        } else {
            this.metadataStore = new ZKLogMetadataStore(conf, namespace, readerZKC, scheduler);
        }

        // create log stream metadata store
        this.writerStreamMetadataStore =
                new ZKLogStreamMetadataStore(
                        clientId,
                        conf,
                        writerZKC,
                        scheduler,
                        statsLogger);
        this.readerStreamMetadataStore =
                new ZKLogStreamMetadataStore(
                        clientId,
                        conf,
                        readerZKC,
                        scheduler,
                        statsLogger);
    }

    @VisibleForTesting
    public static String
    validateAndGetFullLedgerAllocatorPoolPath(DistributedLogConfiguration conf, URI uri) throws IOException {
        String poolPath = conf.getLedgerAllocatorPoolPath();
        LOG.info("PoolPath is {}", poolPath);
        if (null == poolPath || !poolPath.startsWith(".") || poolPath.endsWith("/")) {
            LOG.error("Invalid ledger allocator pool path specified when enabling ledger allocator pool: {}", poolPath);
            throw new IOException("Invalid ledger allocator pool path specified : " + poolPath);
        }
        String poolName = conf.getLedgerAllocatorPoolName();
        if (null == poolName) {
            LOG.error("No ledger allocator pool name specified when enabling ledger allocator pool.");
            throw new IOException("No ledger allocator name specified when enabling ledger allocator pool.");
        }
        String rootPath = uri.getPath() + "/" + poolPath + "/" + poolName;
        try {
            PathUtils.validatePath(rootPath);
        } catch (IllegalArgumentException iae) {
            LOG.error("Invalid ledger allocator pool path specified when enabling ledger allocator pool: {}", poolPath);
            throw new IOException("Invalid ledger allocator pool path specified : " + poolPath);
        }
        return rootPath;
    }

    private void initializeOtherResources() throws IOException {
        // Ledger allocator
        if (conf.getEnableLedgerAllocatorPool()) {
            String allocatorPoolPath = validateAndGetFullLedgerAllocatorPoolPath(conf, namespace);
            allocator = LedgerAllocatorUtils.createLedgerAllocatorPool(
                    allocatorPoolPath,
                    conf.getLedgerAllocatorPoolCoreSize(),
                    conf,
                    writerZKC,
                    writerBKC,
                    scheduler);
            if (null != allocator) {
                allocator.start();
            }
            LOG.info("Created ledger allocator pool under {} with size {}.",
                    allocatorPoolPath, conf.getLedgerAllocatorPoolCoreSize());
        } else {
            allocator = null;
        }

    }

    private void checkState() throws IOException {
        if (closed.get()) {
            LOG.error("BK namespace driver {} is already closed", namespace);
            throw new AlreadyClosedException("BK namespace driver " + namespace + " is already closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        doClose();
    }

    private void doClose() {
        if (null != accessControlManager) {
            accessControlManager.close();
            LOG.info("Access Control Manager Stopped.");
        }

        // Close the allocator
        if (null != allocator) {
            Utils.closeQuietly(allocator);
            LOG.info("Ledger Allocator stopped.");
        }

        // Shutdown log segment metadata stores
        Utils.close(writerStreamMetadataStore);
        Utils.close(readerStreamMetadataStore);

        writerBKC.close();
        readerBKC.close();
        writerZKC.close();
        readerZKC.close();
        // release bookkeeper resources
        eventLoopGroup.shutdownGracefully();
        LOG.info("Release external resources used by channel factory.");
        requestTimer.stop();
        LOG.info("Stopped request timer");
    }

    @Override
    public URI getUri() {
        return namespace;
    }

    @Override
    public String getScheme() {
        return DistributedLogConstants.BACKEND_BK;
    }

    @Override
    public LogMetadataStore getLogMetadataStore() {
        return metadataStore;
    }

    @Override
    public LogStreamMetadataStore getLogStreamMetadataStore(Role role) {
        if (Role.WRITER == role) {
            return writerStreamMetadataStore;
        } else {
            return readerStreamMetadataStore;
        }
    }

    @Override
    public LogSegmentEntryStore getLogSegmentEntryStore(Role role) {
        if (Role.WRITER == role) {
            return getWriterEntryStore();
        } else {
            return getReaderEntryStore();
        }
    }

    private LogSegmentEntryStore getWriterEntryStore() {
        if (null == writerEntryStore) {
            writerEntryStore = new BKLogSegmentEntryStore(
                    conf,
                    dynConf,
                    writerZKC,
                    writerBKC,
                    scheduler,
                    allocator,
                    statsLogger,
                    failureInjector);
        }
        return writerEntryStore;
    }

    private LogSegmentEntryStore getReaderEntryStore() {
        if (null == readerEntryStore) {
            readerEntryStore = new BKLogSegmentEntryStore(
                    conf,
                    dynConf,
                    writerZKC,
                    readerBKC,
                    scheduler,
                    allocator,
                    statsLogger,
                    failureInjector);
        }
        return readerEntryStore;
    }

    @Override
    public AccessControlManager getAccessControlManager() throws IOException {
        if (null == accessControlManager) {
            String aclRootPath = getBkdlConfig().getACLRootPath();
            // Build the access control manager
            if (aclRootPath == null) {
                accessControlManager = DefaultAccessControlManager.INSTANCE;
                LOG.info("Created default access control manager for {}", namespace);
            } else {
                if (!isReservedStreamName(aclRootPath)) {
                    throw new IOException("Invalid Access Control List Root Path : " + aclRootPath);
                }
                String zkRootPath = namespace.getPath() + "/" + aclRootPath;
                LOG.info("Creating zk based access control manager @ {} for {}",
                        zkRootPath, namespace);
                accessControlManager = new ZKAccessControlManager(conf, readerZKC,
                        zkRootPath, scheduler);
                LOG.info("Created zk based access control manager @ {} for {}",
                        zkRootPath, namespace);
            }
        }
        return accessControlManager;
    }

    @Override
    public SubscriptionsStore getSubscriptionsStore(String streamName) {
        return new ZKSubscriptionsStore(
                writerZKC,
                LogMetadataForReader.getSubscribersPath(namespace, streamName, conf.getUnpartitionedStreamName()));
    }

    //
    // Legacy Intefaces
    //

    @SuppressWarnings("deprecation")
    @Override
    public org.apache.distributedlog.api.MetadataAccessor getMetadataAccessor(String streamName)
            throws InvalidStreamNameException, IOException {
        if (getBkdlConfig().isFederatedNamespace()) {
            throw new UnsupportedOperationException();
        }
        checkState();
        streamName = validateAndNormalizeName(streamName);
        return new ZKMetadataAccessor(
                streamName,
                conf,
                namespace,
                sharedWriterZKCBuilder,
                sharedReaderZKCBuilder,
                statsLogger);
    }

    public Map<String, byte[]> enumerateLogsWithMetadataInNamespace()
        throws IOException, IllegalArgumentException {
        String namespaceRootPath = namespace.getPath();
        HashMap<String, byte[]> result = new HashMap<String, byte[]>();
        ZooKeeperClient zkc = writerZKC;
        try {
            ZooKeeper zk = Utils.sync(zkc, namespaceRootPath);
            Stat currentStat = zk.exists(namespaceRootPath, false);
            if (currentStat == null) {
                return result;
            }
            List<String> children = zk.getChildren(namespaceRootPath, false);
            for (String child: children) {
                if (isReservedStreamName(child)) {
                    continue;
                }
                String zkPath = String.format("%s/%s", namespaceRootPath, child);
                currentStat = zk.exists(zkPath, false);
                if (currentStat == null) {
                    result.put(child, new byte[0]);
                } else {
                    result.put(child, zk.getData(zkPath, false, currentStat));
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while reading " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        }
        return result;
    }

    //
    // Zk & Bk Utils
    //

    public static ZooKeeperClientBuilder createZKClientBuilder(String zkcName,
                                                               DistributedLogConfiguration conf,
                                                               String zkServers,
                                                               StatsLogger statsLogger) {
        RetryPolicy retryPolicy = null;
        if (conf.getZKNumRetries() > 0) {
            retryPolicy = new BoundExponentialBackoffRetryPolicy(
                conf.getZKRetryBackoffStartMillis(),
                conf.getZKRetryBackoffMaxMillis(), conf.getZKNumRetries());
        }
        ZooKeeperClientBuilder builder = ZooKeeperClientBuilder.newBuilder()
            .name(zkcName)
            .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
            .retryThreadCount(conf.getZKClientNumberRetryThreads())
            .requestRateLimit(conf.getZKRequestRateLimit())
            .zkServers(zkServers)
            .retryPolicy(retryPolicy)
            .statsLogger(statsLogger)
            .zkAclId(conf.getZkAclId());
        LOG.info("Created shared zooKeeper client builder {}: zkServers = {}, numRetries = {}, sessionTimeout = {},"
                + " retryBackoff = {}, maxRetryBackoff = {}, zkAclId = {}.", zkcName, zkServers,
            conf.getZKNumRetries(), conf.getZKSessionTimeoutMilliseconds(),
            conf.getZKRetryBackoffStartMillis(), conf.getZKRetryBackoffMaxMillis(),
            conf.getZkAclId());
        return builder;
    }

    private BookKeeperClientBuilder createBKCBuilder(String bkcName,
                                                     DistributedLogConfiguration conf,
                                                     String zkServers,
                                                     String ledgersPath,
                                                     EventLoopGroup eventLoopGroup,
                                                     HashedWheelTimer requestTimer,
                                                     Optional<FeatureProvider> featureProviderOptional,
                                                     StatsLogger statsLogger) {
        BookKeeperClientBuilder builder = BookKeeperClientBuilder.newBuilder()
                .name(bkcName)
                .dlConfig(conf)
                .zkServers(zkServers)
                .ledgersPath(ledgersPath)
                .eventLoopGroup(eventLoopGroup)
                .requestTimer(requestTimer)
                .featureProvider(featureProviderOptional)
                .statsLogger(statsLogger);
        LOG.info("Created shared client builder {} : zkServers = {}, ledgersPath = {}, numIOThreads = {}",
            bkcName, zkServers, ledgersPath, conf.getBKClientNumberIOThreads());
        return builder;
    }

    //
    // Test Methods
    //

    @VisibleForTesting
    public ZooKeeperClient getWriterZKC() {
        return writerZKC;
    }

    @VisibleForTesting
    public BookKeeperClient getReaderBKC() {
        return readerBKC;
    }

    @VisibleForTesting
    public AsyncFailureInjector getFailureInjector() {
        return this.failureInjector;
    }

    @VisibleForTesting
    public LogStreamMetadataStore getWriterStreamMetadataStore() {
        return writerStreamMetadataStore;
    }

    @VisibleForTesting
    public LedgerAllocator getLedgerAllocator() {
        return allocator;
    }
}
