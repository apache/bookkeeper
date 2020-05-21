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
package org.apache.distributedlog;

import static org.apache.distributedlog.namespace.NamespaceDriver.Role.WRITER;
import static org.apache.distributedlog.util.DLUtils.validateAndNormalizeName;

import com.google.common.base.Ticker;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.acl.AccessControlManager;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.common.util.PermitLimiter;
import org.apache.distributedlog.common.util.SchedulerUtils;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.exceptions.AlreadyClosedException;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LogNotFoundException;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.logsegment.LogSegmentMetadataCache;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BKDistributedLogNamespace is the default implementation of {@link Namespace}. It uses
 * zookeeper for metadata storage and bookkeeper for data storage.
 * <h3>Metrics</h3>
 *
 * <h4>ZooKeeper Client</h4>
 * See {@link ZooKeeperClient} for detail sub-stats.
 * <ul>
 * <li> `scope`/dlzk_factory_writer_shared/* : stats about the zookeeper client shared by all DL writers.
 * <li> `scope`/dlzk_factory_reader_shared/* : stats about the zookeeper client shared by all DL readers.
 * <li> `scope`/bkzk_factory_writer_shared/* : stats about the zookeeper client used by bookkeeper client
 * shared by all DL writers.
 * <li> `scope`/bkzk_factory_reader_shared/* : stats about the zookeeper client used by bookkeeper client
 * shared by all DL readers.
 * </ul>
 *
 * <h4>BookKeeper Client</h4>
 * BookKeeper client stats are exposed directly to current scope. See {@link BookKeeperClient} for detail stats.
 *
 * <h4>Utils</h4>
 * <ul>
 * <li> `scope`/factory/thread_pool/* : stats about the ordered scheduler used by this namespace.
 * See {@link OrderedScheduler}.
 * <li> `scope`/writeLimiter/* : stats about the global write limiter used by this namespace.
 * See {@link PermitLimiter}.
 * </ul>
 *
 * <h4>DistributedLogManager</h4>
 * All the core stats about reader and writer are exposed under current scope via {@link BKDistributedLogManager}.
 */
public class BKDistributedLogNamespace implements Namespace {
    static final Logger LOG = LoggerFactory.getLogger(BKDistributedLogNamespace.class);

    private final String clientId;
    private final int regionId;
    private final DistributedLogConfiguration conf;
    private final URI namespace;
    // namespace driver
    private final NamespaceDriver driver;
    // resources
    private final OrderedScheduler scheduler;
    private final PermitLimiter writeLimiter;
    private final AsyncFailureInjector failureInjector;
    // log segment metadata store
    private final LogSegmentMetadataCache logSegmentMetadataCache;
    // feature provider
    private final FeatureProvider featureProvider;
    // Stats Loggers
    private final StatsLogger statsLogger;
    private final StatsLogger perLogStatsLogger;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public BKDistributedLogNamespace(
            DistributedLogConfiguration conf,
            URI uri,
            NamespaceDriver driver,
            OrderedScheduler scheduler,
            FeatureProvider featureProvider,
            PermitLimiter writeLimiter,
            AsyncFailureInjector failureInjector,
            StatsLogger statsLogger,
            StatsLogger perLogStatsLogger,
            String clientId,
            int regionId) {
        this.conf = conf;
        this.namespace = uri;
        this.driver = driver;
        this.scheduler = scheduler;
        this.featureProvider = featureProvider;
        this.writeLimiter = writeLimiter;
        this.failureInjector = failureInjector;
        this.statsLogger = statsLogger;
        this.perLogStatsLogger = perLogStatsLogger;
        this.clientId = clientId;
        this.regionId = regionId;

        // create a log segment metadata cache
        this.logSegmentMetadataCache = new LogSegmentMetadataCache(conf, Ticker.systemTicker());
    }

    @Override
    public NamespaceDriver getNamespaceDriver() {
        return driver;
    }

    //
    // Namespace Methods
    //

    @Override
    public void createLog(String logName)
            throws InvalidStreamNameException, IOException {
        checkState();
        logName = validateAndNormalizeName(logName);
        URI uri = Utils.ioResult(driver.getLogMetadataStore().createLog(logName));
        Utils.ioResult(driver.getLogStreamMetadataStore(WRITER).getLog(uri, logName, true, true));
    }

    @Override
    public void deleteLog(String logName)
            throws InvalidStreamNameException, LogNotFoundException, IOException {
        checkState();
        logName = validateAndNormalizeName(logName);
        Optional<URI> uri = Utils.ioResult(driver.getLogMetadataStore().getLogLocation(logName));
        if (!uri.isPresent()) {
            throw new LogNotFoundException("Log " + logName + " isn't found.");
        }
        DistributedLogManager dlm = openLogInternal(
                uri.get(),
                logName,
                Optional.empty(),
                Optional.empty());
        dlm.delete();
    }

    @Override
    public DistributedLogManager openLog(String logName)
            throws InvalidStreamNameException, IOException {
        return openLog(logName,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public DistributedLogManager openLog(String logName,
                                         Optional<DistributedLogConfiguration> logConf,
                                         Optional<DynamicDistributedLogConfiguration> dynamicLogConf,
                                         Optional<StatsLogger> perStreamStatsLogger)
            throws InvalidStreamNameException, IOException {
        checkState();
        logName = validateAndNormalizeName(logName);
        Optional<URI> uri = Utils.ioResult(driver.getLogMetadataStore().getLogLocation(logName));
        if (!uri.isPresent()) {
            throw new LogNotFoundException("Log " + logName + " isn't found.");
        }
        return openLogInternal(
                uri.get(),
                logName,
                logConf,
                dynamicLogConf);
    }

    @Override
    public CompletableFuture<Void> renameLog(String oldName, String newName) {
        try {
            checkState();
            final String oldLogName = validateAndNormalizeName(oldName);
            final String newLogName = validateAndNormalizeName(newName);

            return driver.getLogMetadataStore().getLogLocation(oldName)
                .thenCompose(uriOptional -> {
                    if (uriOptional.isPresent()) {
                        return driver.getLogStreamMetadataStore(WRITER)
                            .renameLog(uriOptional.get(), oldLogName, newLogName);
                    } else {
                        return FutureUtils.exception(
                            new LogNotFoundException("Log " + oldLogName + " isn't found."));
                    }
                });
        } catch (IOException ioe) {
            return FutureUtils.exception(ioe);
        }
    }

    @Override
    public boolean logExists(String logName)
        throws IOException, IllegalArgumentException {
        checkState();
        Optional<URI> uri = Utils.ioResult(driver.getLogMetadataStore().getLogLocation(logName));
        if (uri.isPresent()) {
            try {
                Utils.ioResult(driver.getLogStreamMetadataStore(WRITER)
                        .logExists(uri.get(), logName));
                return true;
            } catch (LogNotFoundException lnfe) {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public Iterator<String> getLogs() throws IOException {
        checkState();
        return Utils.ioResult(driver.getLogMetadataStore().getLogs(""));
    }

    @Override
    public Iterator<String> getLogs(String logNamePrefix) throws IOException {
        checkState();
        logNamePrefix = validateAndNormalizeName(logNamePrefix);
        return Utils.ioResult(driver.getLogMetadataStore().getLogs(logNamePrefix));
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        driver.getLogMetadataStore().registerNamespaceListener(listener);
    }

    @Override
    public synchronized AccessControlManager createAccessControlManager() throws IOException {
        checkState();
        return driver.getAccessControlManager();
    }

    /**
     * Open the log in location <i>uri</i>.
     *
     * @param uri
     *          location to store the log
     * @param nameOfLogStream
     *          name of the log
     * @param logConfiguration
     *          optional stream configuration
     * @param dynamicLogConfiguration
     *          dynamic stream configuration overrides.
     * @return distributedlog manager instance.
     * @throws InvalidStreamNameException if the stream name is invalid
     * @throws IOException
     */
    protected DistributedLogManager openLogInternal(
            URI uri,
            String nameOfLogStream,
            Optional<DistributedLogConfiguration> logConfiguration,
            Optional<DynamicDistributedLogConfiguration> dynamicLogConfiguration)
        throws InvalidStreamNameException, IOException {
        // Make sure the name is well formed
        checkState();
        nameOfLogStream = validateAndNormalizeName(nameOfLogStream);

        DistributedLogConfiguration mergedConfiguration = new DistributedLogConfiguration();
        mergedConfiguration.addConfiguration(conf);
        mergedConfiguration.loadStreamConf(logConfiguration);
        // If dynamic config was not provided, default to a static view of the global configuration.
        DynamicDistributedLogConfiguration dynConf = null;
        if (dynamicLogConfiguration.isPresent()) {
            dynConf = dynamicLogConfiguration.get();
        } else {
            dynConf = ConfUtils.getConstDynConf(mergedConfiguration);
        }

        return new BKDistributedLogManager(
                nameOfLogStream,                    /* Log Name */
                mergedConfiguration,                /* Configuration */
                dynConf,                            /* Dynamic Configuration */
                uri,                                /* Namespace URI */
                driver,                             /* Namespace Driver */
                logSegmentMetadataCache,            /* Log Segment Metadata Cache */
                scheduler,                          /* DL scheduler */
                clientId,                           /* Client Id */
                regionId,                           /* Region Id */
                writeLimiter,                       /* Write Limiter */
                featureProvider.scope("dl"),        /* Feature Provider */
                failureInjector,                    /* Failure Injector */
                statsLogger,                        /* Stats Logger */
                perLogStatsLogger,                  /* Per Log Stats Logger */
                Optional.empty()                    /* shared resources, we don't need to close any resources in dlm */
        );
    }

    /**
     * Check the namespace state.
     *
     * @throws IOException
     */
    private void checkState() throws IOException {
        if (closed.get()) {
            LOG.error("BK namespace {} is already closed", namespace);
            throw new AlreadyClosedException("BK namespace " + namespace + " is already closed");
        }
    }

    /**
     * Close the distributed log manager factory, freeing any resources it may hold.
     * close the resource in reverse order v.s. in which they are started
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // close the write limiter
        this.writeLimiter.close();
        // shutdown the driver
        Utils.close(driver);
        // Shutdown the schedulers
        SchedulerUtils.shutdownScheduler(scheduler, conf.getSchedulerShutdownTimeoutMs(),
                TimeUnit.MILLISECONDS);
        LOG.info("Executor Service Stopped.");
    }
}
