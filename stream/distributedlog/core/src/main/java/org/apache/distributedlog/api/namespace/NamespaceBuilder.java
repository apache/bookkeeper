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
package org.apache.distributedlog.api.namespace;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.URI;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Stable;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.BKDistributedLogNamespace;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.DistributedLogConstants;
import org.apache.distributedlog.common.util.PermitLimiter;
import org.apache.distributedlog.config.DynamicDistributedLogConfiguration;
import org.apache.distributedlog.feature.CoreFeatureKeys;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.injector.AsyncRandomFailureInjector;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.namespace.NamespaceDriverManager;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.distributedlog.util.DLUtils;
import org.apache.distributedlog.util.SimplePermitLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder to construct a <code>Namespace</code>.
 * The builder takes the responsibility of loading backend according to the uri.
 *
 * @see Namespace
 * @since 0.3.32
 */
@Public
@Stable
public class NamespaceBuilder {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceBuilder.class);

    public static NamespaceBuilder newBuilder() {
        return new NamespaceBuilder();
    }

    private DistributedLogConfiguration _conf = null;
    private DynamicDistributedLogConfiguration _dynConf = null;
    private URI _uri = null;
    private StatsLogger _statsLogger = NullStatsLogger.INSTANCE;
    private StatsLogger _perLogStatsLogger = NullStatsLogger.INSTANCE;
    private FeatureProvider _featureProvider = null;
    private String _clientId = DistributedLogConstants.UNKNOWN_CLIENT_ID;
    private int _regionId = DistributedLogConstants.LOCAL_REGION_ID;

    // private constructor
    private NamespaceBuilder() {}

    /**
     * DistributedLog Configuration used for the namespace.
     *
     * @param conf
     *          distributedlog configuration
     * @return namespace builder.
     */
    public NamespaceBuilder conf(DistributedLogConfiguration conf) {
        this._conf = conf;
        return this;
    }

    /**
     * Dynamic DistributedLog Configuration used for the namespace.
     *
     * @param dynConf dynamic distributedlog configuration
     * @return namespace builder
     */
    public NamespaceBuilder dynConf(DynamicDistributedLogConfiguration dynConf) {
        this._dynConf = dynConf;
        return this;
    }

    /**
     * Namespace Location.
     *
     * @param uri
     *          namespace location uri.
     * @see Namespace
     * @return namespace builder.
     */
    public NamespaceBuilder uri(URI uri) {
        this._uri = uri;
        return this;
    }

    /**
     * Stats Logger used for stats collection.
     *
     * @param statsLogger
     *          stats logger
     * @return namespace builder.
     */
    public NamespaceBuilder statsLogger(StatsLogger statsLogger) {
        this._statsLogger = statsLogger;
        return this;
    }

    /**
     * Stats Logger used for collecting per log stats.
     *
     * @param statsLogger
     *          stats logger for collecting per log stats
     * @return namespace builder.
     */
    public NamespaceBuilder perLogStatsLogger(StatsLogger statsLogger) {
        this._perLogStatsLogger = statsLogger;
        return this;
    }

    /**
     * Feature provider used to control the availabilities of features in the namespace.
     *
     * @param featureProvider
     *          feature provider to control availabilities of features.
     * @return namespace builder.
     */
    public NamespaceBuilder featureProvider(FeatureProvider featureProvider) {
        this._featureProvider = featureProvider;
        return this;
    }

    /**
     * Client Id used for accessing the namespace.
     *
     * @param clientId
     *          client id used for accessing the namespace
     * @return namespace builder.
     */
    public NamespaceBuilder clientId(String clientId) {
        this._clientId = clientId;
        return this;
    }

    /**
     * Region Id used for encoding logs in the namespace. The region id
     * is useful when the namespace is globally spanning over regions.
     *
     * @param regionId
     *          region id.
     * @return namespace builder.
     */
    public NamespaceBuilder regionId(int regionId) {
        this._regionId = regionId;
        return this;
    }

    @SuppressWarnings("deprecation")
    private static StatsLogger normalizePerLogStatsLogger(StatsLogger statsLogger,
                                                          StatsLogger perLogStatsLogger,
                                                          DistributedLogConfiguration conf) {
        StatsLogger normalizedPerLogStatsLogger = perLogStatsLogger;
        if (perLogStatsLogger == NullStatsLogger.INSTANCE
                && conf.getEnablePerStreamStat()) {
            normalizedPerLogStatsLogger = statsLogger.scope("stream");
        }
        return normalizedPerLogStatsLogger;
    }

    /**
     * Build the namespace.
     *
     * @return the namespace instance.
     * @throws IllegalArgumentException when there is illegal argument provided in the builder
     * @throws NullPointerException when there is null argument provided in the builder
     * @throws IOException when fail to build the backend
     */
    public Namespace build()
            throws IllegalArgumentException, NullPointerException, IOException {
        // Check arguments
        checkNotNull(_conf, "No DistributedLog Configuration.");
        checkNotNull(_uri, "No DistributedLog URI");

        // validate the configuration
        _conf.validate();
        if (null == _dynConf) {
            _dynConf = ConfUtils.getConstDynConf(_conf);
        }

        // retrieve the namespace driver
        NamespaceDriver driver = NamespaceDriverManager.getDriver(_uri);
        URI normalizedUri = DLUtils.normalizeURI(_uri);

        // build the feature provider
        FeatureProvider featureProvider;
        if (null == _featureProvider) {
            featureProvider = new SettableFeatureProvider("", 0);
            logger.info("No feature provider is set. All features are disabled now.");
        } else {
            featureProvider = _featureProvider;
        }

        // build the failure injector
        AsyncFailureInjector failureInjector = AsyncRandomFailureInjector.newBuilder()
                .injectDelays(_conf.getEIInjectReadAheadDelay(),
                              _conf.getEIInjectReadAheadDelayPercent(),
                              _conf.getEIInjectMaxReadAheadDelayMs())
                .injectErrors(false, 10)
                .injectStops(_conf.getEIInjectReadAheadStall(), 10)
                .injectCorruption(_conf.getEIInjectReadAheadBrokenEntries())
                .build();

        // normalize the per log stats logger
        StatsLogger perLogStatsLogger = normalizePerLogStatsLogger(_statsLogger, _perLogStatsLogger, _conf);

        // build the scheduler
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("DLM-" + normalizedUri.getPath())
                .numThreads(_conf.getNumWorkerThreads())
                .build();

        // initialize the namespace driver
        driver.initialize(
                _conf,
                _dynConf,
                normalizedUri,
                scheduler,
                featureProvider,
                failureInjector,
                _statsLogger,
                perLogStatsLogger,
                DLUtils.normalizeClientId(_clientId),
                _regionId);

        // initialize the write limiter
        PermitLimiter writeLimiter;
        if (_conf.getGlobalOutstandingWriteLimit() < 0) {
            writeLimiter = PermitLimiter.NULL_PERMIT_LIMITER;
        } else {
            Feature disableWriteLimitFeature = featureProvider.getFeature(
                CoreFeatureKeys.DISABLE_WRITE_LIMIT.name().toLowerCase());
            writeLimiter = new SimplePermitLimiter(
                _conf.getOutstandingWriteLimitDarkmode(),
                _conf.getGlobalOutstandingWriteLimit(),
                _statsLogger.scope("writeLimiter"),
                true /* singleton */,
                disableWriteLimitFeature);
        }

        return new BKDistributedLogNamespace(
                _conf,
                normalizedUri,
                driver,
                scheduler,
                featureProvider,
                writeLimiter,
                failureInjector,
                _statsLogger,
                perLogStatsLogger,
                DLUtils.normalizeClientId(_clientId),
                _regionId);
    }
}
