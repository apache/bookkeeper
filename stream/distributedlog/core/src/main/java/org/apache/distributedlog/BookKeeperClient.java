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
package org.apache.distributedlog;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.RetryPolicy;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.distributedlog.ZooKeeperClient.Credentials;
import org.apache.distributedlog.ZooKeeperClient.DigestCredentials;
import org.apache.distributedlog.bk.LedgerMetadata;
import org.apache.distributedlog.exceptions.AlreadyClosedException;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.net.NetUtils;
import org.apache.distributedlog.util.ConfUtils;

/**
 * BookKeeper Client wrapper over {@link BookKeeper}.
 *
 * <h3>Metrics</h3>
 * <ul>
 * <li> bookkeeper operation stats are exposed under current scope by {@link BookKeeper}
 * </ul>
 */
@CustomLog
public class BookKeeperClient {

    // Parameters to build bookkeeper client
    private final DistributedLogConfiguration conf;
    private final String name;
    private final String zkServers;
    private final String ledgersPath;
    private final byte[] passwd;
    private final EventLoopGroup eventLoopGroup;
    private final HashedWheelTimer requestTimer;
    private final StatsLogger statsLogger;

    // bookkeeper client state
    private boolean closed = false;
    private BookKeeper bkc = null;
    private ZooKeeperClient zkc;
    private final boolean ownZK;
    // feature provider
    private final Optional<FeatureProvider> featureProvider;

    @SuppressWarnings("deprecation")
    private synchronized void commonInitialization(
            DistributedLogConfiguration conf,
            String ledgersPath,
            EventLoopGroup eventLoopGroup,
            StatsLogger statsLogger, HashedWheelTimer requestTimer)
        throws IOException, InterruptedException {
        ClientConfiguration bkConfig = new ClientConfiguration();
        bkConfig.setAddEntryTimeout(conf.getBKClientWriteTimeout());
        bkConfig.setReadTimeout(conf.getBKClientReadTimeout());
        bkConfig.setZkLedgersRootPath(ledgersPath);
        bkConfig.setZkTimeout(conf.getBKClientZKSessionTimeoutMilliSeconds());
        bkConfig.setNumWorkerThreads(conf.getBKClientNumberWorkerThreads());
        bkConfig.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);
        bkConfig.setZkRequestRateLimit(conf.getBKClientZKRequestRateLimit());
        bkConfig.setProperty(RegionAwareEnsemblePlacementPolicy.REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME,
                DistributedLogConstants.DISALLOW_PLACEMENT_IN_REGION_FEATURE_NAME);
        // reload configuration from dl configuration with settings prefixed with 'bkc.'
        ConfUtils.loadConfiguration(bkConfig, conf, "bkc.");

        Class<? extends DNSToSwitchMapping> dnsResolverCls;
        try {
            dnsResolverCls = conf.getEnsemblePlacementDnsResolverClass();
        } catch (ConfigurationException e) {
            log.error().exception(e).log("Failed to load bk dns resolver");
            throw new IOException("Failed to load bk dns resolver : ", e);
        }
        final DNSToSwitchMapping dnsResolver =
                NetUtils.getDNSResolver(dnsResolverCls, conf.getBkDNSResolverOverrides());

        try {
            this.bkc = BookKeeper.forConfig(bkConfig)
                .setZookeeper(zkc.get())
                .setEventLoopGroup(eventLoopGroup)
                .setStatsLogger(statsLogger)
                .dnsResolver(dnsResolver)
                .requestTimer(requestTimer)
                .featureProvider(featureProvider.orElse(null))
                .build();
        } catch (BKException bke) {
            throw new IOException(bke);
        }
    }

    BookKeeperClient(DistributedLogConfiguration conf,
                     String name,
                     String zkServers,
                     ZooKeeperClient zkc,
                     String ledgersPath,
                     EventLoopGroup eventLoopGroup,
                     HashedWheelTimer requestTimer,
                     StatsLogger statsLogger,
                     Optional<FeatureProvider> featureProvider) {
        this.conf = conf;
        this.name = name;
        this.zkServers = zkServers;
        this.ledgersPath = ledgersPath;
        this.passwd = conf.getBKDigestPW().getBytes(UTF_8);
        this.eventLoopGroup = eventLoopGroup;
        this.requestTimer = requestTimer;
        this.statsLogger = statsLogger;
        this.featureProvider = featureProvider;
        this.ownZK = null == zkc;
        if (null != zkc) {
            // reference the passing zookeeper client
            this.zkc = zkc;
        }
    }

    private synchronized void initialize() throws IOException {
        if (null != this.bkc) {
            return;
        }
        if (null == this.zkc) {
            int zkSessionTimeout = conf.getBKClientZKSessionTimeoutMilliSeconds();
            RetryPolicy retryPolicy = new BoundExponentialBackoffRetryPolicy(
                        conf.getBKClientZKRetryBackoffStartMillis(),
                        conf.getBKClientZKRetryBackoffMaxMillis(), conf.getBKClientZKNumRetries());
            Credentials credentials = Credentials.NONE;
            if (conf.getZkAclId() != null) {
                credentials = new DigestCredentials(conf.getZkAclId(), conf.getZkAclId());
            }

            this.zkc = new ZooKeeperClient(name + ":zk", zkSessionTimeout, 2 * zkSessionTimeout, zkServers,
                                           retryPolicy, statsLogger.scope("bkc_zkc"),
                    conf.getZKClientNumberRetryThreads(), conf.getBKClientZKRequestRateLimit(), credentials);
        }

        try {
            commonInitialization(conf, ledgersPath, eventLoopGroup, statsLogger, requestTimer);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DLInterruptedException("Interrupted on creating bookkeeper client " + name + " : ", e);
        }

        if (ownZK) {
            log.info()
                    .attr("name", name)
                    .attr("ledgersPath", ledgersPath)
                    .attr("bKClientZKNumRetries", conf.getBKClientZKNumRetries())
                    .attr("bKClientZKSessionTimeoutMilliSeconds", conf.getBKClientZKSessionTimeoutMilliSeconds())
                    .attr("bKClientZKRetryBackoffStartMillis", conf.getBKClientZKRetryBackoffStartMillis())
                    .attr("bKClientZKRetryBackoffMaxMillis", conf.getBKClientZKRetryBackoffMaxMillis())
                    .attr("bkDNSResolverOverrides", conf.getBkDNSResolverOverrides())
                    .log("BookKeeper Client created with its own ZK Client");
        } else {
            log.info()
                    .attr("name", name)
                    .attr("ledgersPath", ledgersPath)
                    .attr("zKNumRetries", conf.getZKNumRetries())
                    .attr("zKSessionTimeoutMilliseconds", conf.getZKSessionTimeoutMilliseconds())
                    .attr("zKRetryBackoffStartMillis", conf.getZKRetryBackoffStartMillis())
                    .attr("zKRetryBackoffMaxMillis", conf.getZKRetryBackoffMaxMillis())
                    .attr("bkDNSResolverOverrides", conf.getBkDNSResolverOverrides())
                    .log("BookKeeper Client created with shared zookeeper client");
        }
    }

    public synchronized BookKeeper get() throws IOException {
        checkClosedOrInError();
        if (null == bkc) {
            initialize();
        }
        return bkc;
    }

    // Util functions
    public CompletableFuture<LedgerHandle> createLedger(int ensembleSize,
                                                        int writeQuorumSize,
                                                        int ackQuorumSize,
                                                        LedgerMetadata ledgerMetadata) {
        BookKeeper bk;
        try {
            bk = get();
        } catch (IOException ioe) {
            return FutureUtils.exception(ioe);
        }
        final CompletableFuture<LedgerHandle> promise = new CompletableFuture<LedgerHandle>();
        bk.asyncCreateLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                BookKeeper.DigestType.CRC32, passwd, new AsyncCallback.CreateCallback() {
                    @Override
                    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                        if (BKException.Code.OK == rc) {
                            promise.complete(lh);
                        } else {
                            promise.completeExceptionally(BKException.create(rc));
                        }
                    }
                }, null, ledgerMetadata == null ? Collections.emptyMap() : ledgerMetadata.getMetadata());
        return promise;
    }

    public CompletableFuture<Void> deleteLedger(long lid,
                                     final boolean ignoreNonExistentLedger) {
        BookKeeper bk;
        try {
            bk = get();
        } catch (IOException ioe) {
            return FutureUtils.exception(ioe);
        }
        final CompletableFuture<Void> promise = new CompletableFuture<Void>();
        bk.asyncDeleteLedger(lid, new AsyncCallback.DeleteCallback() {
            @Override
            public void deleteComplete(int rc, Object ctx) {
                if (BKException.Code.OK == rc) {
                    promise.complete(null);
                } else if (Code.NoSuchLedgerExistsOnMetadataServerException == rc) {
                    if (ignoreNonExistentLedger) {
                        promise.complete(null);
                    } else {
                        promise.completeExceptionally(BKException.create(rc));
                    }
                } else {
                    promise.completeExceptionally(BKException.create(rc));
                }
            }
        }, null);
        return promise;
    }

    public void close() {
        BookKeeper bkcToClose;
        ZooKeeperClient zkcToClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            bkcToClose = bkc;
            zkcToClose = zkc;
        }

        log.info().attr("name", name).log("BookKeeper Client closed");
        if (null != bkcToClose) {
            try {
                bkcToClose.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn().attr("name", name).exception(e).log("Interrupted on closing bookkeeper client");
                Thread.currentThread().interrupt();
            } catch (BKException e) {
                log.warn().attr("name", name).exception(e).log("Error on closing bookkeeper client");
            }
        }
        if (null != zkcToClose) {
            if (ownZK) {
                zkcToClose.close();
            }
        }
    }

    public synchronized void checkClosedOrInError() throws AlreadyClosedException {
        if (closed) {
            log.error().attr("name", name).log("BookKeeper Client is already closed");
            throw new AlreadyClosedException("BookKeeper Client " + name + " is already closed");
        }
    }
}
