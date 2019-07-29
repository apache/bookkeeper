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

import static org.junit.Assert.assertTrue;

import com.google.common.base.Optional;
import com.google.common.base.Ticker;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.common.util.PermitLimiter;
import org.apache.distributedlog.common.util.SchedulerUtils;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.impl.logsegment.BKLogSegmentEntryWriter;
import org.apache.distributedlog.injector.AsyncFailureInjector;
import org.apache.distributedlog.injector.AsyncRandomFailureInjector;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.distributedlog.logsegment.LogSegmentEntryWriter;
import org.apache.distributedlog.logsegment.LogSegmentMetadataCache;
import org.apache.distributedlog.logsegment.LogSegmentMetadataStore;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DistributedLogBase providing test environment setup for other Test Cases.
 */
public class TestDistributedLogBase {
    static final Logger LOG = LoggerFactory.getLogger(TestDistributedLogBase.class);

    static {
        // org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from 3.5.3 four letter words
        // are disabled by default due to security reasons
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }

    // Num worker threads should be one, since the exec service is used for the ordered
    // future pool in test cases, and setting to > 1 will therefore result in unordered
    // write ops.
    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration()
                .setEnableReadAhead(true)
                .setReadAheadMaxRecords(1000)
                .setReadAheadBatchSize(10)
                .setLockTimeout(1)
                .setNumWorkerThreads(1)
                .setReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis(20)
                .setSchedulerShutdownTimeoutMs(0)
                .setDLLedgerMetadataLayoutVersion(LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    protected ZooKeeper zkc;
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    protected static int zkPort;
    protected static int numBookies = 3;
    protected static final List<File> TMP_DIRS = new ArrayList<File>();

    @BeforeClass
    public static void setupCluster() throws Exception {
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
        TMP_DIRS.add(zkTmpDir);
        Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir);
        zks = serverAndPort.getLeft();
        zkPort = serverAndPort.getRight();
        bkutil = LocalDLMEmulator.newBuilder()
                .numBookies(numBookies)
                .zkHost("127.0.0.1")
                .zkPort(zkPort)
                .serverConf(DLMTestUtil.loadTestBkConf())
                .shouldStartZK(false)
                .build();
        bkutil.start();
        zkServers = "127.0.0.1:" + zkPort;
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.warn("Uncaught exception at Thread {} : ", t.getName(), e);
            }
        });
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
        for (File dir : TMP_DIRS) {
            FileUtils.forceDeleteOnExit(dir);
        }
    }

    @Before
    public void setup() throws Exception {
        try {
            zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", zkPort);
        } catch (Exception ex) {
            LOG.error("hit exception connecting to zookeeper at {}:{}", "127.0.0.1", zkPort, ex);
            throw ex;
        }
    }

    @After
    public void teardown() throws Exception {
        if (null != zkc) {
            zkc.close();
        }
    }

    protected LogRecord waitForNextRecord(LogReader reader) throws Exception {
        LogRecord record = reader.readNext(false);
        while (null == record) {
            record = reader.readNext(false);
        }
        return record;
    }

    public URI createDLMURI(String path) throws Exception {
        return DLMTestUtil.createDLMURI(zkPort, path);
    }

    protected void ensureURICreated(URI uri) throws Exception {
        ensureURICreated(zkc, uri);
    }

    protected void ensureURICreated(ZooKeeper zkc, URI uri) throws Exception {
        try {
            zkc.create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            // ignore
        }
    }

    public BKDistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                                String name) throws Exception {
        return createNewDLM(conf, name, PermitLimiter.NULL_PERMIT_LIMITER);
    }

    public BKDistributedLogManager createNewDLM(DistributedLogConfiguration conf,
                                                String name,
                                                PermitLimiter writeLimiter)
            throws Exception {
        URI uri = createDLMURI("/" + name);
        ensureURICreated(uri);
        final Namespace namespace = NamespaceBuilder.newBuilder()
                .uri(uri)
                .conf(conf)
                .build();
        final OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(1)
                .name("test-scheduler")
                .build();
        AsyncCloseable resourcesCloseable = new AsyncCloseable() {
            @Override
            public CompletableFuture<Void> asyncClose() {
                LOG.info("Shutting down the scheduler");
                SchedulerUtils.shutdownScheduler(scheduler, 1, TimeUnit.SECONDS);
                LOG.info("Shut down the scheduler");
                LOG.info("Closing the namespace");
                namespace.close();
                LOG.info("Closed the namespace");
                return FutureUtils.Void();
            }
        };
        AsyncFailureInjector failureInjector = AsyncRandomFailureInjector.newBuilder()
                .injectDelays(conf.getEIInjectReadAheadDelay(),
                        conf.getEIInjectReadAheadDelayPercent(),
                        conf.getEIInjectMaxReadAheadDelayMs())
                .injectErrors(false, 10)
                .injectStops(conf.getEIInjectReadAheadStall(), 10)
                .injectCorruption(conf.getEIInjectReadAheadBrokenEntries())
                .build();
        return new BKDistributedLogManager(
                name,
                conf,
                ConfUtils.getConstDynConf(conf),
                uri,
                namespace.getNamespaceDriver(),
                new LogSegmentMetadataCache(conf, Ticker.systemTicker()),
                scheduler,
                DistributedLogConstants.UNKNOWN_CLIENT_ID,
                DistributedLogConstants.LOCAL_REGION_ID,
                writeLimiter,
                new SettableFeatureProvider("", 0),
                failureInjector,
                NullStatsLogger.INSTANCE,
                NullStatsLogger.INSTANCE,
                Optional.of(resourcesCloseable));
    }

    protected LogSegmentMetadataStore getLogSegmentMetadataStore(Namespace namespace)
            throws IOException {
        return namespace.getNamespaceDriver().getLogStreamMetadataStore(NamespaceDriver.Role.READER)
                .getLogSegmentMetadataStore();
    }

    protected ZooKeeperClient getZooKeeperClient(Namespace namespace) throws Exception {
        NamespaceDriver driver = namespace.getNamespaceDriver();
        assertTrue(driver instanceof BKNamespaceDriver);
        return ((BKNamespaceDriver) driver).getWriterZKC();
    }

    @SuppressWarnings("deprecation")
    protected BookKeeperClient getBookKeeperClient(Namespace namespace) throws Exception {
        NamespaceDriver driver = namespace.getNamespaceDriver();
        assertTrue(driver instanceof BKNamespaceDriver);
        return ((BKNamespaceDriver) driver).getReaderBKC();
    }

    protected LedgerHandle getLedgerHandle(BKLogSegmentWriter segmentWriter) {
        LogSegmentEntryWriter entryWriter = segmentWriter.getEntryWriter();
        assertTrue(entryWriter instanceof BKLogSegmentEntryWriter);
        return ((BKLogSegmentEntryWriter) entryWriter).getLedgerHandle();
    }
}
