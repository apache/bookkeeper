/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.cluster;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.NamespaceExistsException;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.StorageServer;
import org.apache.bookkeeper.stream.storage.api.controller.StorageController;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.sc.helix.HelixStorageController;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * A Cluster that runs a few storage nodes.
 */
@Slf4j
public class StreamCluster
    extends AbstractLifecycleComponent<StorageConfiguration> {

    private static final int MAX_RETRIES = 20;

    /**
     * Build a stream cluster from the provided cluster {@code spec}.
     *
     * @param spec the spec to build stream cluster.
     * @return stream cluster spec.
     */
    public static StreamCluster build(StreamClusterSpec spec) {
        return new StreamCluster(spec);
    }

    //
    // DL Settings
    //
    private static final String ROOT_PATH = "/stream";
    private static final String LEDGERS_PATH = "/stream/ledgers";
    private static final String LEDGERS_AVAILABLE_PATH = "/stream/ledgers/available";
    private static final String NAMESPACE = "/stream/storage";

    private final StreamClusterSpec spec;
    private final List<Endpoint> rpcEndpoints;
    private CompositeConfiguration baseConf;
    private String zkEnsemble;
    private int zkPort;
    private ZooKeeperServerShim zks;
    private List<LifecycleComponent> servers;
    private int nextBookiePort;
    private int nextGrpcPort;

    private StreamCluster(StreamClusterSpec spec) {
        super(
            "stream-cluster",
            new StorageConfiguration(spec.baseConf()),
            NullStatsLogger.INSTANCE);
        this.spec = spec;
        this.servers = Lists.newArrayListWithExpectedSize(spec.numServers());
        this.rpcEndpoints = Lists.newArrayListWithExpectedSize(spec.numServers());
        this.nextBookiePort = spec.initialBookiePort();
        this.nextGrpcPort = spec.initialGrpcPort();
    }

    public List<Endpoint> getRpcEndpoints() {
        return rpcEndpoints;
    }

    public String getZkServers() {
        return zkEnsemble;
    }

    private void startZooKeeper() throws Exception {
        if (!spec.shouldStartZooKeeper()) {
            zkPort = spec.zkPort();
            zkEnsemble = spec.zkServers() + ":" + zkPort;
            return;
        }

        File zkDir = new File(spec.storageRootDir, "zookeeper");
        Pair<ZooKeeperServerShim, Integer> zkServerAndPort =
            LocalDLMEmulator.runZookeeperOnAnyPort(zkDir);
        zks = zkServerAndPort.getLeft();
        zkPort = zkServerAndPort.getRight();
        log.info("Started zookeeper at port {}.", zkPort);
        zkEnsemble = "127.0.0.1:" + zkPort;
    }

    private void stopZooKeeper() {
        // stop the zookeeper server
        if (null != zks) {
            zks.stop();
        }
    }

    private void initializeCluster() throws Exception {
        log.info("Initializing the stream cluster.");
        ZooKeeper zkc = null;
        try (StorageController controller = new HelixStorageController(zkEnsemble)) {
            // initialize the configuration
            ServerConfiguration serverConf = new ServerConfiguration();
            serverConf.setMetadataServiceUri("zk://" + zkEnsemble + LEDGERS_PATH);
            serverConf.setAllowLoopback(true);
            serverConf.setGcWaitTime(300000);
            serverConf.setDiskUsageWarnThreshold(0.9999f);
            serverConf.setDiskUsageThreshold(0.999999f);
            this.baseConf = serverConf;

            zkc = ZooKeeperClient.newBuilder()
                .connectString(zkEnsemble)
                .sessionTimeoutMs(60000)
                .build();
            Transaction txn = zkc.transaction();
            txn.create(
                ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            txn.create(
                LEDGERS_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            txn.create(
                LEDGERS_AVAILABLE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            try {
                txn.commit();
            } catch (KeeperException ke) {
                if (Code.NODEEXISTS != ke.code()) {
                    throw ke;
                }
            }

            log.info("Initialize the bookkeeper metadata.");

            // initialize the storage
            controller.createCluster("stream/helix", spec.numServers() * 2, 1);
            log.info("Initialized the helix metadata with {} storage containers.", spec.numServers() * 2);
        } finally {
            if (null != zkc) {
                zkc.close();
            }
        }
    }

    private LifecycleComponent startServer() throws Exception {
        int bookiePort;
        int grpcPort;
        boolean success = false;
        int retries = 0;

        while (!success) {
            synchronized (this) {
                bookiePort = nextBookiePort++;
                grpcPort = nextGrpcPort++;
            }
            LifecycleComponent server = null;
            try {
                ServerConfiguration serverConf = new ServerConfiguration();
                serverConf.loadConf(baseConf);
                serverConf.setBookiePort(bookiePort);
                File bkDir = new File(spec.storageRootDir(), "bookie_" + bookiePort);
                serverConf.setJournalDirName(bkDir.getPath());
                serverConf.setLedgerDirNames(new String[]{bkDir.getPath()});

                DistributedLogConfiguration dlConf = new DistributedLogConfiguration();
                dlConf.loadConf(serverConf);

                File rangesStoreDir = new File(spec.storageRootDir(), "ranges_" + grpcPort);
                StorageConfiguration storageConf = new StorageConfiguration(serverConf);
                storageConf.setRangeStoreDirNames(new String[]{rangesStoreDir.getPath()});
                storageConf.setServeReadOnlyTables(spec.serveReadOnlyTable);

                log.info("Attempting to start storage server at (bookie port = {}, grpc port = {})"
                        + " : bkDir = {}, rangesStoreDir = {}, serveReadOnlyTables = {}",
                    bookiePort, grpcPort, bkDir, rangesStoreDir, spec.serveReadOnlyTable);
                server = StorageServer.startStorageServer(
                    serverConf,
                    grpcPort,
                    spec.numServers() * 2,
                    Optional.empty());
                server.start();
                log.info("Started storage server at (bookie port = {}, grpc port = {})",
                    bookiePort, grpcPort);
                this.rpcEndpoints.add(StorageServer.createLocalEndpoint(grpcPort, false));
                return server;
            } catch (Throwable e) {
                log.error("Failed to start storage server", e);
                if (null != server) {
                    server.stop();
                }
                if (e.getCause() instanceof BindException) {
                    retries++;
                    if (retries > MAX_RETRIES) {
                        throw (BindException) e.getCause();
                    }
                } else {
                    throw e;
                }
            }
        }
        throw new IOException("Failed to start any storage server.");
    }

    private void startServers() throws Exception {
        log.info("Starting {} storage servers.", spec.numServers());
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<LifecycleComponent>> startFutures = Lists.newArrayList();
        for (int i = 0; i < spec.numServers(); i++) {
            Future<LifecycleComponent> future = executor.submit(() -> startServer());
            startFutures.add(future);
        }
        for (Future<LifecycleComponent> future : startFutures) {
            servers.add(future.get());
        }
        log.info("Started {} storage servers.", spec.numServers());
        executor.shutdown();
    }

    private void createDefaultNamespaces() throws Exception {
        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .addEndpoints(getRpcEndpoints().toArray(new Endpoint[getRpcEndpoints().size()]))
            .usePlaintext(true)
            .build();
        log.info("RpcEndpoints are : {}", settings.endpoints());
        String namespaceName = "default";
        try (StorageAdminClient admin = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .buildAdmin()) {

            System.out.println("Creating namespace '" + namespaceName + "' ...");
            try {
                NamespaceProperties nsProps = result(
                    admin.createNamespace(
                        namespaceName,
                        NamespaceConfiguration.newBuilder()
                            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
                            .build()));
                System.out.println("Successfully created namespace '" + namespaceName + "':");
                System.out.println(nsProps);
            } catch (NamespaceExistsException nee) {
                System.out.println("Namespace '" + namespaceName + "' already exists.");
            }
        }
    }

    private void stopServers() {
        for (LifecycleComponent server : servers) {
            server.close();
        }
    }

    @Override
    protected void doStart() {
        try {
            // start zookeeper servers
            startZooKeeper();

            // initialize the cluster
            initializeCluster();

            // stop servers
            startServers();

            // create default namespaces
            createDefaultNamespaces();

            // wait for 10 seconds
            TimeUnit.SECONDS.sleep(10);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        // stop the servers
        stopServers();
        // stop zookeeper
        stopZooKeeper();
    }
}
