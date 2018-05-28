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
package org.apache.bookkeeper.stream.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.stream.storage.StorageConstants.ZK_METADATA_ROOT_PATH;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.dlog.DLCheckpointStore;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stream.server.conf.DLConfiguration;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.server.grpc.GrpcServerSpec;
import org.apache.bookkeeper.stream.server.service.BookieService;
import org.apache.bookkeeper.stream.server.service.BookieWatchService;
import org.apache.bookkeeper.stream.server.service.ClusterControllerService;
import org.apache.bookkeeper.stream.server.service.CuratorProviderService;
import org.apache.bookkeeper.stream.server.service.DLNamespaceProviderService;
import org.apache.bookkeeper.stream.server.service.GrpcService;
import org.apache.bookkeeper.stream.server.service.RegistrationServiceProvider;
import org.apache.bookkeeper.stream.server.service.RegistrationStateService;
import org.apache.bookkeeper.stream.server.service.StatsProviderService;
import org.apache.bookkeeper.stream.server.service.StorageService;
import org.apache.bookkeeper.stream.storage.StorageContainerStoreBuilder;
import org.apache.bookkeeper.stream.storage.StorageResources;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.cluster.ClusterControllerImpl;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterControllerLeaderSelector;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerController;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.sc.ZkStorageContainerManager;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactoryImpl;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.distributedlog.DistributedLogConfiguration;

/**
 * A storage server is a server that run storage service and serving rpc requests.
 */
@Slf4j
public class StorageServer {

    private static class ServerArguments {

        @Parameter(names = {"-c", "--conf"}, description = "Configuration file for storage server")
        private String serverConfigFile;

        @Parameter(names = {"-p", "--port"}, description = "Port to listen on for gPRC server")
        private int port = 4181;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

    }

    private static void loadConfFile(CompositeConfiguration conf, String confFile)
        throws IllegalArgumentException {
        try {
            Configuration loadedConf = new PropertiesConfiguration(
                new File(confFile).toURI().toURL());
            conf.addConfiguration(loadedConf);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file {}", confFile, e);
            throw new IllegalArgumentException("Could not open configuration file " + confFile, e);
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file {}", confFile, e);
            throw new IllegalArgumentException("Malformed configuration file " + confFile, e);
        }
        log.info("Loaded configuration file {}", confFile);
    }

    public static Endpoint createLocalEndpoint(int port, boolean useHostname) throws UnknownHostException {
        String hostname;
        if (useHostname) {
            hostname = InetAddress.getLocalHost().getHostName();
        } else {
            hostname = InetAddress.getLocalHost().getHostAddress();
        }
        return Endpoint.newBuilder()
            .setHostname(hostname)
            .setPort(port)
            .build();
    }

    public static void main(String[] args) {
        int retCode = doMain(args);
        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) {
        // register thread uncaught exception handler
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) ->
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage()));

        // parse the commandline
        ServerArguments arguments = new ServerArguments();
        JCommander jCommander = new JCommander(arguments);
        jCommander.setProgramName("StorageServer");
        jCommander.parse(args);

        if (arguments.help) {
            jCommander.usage();
            return ExitCode.INVALID_CONF.code();
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        if (null != arguments.serverConfigFile) {
            loadConfFile(conf, arguments.serverConfigFile);
        }

        int grpcPort = arguments.port;

        LifecycleComponent storageServer;
        try {
            storageServer = buildStorageServer(
                conf,
                grpcPort);
        } catch (ConfigurationException e) {
            log.error("Invalid storage configuration", e);
            return ExitCode.INVALID_CONF.code();
        } catch (UnknownHostException e) {
            log.error("Unknonw host name", e);
            return ExitCode.UNKNOWN_HOSTNAME.code();
        }

        CompletableFuture<Void> liveFuture =
            ComponentStarter.startComponent(storageServer);
        try {
            liveFuture.get();
        } catch (InterruptedException e) {
            // the server is interrupted.
            Thread.currentThread().interrupt();
            log.info("Storage server is interrupted. Exiting ...");
        } catch (ExecutionException e) {
            log.info("Storage server is exiting ...");
        }
        return ExitCode.OK.code();
    }

    public static LifecycleComponent buildStorageServer(CompositeConfiguration conf,
                                                        int grpcPort)
            throws UnknownHostException, ConfigurationException {
        return buildStorageServer(conf, grpcPort, true, NullStatsLogger.INSTANCE);
    }

    public static LifecycleComponent buildStorageServer(CompositeConfiguration conf,
                                                        int grpcPort,
                                                        boolean startBookieAndStartProvider,
                                                        StatsLogger externalStatsLogger)
        throws ConfigurationException, UnknownHostException {

        LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack.newBuilder()
            .withName("storage-server");

        BookieConfiguration bkConf = BookieConfiguration.of(conf);
        bkConf.validate();

        DLConfiguration dlConf = DLConfiguration.of(conf);
        dlConf.validate();

        StorageServerConfiguration serverConf = StorageServerConfiguration.of(conf);
        serverConf.validate();

        StorageConfiguration storageConf = new StorageConfiguration(conf);
        storageConf.validate();

        // Get my local endpoint
        Endpoint myEndpoint = createLocalEndpoint(grpcPort, false);

        // Create shared resources
        StorageResources storageResources = StorageResources.create();

        // Create the stats provider
        StatsLogger rootStatsLogger;
        if (startBookieAndStartProvider) {
            StatsProviderService statsProviderService = new StatsProviderService(bkConf);
            rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");
            serverBuilder.addComponent(statsProviderService);
        } else {
            rootStatsLogger = checkNotNull(externalStatsLogger,
                "External stats logger is not provided while not starting stats provider");
        }

        // Create the bookie service
        ServerConfiguration bkServerConf;
        if (startBookieAndStartProvider) {
            BookieService bookieService = new BookieService(bkConf, rootStatsLogger);
            serverBuilder.addComponent(bookieService);
            bkServerConf = bookieService.serverConf();
        } else {
            bkServerConf = new ServerConfiguration();
            bkServerConf.loadConf(bkConf.getUnderlyingConf());
        }

        // Create the bookie watch service
        BookieWatchService bkWatchService;
        {
            DistributedLogConfiguration dlogConf = new DistributedLogConfiguration();
            bkWatchService = new BookieWatchService(
                dlogConf.getEnsembleSize(),
                bkConf,
                NullStatsLogger.INSTANCE);
        }

        // Create the curator provider service
        CuratorProviderService curatorProviderService = new CuratorProviderService(
            bkServerConf, dlConf, rootStatsLogger.scope("curator"));

        // Create the distributedlog namespace service
        DLNamespaceProviderService dlNamespaceProvider = new DLNamespaceProviderService(
            bkServerConf,
            dlConf,
            rootStatsLogger.scope("dlog"));

        // Create range (stream) store
        StorageContainerStoreBuilder storageContainerStoreBuilder = StorageContainerStoreBuilder.newBuilder()
            .withStatsLogger(rootStatsLogger.scope("storage"))
            .withStorageConfiguration(storageConf)
            // the storage resources shared across multiple components
            .withStorageResources(storageResources)
            // the placement policy
            .withStorageContainerPlacementPolicyFactory(() -> {
                long numStorageContainers;
                try (ZkClusterMetadataStore store = new ZkClusterMetadataStore(
                    curatorProviderService.get(),
                    ZKMetadataDriverBase.resolveZkServers(bkServerConf),
                    ZK_METADATA_ROOT_PATH)) {
                    numStorageContainers = store.getClusterMetadata().getNumStorageContainers();
                }
                return StorageContainerPlacementPolicyImpl.of((int) numStorageContainers);
            })
            // the default log backend uri
            .withDefaultBackendUri(dlNamespaceProvider.getDlogUri())
            // with zk-based storage container manager
            .withStorageContainerManagerFactory((storeConf, registry) ->
                new ZkStorageContainerManager(
                    myEndpoint,
                    storageConf,
                    new ZkClusterMetadataStore(
                        curatorProviderService.get(),
                        ZKMetadataDriverBase.resolveZkServers(bkServerConf),
                        ZK_METADATA_ROOT_PATH),
                    registry,
                    rootStatsLogger.scope("sc").scope("manager")))
            // with the inter storage container client manager
            .withRangeStoreFactory(
                new MVCCStoreFactoryImpl(
                    dlNamespaceProvider,
                    () -> new DLCheckpointStore(dlNamespaceProvider.get()),
                    storageConf.getRangeStoreDirs(),
                    storageResources,
                    storageConf.getServeReadOnlyTables()));
        StorageService storageService = new StorageService(
            storageConf, storageContainerStoreBuilder, rootStatsLogger.scope("storage"));

        // Create gRPC server
        StatsLogger rpcStatsLogger = rootStatsLogger.scope("grpc");
        GrpcServerSpec serverSpec = GrpcServerSpec.builder()
            .storeSupplier(storageService)
            .storeServerConf(serverConf)
            .endpoint(myEndpoint)
            .statsLogger(rpcStatsLogger)
            .build();
        GrpcService grpcService = new GrpcService(
            serverConf, serverSpec, rpcStatsLogger);

        // Create a registration service provider
        RegistrationServiceProvider regService = new RegistrationServiceProvider(
            bkServerConf,
            dlConf,
            rootStatsLogger.scope("registration").scope("provider"));

        // Create a registration state service only when service is ready.
        RegistrationStateService regStateService = new RegistrationStateService(
            myEndpoint,
            bkServerConf,
            bkConf,
            regService,
            rootStatsLogger.scope("registration"));

        // Create a cluster controller service
        ClusterControllerService clusterControllerService = new ClusterControllerService(
            storageConf,
            () -> new ClusterControllerImpl(
                new ZkClusterMetadataStore(
                    curatorProviderService.get(),
                    ZKMetadataDriverBase.resolveZkServers(bkServerConf),
                    ZK_METADATA_ROOT_PATH),
                regService.get(),
                new DefaultStorageContainerController(),
                new ZkClusterControllerLeaderSelector(curatorProviderService.get(), ZK_METADATA_ROOT_PATH),
                storageConf),
            rootStatsLogger.scope("cluster_controller"));

        // Create all the service stack
        return serverBuilder
            .addComponent(bkWatchService)           // service that watches bookies
            .addComponent(curatorProviderService)   // service that provides curator client
            .addComponent(dlNamespaceProvider)      // service that provides dl namespace
            .addComponent(storageService)           // range (stream) store
            .addComponent(grpcService)              // range (stream) server (gRPC)
            .addComponent(regService)               // service that provides registration client
            .addComponent(regStateService)          // service that manages server state
            .addComponent(clusterControllerService) // service that run cluster controller service
            .build();
    }

}
