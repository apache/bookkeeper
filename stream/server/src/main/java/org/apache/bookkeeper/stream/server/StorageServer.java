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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stream.server.conf.DLConfiguration;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.server.grpc.GrpcServerSpec;
import org.apache.bookkeeper.stream.server.service.BookieService;
import org.apache.bookkeeper.stream.server.service.DLNamespaceProviderService;
import org.apache.bookkeeper.stream.server.service.GrpcService;
import org.apache.bookkeeper.stream.server.service.StatsProviderService;
import org.apache.bookkeeper.stream.server.service.StorageService;
import org.apache.bookkeeper.stream.storage.RangeStoreBuilder;
import org.apache.bookkeeper.stream.storage.StorageResources;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.sc.helix.HelixStorageContainerManager;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactoryImpl;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * A storage server is a server that run storage service and serving rpc requests.
 */
@Slf4j
public class StorageServer {

    private static class ServerArguments {

        @Parameter(names = {"-c", "--conf"}, description = "Configuration file for storage server")
        private String serverConfigFile;

        @Parameter(names = {"-p", "--port"}, description = "Port to listen on for gPRC server")
        private int port = 3182;

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
            storageServer = startStorageServer(
                conf,
                grpcPort,
                1024,
                Optional.empty());
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

    public static LifecycleComponent startStorageServer(CompositeConfiguration conf,
                                                        int grpcPort,
                                                        int numStorageContainers,
                                                        Optional<String> instanceName)
        throws ConfigurationException, UnknownHostException {
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
        StatsProviderService statsProviderService = new StatsProviderService(bkConf);
        StatsLogger rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");

        // Create the bookie service
        BookieService bookieService = new BookieService(bkConf, rootStatsLogger);

        // Create the distributedlog namespace service
        DLNamespaceProviderService dlNamespaceProvider = new DLNamespaceProviderService(
            bookieService.serverConf(),
            dlConf,
            rootStatsLogger.scope("dl"));

        // Create range (stream) store
        RangeStoreBuilder rangeStoreBuilder = RangeStoreBuilder.newBuilder()
            .withStatsLogger(rootStatsLogger.scope("storage"))
            .withStorageConfiguration(storageConf)
            // the storage resources shared across multiple components
            .withStorageResources(storageResources)
            // the number of storage containers
            .withNumStorageContainers(numStorageContainers)
            // the default log backend uri
            .withDefaultBackendUri(dlNamespaceProvider.getDlogUri())
            // with the storage container manager (currently it is helix)
            .withStorageContainerManagerFactory((ignored, storeConf, registry) ->
                new HelixStorageContainerManager(
                    ZKMetadataDriverBase.resolveZkServers(bookieService.serverConf()),
                    "stream/helix",
                    storeConf,
                    registry,
                    myEndpoint,
                    instanceName,
                    rootStatsLogger.scope("helix")))
            // with the inter storage container client manager
            .withRangeStoreFactory(
                new MVCCStoreFactoryImpl(
                    dlNamespaceProvider,
                    storageConf.getRangeStoreDirs(),
                    storageResources,
                    storageConf.getServeReadOnlyTables()));
        StorageService storageService = new StorageService(
            storageConf, rangeStoreBuilder, rootStatsLogger.scope("storage"));

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


        // Create all the service stack
        return LifecycleComponentStack.newBuilder()
            .withName("storage-server")
            .addComponent(statsProviderService)     // stats provider
            .addComponent(bookieService)            // bookie server
            .addComponent(dlNamespaceProvider)      // service that provides dl namespace
            .addComponent(storageService)           // range (stream) store
            .addComponent(grpcService)              // range (stream) server (gRPC)
            .build();
    }

}
