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
package org.apache.bookkeeper.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.UncleanShutdownDetection;
import org.apache.bookkeeper.bookie.UncleanShutdownDetectionImpl;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorWithOomHandler;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.BookieServiceInfo.Endpoint;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShimFactory;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local Bookkeeper.
 */
public class LocalBookKeeper implements AutoCloseable {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalBookKeeper.class);
    public static final int CONNECTION_TIMEOUT = 30000;

    private static String newMetadataServiceUri(String zkServers, int port, String layout, String ledgerPath) {
        return "zk+" + layout + "://" + zkServers + ":" + port + ledgerPath;
    }

    int numberOfBookies;

    public LocalBookKeeper(
            int numberOfBookies,
            ServerConfiguration baseConf,
            String localBookiesConfigDirName,
            boolean stopOnExit, String dirSuffix,
            String zkHost, int zkPort) {
        this.numberOfBookies = numberOfBookies;
        this.localBookiesConfigDir = new File(localBookiesConfigDirName);
        this.baseConf = baseConf;
        this.localBookies = new ArrayList<>();
        this.stopOnExit = stopOnExit;
        this.dirSuffix = dirSuffix;
        this.zkHost = zkHost;
        this.zkPort = zkPort;
        this.dirsToCleanUp = new ArrayList<>();
        LOG.info("Running {} bookie(s) on zk ensemble = '{}:{}'.", this.numberOfBookies,
                zooKeeperDefaultHost, zooKeeperDefaultPort);
    }

    private static String zooKeeperDefaultHost = "127.0.0.1";
    private static int zooKeeperDefaultPort = 2181;
    private static int zkSessionTimeOut = 5000;
    private static String defaultLocalBookiesConfigDir = "/tmp/localbookies-config";

    //BookKeeper variables
    List<LocalBookie> localBookies;
    ZooKeeperServerShim zks;
    String zkHost;
    int zkPort;
    String dirSuffix;
    ByteBufAllocatorWithOomHandler allocator;
    private ServerConfiguration baseConf;
    File localBookiesConfigDir;
    List<File> dirsToCleanUp;
    boolean stopOnExit;

    /**
     * @param maxCC
     *          Max Concurrency of Client
     * @param zookeeperPort
     *          ZooKeeper Server Port
     */
    public static ZooKeeperServerShim runZookeeper(int maxCC, int zookeeperPort) throws IOException {
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "localbookkeeper");
        return runZookeeper(maxCC, zookeeperPort, zkTmpDir);
    }

    public static ZooKeeperServerShim runZookeeper(int maxCC, int zookeeperPort, File zkDir) throws IOException {
        LOG.info("Starting ZK server");
        ZooKeeperServerShim server = ZooKeeperServerShimFactory.createServer(zkDir, zkDir, zookeeperPort, maxCC);
        server.start();

        boolean b = waitForServerUp(InetAddress.getLoopbackAddress().getHostAddress() + ":" + zookeeperPort,
          CONNECTION_TIMEOUT);
        if (LOG.isDebugEnabled()) {
            LOG.debug("ZooKeeper server up: {}", b);
        }
        return server;
    }

    private void initializeZookeeper() throws IOException {
        LOG.info("Instantiate ZK Client");
        //initialize the zk client with values
        try (ZooKeeperClient zkc = ZooKeeperClient.newBuilder()
                    .connectString(zkHost + ":" + zkPort)
                    .sessionTimeoutMs(zkSessionTimeOut)
                    .build()) {
            String zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf);
            ZkUtils.createFullPathOptimistic(zkc, zkLedgersRootPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            List<Op> multiOps = Lists.newArrayListWithExpectedSize(2);
            multiOps.add(
                Op.create(zkLedgersRootPath + "/" + AVAILABLE_NODE,
                    new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            multiOps.add(
                Op.create(zkLedgersRootPath + "/" + AVAILABLE_NODE + "/" + READONLY,
                    new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            zkc.multi(multiOps);
            // No need to create an entry for each requested bookie anymore as the
            // BookieServers will register themselves with ZooKeeper on startup.
        } catch (KeeperException e) {
            LOG.error("Exception while creating znodes", e);
            throw new IOException("Error creating znodes : ", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while creating znodes", e);
            throw new IOException("Error creating znodes : ", e);
        }
    }

    private static void cleanupDirectories(List<File> dirs) throws IOException {
        for (File dir : dirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private void runBookies()
            throws Exception {
        LOG.info("Starting Bookie(s)");
        // Create Bookie Servers (B1, B2, B3)

        if (localBookiesConfigDir.exists() && localBookiesConfigDir.isFile()) {
            throw new IOException("Unable to create LocalBookiesConfigDir, since there is a file at "
                    + localBookiesConfigDir.getAbsolutePath());
        }
        if (!localBookiesConfigDir.exists() && !localBookiesConfigDir.mkdirs()) {
            throw new IOException(
                    "Unable to create LocalBookiesConfigDir - " + localBookiesConfigDir.getAbsolutePath());
        }
        allocator = BookieResources.createAllocator(baseConf);
        for (int i = 0; i < numberOfBookies; i++) {
            runBookie(i);
        }

        /*
         * baseconf.conf is needed because for executing any BookieShell command
         * of Metadata/Zookeeper Operation nature we need a valid conf file
         * having correct zk details and this could be used for running any such
         * bookieshell commands if bookieid is not provided as parameter to
         * bookkeeper shell operation. for eg:
         * "./bookkeeper shell localbookie listbookies -rw". But for execution
         * shell command of bookie Operation nature we need to provide bookieid,
         * for eg "./bookkeeper shell -localbookie 10.3.27.190:5000 lastmark",
         * so this shell command would use '10.3.27.190:5000.conf' file
         */
        ServerConfiguration baseConfWithCorrectZKServers = new ServerConfiguration(
                (ServerConfiguration) baseConf.clone());
        if (null == baseConf.getMetadataServiceUriUnchecked()) {
            baseConfWithCorrectZKServers.setMetadataServiceUri(baseConf.getMetadataServiceUri());
        }
        serializeLocalBookieConfig(baseConfWithCorrectZKServers, "baseconf.conf");
    }

    @SuppressWarnings("deprecation")
    private void runBookie(int bookieIndex) throws Exception {
        File journalDirs;
        if (null == baseConf.getJournalDirNameWithoutDefault()) {
            journalDirs = IOUtils.createTempDir("localbookkeeper" + bookieIndex, dirSuffix);
            dirsToCleanUp.add(journalDirs);
        } else {
            journalDirs = new File(baseConf.getJournalDirName(), "bookie" + bookieIndex);
        }
        if (journalDirs.exists()) {
            if (journalDirs.isDirectory()) {
                FileUtils.deleteDirectory(journalDirs);
            } else if (!journalDirs.delete()) {
                throw new IOException("Couldn't cleanup bookie journal dir " + journalDirs);
            }
        }
        if (!journalDirs.mkdirs()) {
            throw new IOException("Couldn't create bookie journal dir " + journalDirs);
        }

        String[] ledgerDirs = baseConf.getLedgerDirWithoutDefault();
        if ((null == ledgerDirs) || (0 == ledgerDirs.length)) {
            ledgerDirs = new String[]{journalDirs.getPath()};
        } else {
            for (int l = 0; l < ledgerDirs.length; l++) {
                File dir = new File(ledgerDirs[l], "bookie" + bookieIndex);
                if (dir.exists()) {
                    if (dir.isDirectory()) {
                        FileUtils.deleteDirectory(dir);
                    } else if (!dir.delete()) {
                        throw new IOException("Couldn't cleanup bookie ledger dir " + dir);
                    }
                }
                if (!dir.mkdirs()) {
                    throw new IOException("Couldn't create bookie ledger dir " + dir);
                }
                dirsToCleanUp.add(dir);
                ledgerDirs[l] = dir.getPath();
            }
        }
        ServerConfiguration conf = new ServerConfiguration((ServerConfiguration) baseConf.clone());

        conf.setBookiePort(PortManager.nextFreePort());

        if (null == baseConf.getMetadataServiceUriUnchecked()) {
            conf.setMetadataServiceUri(baseConf.getMetadataServiceUri());
        }

        conf.setJournalDirName(journalDirs.getPath());
        conf.setLedgerDirNames(ledgerDirs);

        // write config into file before start so we can know what's wrong if start failed
        String fileName = BookieImpl.getBookieId(conf).toString() + ".conf";
        serializeLocalBookieConfig(conf, fileName);

        LocalBookie b = new LocalBookie(conf);
        b.start();
        localBookies.add(b);
    }

    private void setZooKeeperShim(ZooKeeperServerShim zks, File zkTmpDir) {
        this.zks = zks;
        this.dirsToCleanUp.add(zkTmpDir);
    }

    public static LocalBookKeeper getLocalBookies(String zkHost,
                                                     int zkPort,
                                                     int numBookies,
                                                     boolean shouldStartZK,
                                                     ServerConfiguration conf) throws Exception {
        return getLocalBookiesInternal(
                conf, zkHost, zkPort, numBookies, shouldStartZK,
                true, "test", null, defaultLocalBookiesConfigDir);
    }

    @SuppressWarnings("deprecation")
    private static LocalBookKeeper getLocalBookiesInternal(ServerConfiguration conf,
                                                             String zkHost,
                                                             int zkPort,
                                                             int numBookies,
                                                             boolean shouldStartZK,
                                                             boolean stopOnExit,
                                                             String dirSuffix,
                                                             String zkDataDir,
                                                             String localBookiesConfigDirName) throws Exception {
        conf.setMetadataServiceUri(
                newMetadataServiceUri(
                        zkHost,
                        zkPort,
                        conf.getLedgerManagerLayoutStringFromFactoryClass(),
                        conf.getZkLedgersRootPath()));
        LocalBookKeeper lb = new LocalBookKeeper(numBookies, conf, localBookiesConfigDirName, stopOnExit,
                dirSuffix, zkHost, zkPort);
        if (shouldStartZK) {
            File zkDataDirFile = null;
            if (zkDataDir != null) {
                zkDataDirFile = new File(zkDataDir);
                if (zkDataDirFile.exists() && zkDataDirFile.isFile()) {
                    throw new IOException("Unable to create zkDataDir, since there is a file at "
                            + zkDataDirFile.getAbsolutePath());
                }
                if (!zkDataDirFile.exists() && !zkDataDirFile.mkdirs()) {
                    throw new IOException("Unable to create zkDataDir - " + zkDataDirFile.getAbsolutePath());
                }
            }
            File zkTmpDir = IOUtils.createTempDir("zookeeper", dirSuffix, zkDataDirFile);
            lb.setZooKeeperShim(LocalBookKeeper.runZookeeper(1000, zkPort, zkTmpDir), zkTmpDir);
        }

        return lb;
    }

    /**
     * Serializes the config object to the specified file in localBookiesConfigDir.
     *
     * @param localBookieConfig
     *         config object which has to be serialized
     * @param fileName
     *         name of the file
     * @throws IOException
     */
    private void serializeLocalBookieConfig(ServerConfiguration localBookieConfig, String fileName) throws IOException {
        if (StringUtils.isBlank(fileName)
                || fileName.contains("..")
                || fileName.contains("/")
                || fileName.contains("\\")) {
            throw new IllegalArgumentException("Invalid filename: " + fileName);
        }

        File localBookieConfFile = new File(localBookiesConfigDir, fileName);
        if (localBookieConfFile.exists() && !localBookieConfFile.delete()) {
            throw new IOException(
                    "Unable to delete the existing LocalBookieConfigFile - " + localBookieConfFile.getAbsolutePath());
        }
        if (!localBookieConfFile.createNewFile()) {
            throw new IOException("Unable to create new File - " + localBookieConfFile.getAbsolutePath());
        }
        Iterator<String> keys = localBookieConfig.getKeys();
        try (PrintWriter writer = new PrintWriter(localBookieConfFile, "UTF-8")) {
            while (keys.hasNext()) {
                String key = keys.next();
                String[] values = localBookieConfig.getStringArray(key);
                StringBuilder concatenatedValue = new StringBuilder(values[0]);
                for (int i = 1; i < values.length; i++) {
                    concatenatedValue.append(",").append(values[i]);
                }
                writer.println(key + "=" + concatenatedValue);
            }
        }
    }

    public static void main(String[] args) {
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        try {
            if (args.length < 1) {
                usage();
                System.exit(-1);
            }

            int numBookies = 0;
            try {
                numBookies = Integer.parseInt(args[0]);
            } catch (NumberFormatException nfe) {
                LOG.error("Unrecognized number-of-bookies: {}", args[0]);
                usage();
                System.exit(-1);
            }

            ServerConfiguration conf = new ServerConfiguration();
            conf.setAllowLoopback(true);
            if (args.length >= 2) {
                String confFile = args[1];
                try {
                    conf.loadConf(new File(confFile).toURI().toURL());
                    LOG.info("Using configuration file {}", confFile);
                } catch (Exception e) {
                    // load conf failed
                    LOG.warn("Error loading configuration file {}", confFile, e);
                }
            }

            String zkDataDir = null;
            if (args.length >= 3) {
                zkDataDir = args[2];
            }

            String localBookiesConfigDirName = defaultLocalBookiesConfigDir;
            if (args.length >= 4) {
                localBookiesConfigDirName = args[3];
            }

            try (LocalBookKeeper lb = getLocalBookiesInternal(conf, zooKeeperDefaultHost, zooKeeperDefaultPort,
                    numBookies, true, false, "test", zkDataDir,
                    localBookiesConfigDirName)) {
                try {
                    lb.start();
                    while (true) {
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        } catch (Exception e) {
            LOG.error("Exiting LocalBookKeeper because of exception in main method", e);
            /*
             * This is needed because, some non-daemon thread (probably in ZK or
             * some other dependent service) is preventing the JVM from exiting, though
             * there is exception in main thread.
             */
            System.exit(-1);
        }
    }

    private static void usage() {
        System.err.println(
                "Usage: LocalBookKeeper number-of-bookies [path to bookie config] "
                + "[path to create ZK data directory at] [path to LocalBookiesConfigDir]");
    }

    public static boolean waitForServerUp(String hp, long timeout) {
        long start = System.currentTimeMillis();
        String[] split = hp.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try (Socket sock = new Socket(host, port);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream(), UTF_8))) {
                OutputStream outstream = sock.getOutputStream();
                outstream.write("stat".getBytes(UTF_8));
                outstream.flush();

                String line = reader.readLine();
                if (line != null && line.startsWith("Zookeeper version:")) {
                    LOG.info("Server UP");
                    return true;
                }
            } catch (IOException e) {
                // ignore as this is expected
                LOG.info("server " + hp + " not up " + e);
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // ignore
            }
        }
        return false;
    }

    public void start() throws Exception {
        initializeZookeeper();
        runBookies();
    }

    public void addBookie() throws Exception {
        int bookieIndex = localBookies.size() + 1;
        runBookie(bookieIndex);
    }

    public void removeBookie() throws Exception {
        int index = localBookies.size() - 1;
        LocalBookie bookie = localBookies.get(index);
        bookie.shutdown();
        localBookies.remove(index);
    }

    public void shutdownBookies() throws Exception {
        for (LocalBookie b : localBookies) {
            b.shutdown();
        }
    }

    @Override
    public void close() throws Exception {
        if (stopOnExit) {
            shutdownBookies();

            if (null != zks) {
                zks.stop();
            }
        }

        cleanupDirectories(dirsToCleanUp);
    }

    private class LocalBookie {
        final BookieServer server;
        final Bookie bookie;
        final MetadataBookieDriver metadataDriver;
        final RegistrationManager registrationManager;
        final LedgerManagerFactory lmFactory;
        final LedgerManager ledgerManager;

        LocalBookie(ServerConfiguration conf) throws Exception {
            metadataDriver = BookieResources.createMetadataDriver(conf, NullStatsLogger.INSTANCE);
            registrationManager = metadataDriver.createRegistrationManager();
            lmFactory = metadataDriver.getLedgerManagerFactory();
            ledgerManager = lmFactory.newLedgerManager();

            DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
            LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                    conf, diskChecker, NullStatsLogger.INSTANCE);
            LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                    conf, diskChecker, NullStatsLogger.INSTANCE, ledgerDirsManager);
            LedgerStorage storage = BookieResources.createLedgerStorage(
                    conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                    NullStatsLogger.INSTANCE, allocator);
            UncleanShutdownDetection shutdownManager = new UncleanShutdownDetectionImpl(ledgerDirsManager);

            final ComponentInfoPublisher componentInfoPublisher = new ComponentInfoPublisher();
            final Supplier<BookieServiceInfo> bookieServiceInfoProvider =
                    () -> buildBookieServiceInfo(componentInfoPublisher);

            componentInfoPublisher.startupFinished();
            bookie = new BookieImpl(conf, registrationManager, storage, diskChecker,
                                    ledgerDirsManager, indexDirsManager,
                                    NullStatsLogger.INSTANCE, allocator, bookieServiceInfoProvider);
            server = new BookieServer(conf, bookie, NullStatsLogger.INSTANCE, allocator,
                                      shutdownManager);
        }

        void start() throws Exception {
            server.start();
        }

        void shutdown() throws Exception {
            server.shutdown();
            ledgerManager.close();
            lmFactory.close();
            registrationManager.close();
            metadataDriver.close();
        }
    }

    /**
     * Create the {@link BookieServiceInfo} starting from the published endpoints.
     *
     * @see ComponentInfoPublisher
     * @param componentInfoPublisher the endpoint publisher
     * @return the created bookie service info
     */
    private static BookieServiceInfo buildBookieServiceInfo(ComponentInfoPublisher componentInfoPublisher) {
        List<Endpoint> endpoints = componentInfoPublisher.getEndpoints().values()
                .stream().map(e -> {
                    return new Endpoint(
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
