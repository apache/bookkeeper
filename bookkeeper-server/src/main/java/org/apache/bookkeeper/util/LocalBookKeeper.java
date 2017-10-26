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
package org.apache.bookkeeper.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShimFactory;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.tls.SecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import static com.google.common.base.Charsets.UTF_8;

public class LocalBookKeeper {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalBookKeeper.class);
    public static final int CONNECTION_TIMEOUT = 30000;

    int numberOfBookies;

    public LocalBookKeeper() {
        this(3);
    }

    public LocalBookKeeper(int numberOfBookies) {
        this(numberOfBookies, 5000);
    }

    public LocalBookKeeper(int numberOfBookies, int initialPort) {
        this.numberOfBookies = numberOfBookies;
        this.initialPort = initialPort;
        LOG.info("Running {} bookie(s) on zkServer {}.", this.numberOfBookies);
    }

    static String ZooKeeperDefaultHost = "127.0.0.1";
    static int ZooKeeperDefaultPort = 2181;
    static int zkSessionTimeOut = 5000;
    static Integer BookieDefaultInitialPort = 5000;

    //BookKeeper variables
    File journalDirs[];
    BookieServer bs[];
    ServerConfiguration bsConfs[];
    Integer initialPort = 5000;

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

    private void initializeZookeeper(String zkHost, int zkPort) throws IOException {
        LOG.info("Instantiate ZK Client");
        //initialize the zk client with values
        ZooKeeperClient zkc = null;
        try {
            zkc = ZooKeeperClient.newBuilder()
                    .connectString(zkHost + ":" + zkPort)
                    .sessionTimeoutMs(zkSessionTimeOut)
                    .build();
            zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // No need to create an entry for each requested bookie anymore as the
            // BookieServers will register themselves with ZooKeeper on startup.
        } catch (KeeperException e) {
            LOG.error("Exception while creating znodes", e);
            throw new IOException("Error creating znodes : ", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted while creating znodes", e);
            throw new IOException("Error creating znodes : ", e);
        }
    }

    private static void cleanupDirectories(List<File> dirs) throws IOException {
        for (File dir : dirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private List<File> runBookies(ServerConfiguration baseConf, String dirSuffix)
            throws IOException, KeeperException, InterruptedException, BookieException,
            UnavailableException, CompatibilityException, SecurityException, BKException {
        List<File> tempDirs = new ArrayList<File>();
        try {
            runBookies(baseConf, tempDirs, dirSuffix);
            return tempDirs;
        } catch (IOException ioe) {
            cleanupDirectories(tempDirs);
            throw ioe;
        } catch (KeeperException ke) {
            cleanupDirectories(tempDirs);
            throw ke;
        } catch (InterruptedException ie) {
            cleanupDirectories(tempDirs);
            throw ie;
        } catch (BookieException be) {
            cleanupDirectories(tempDirs);
            throw be;
        } catch (UnavailableException ue) {
            cleanupDirectories(tempDirs);
            throw ue;
        } catch (CompatibilityException ce) {
            cleanupDirectories(tempDirs);
            throw ce;
        }
    }

    @SuppressWarnings("deprecation")
    private void runBookies(ServerConfiguration baseConf, List<File> tempDirs, String dirSuffix)
            throws IOException, KeeperException, InterruptedException, BookieException, UnavailableException,
            CompatibilityException, SecurityException, BKException {
        LOG.info("Starting Bookie(s)");
        // Create Bookie Servers (B1, B2, B3)

        journalDirs = new File[numberOfBookies];
        bs = new BookieServer[numberOfBookies];
        bsConfs = new ServerConfiguration[numberOfBookies];

        for(int i = 0; i < numberOfBookies; i++) {
            if (null == baseConf.getJournalDirNameWithoutDefault()) {
                journalDirs[i] = IOUtils.createTempDir("localbookkeeper" + Integer.toString(i), dirSuffix);
                tempDirs.add(journalDirs[i]);
            } else {
                journalDirs[i] = new File(baseConf.getJournalDirName(), "bookie" + Integer.toString(i));
            }
            if (journalDirs[i].exists()) {
                if (journalDirs[i].isDirectory()) {
                    FileUtils.deleteDirectory(journalDirs[i]);
                } else if (!journalDirs[i].delete()) {
                    throw new IOException("Couldn't cleanup bookie journal dir " + journalDirs[i]);
                }
            }
            if (!journalDirs[i].mkdirs()) {
                throw new IOException("Couldn't create bookie journal dir " + journalDirs[i]);
            }

            String [] ledgerDirs = baseConf.getLedgerDirWithoutDefault();
            if ((null == ledgerDirs) || (0 == ledgerDirs.length)) {
                ledgerDirs = new String[] { journalDirs[i].getPath() };
            } else {
                for (int l = 0; l < ledgerDirs.length; l++) {
                    File dir = new File(ledgerDirs[l], "bookie" + Integer.toString(i));
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
                    ledgerDirs[l] = dir.getPath();
                }
            }

            bsConfs[i] = new ServerConfiguration(baseConf);

            // If the caller specified ephemeral ports then use ephemeral ports for all
            // the bookies else use numBookie ports starting at initialPort
            if (0 == initialPort) {
                bsConfs[i].setBookiePort(0);
            } else {
                bsConfs[i].setBookiePort(initialPort + i);
            }

            if (null == baseConf.getZkServers()) {
                bsConfs[i].setZkServers(InetAddress.getLocalHost().getHostAddress() + ":"
                                  + ZooKeeperDefaultPort);
            }


            bsConfs[i].setJournalDirName(journalDirs[i].getPath());
            bsConfs[i].setLedgerDirNames(ledgerDirs);

            bs[i] = new BookieServer(bsConfs[i]);
            bs[i].start();
        }
    }

    public static void startLocalBookies(String zkHost,
                                         int zkPort,
                                         int numBookies,
                                         boolean shouldStartZK,
                                         int initialBookiePort)
            throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        startLocalBookiesInternal(conf, zkHost, zkPort, numBookies, shouldStartZK, initialBookiePort, true, "test");
    }

    public static void startLocalBookies(String zkHost,
                                         int zkPort,
                                         int numBookies,
                                         boolean shouldStartZK,
                                         int initialBookiePort,
                                         ServerConfiguration conf)
            throws Exception {
        startLocalBookiesInternal(conf, zkHost, zkPort, numBookies, shouldStartZK, initialBookiePort, true, "test");
    }

    public static void startLocalBookies(String zkHost,
                                         int zkPort,
                                         int numBookies,
                                         boolean shouldStartZK,
                                         int initialBookiePort,
                                         String dirSuffix)
            throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        startLocalBookiesInternal(conf, zkHost, zkPort, numBookies, shouldStartZK, initialBookiePort, true, dirSuffix);
    }

    static void startLocalBookiesInternal(ServerConfiguration conf,
                                          String zkHost,
                                          int zkPort,
                                          int numBookies,
                                          boolean shouldStartZK,
                                          int initialBookiePort,
                                          boolean stopOnExit,
                                          String dirSuffix)
            throws Exception {
        LocalBookKeeper lb = new LocalBookKeeper(numBookies, initialBookiePort);

        ZooKeeperServerShim zks = null;
        File zkTmpDir = null;
        List<File> bkTmpDirs = null;
        try {
            if (shouldStartZK) {
                zkTmpDir = IOUtils.createTempDir("zookeeper", dirSuffix);
                zks = LocalBookKeeper.runZookeeper(1000, zkPort, zkTmpDir);
            }

            lb.initializeZookeeper(zkHost, zkPort);
            conf.setZkServers(zkHost + ":" + zkPort);
            bkTmpDirs = lb.runBookies(conf, dirSuffix);

            try {
                while (true) {
                    Thread.sleep(5000);
                }
            } catch (InterruptedException ie) {
                if (stopOnExit) {
                    lb.shutdownBookies();

                    if (null != zks) {
                        zks.stop();
                    }
                }
                throw ie;
            }
        } catch (Exception e) {
            LOG.error("Failed to run {} bookies : zk ensemble = '{}:{}'",
                new Object[] { numBookies, zkHost, zkPort, e });
            throw e;
        } finally {
            if (stopOnExit) {
                cleanupDirectories(bkTmpDirs);
                if (null != zkTmpDir) {
                    FileUtils.deleteDirectory(zkTmpDir);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception, SecurityException {
        if(args.length < 1) {
            usage();
            System.exit(-1);
        }

        int numBookies = Integer.parseInt(args[0]);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowLoopback(true);
        if (args.length >= 2) {
            String confFile = args[1];
            try {
                conf.loadConf(new File(confFile).toURI().toURL());
                LOG.info("Using configuration file " + confFile);
            } catch (Exception e) {
                // load conf failed
                LOG.warn("Error loading configuration file " + confFile, e);
            }
        }

        startLocalBookiesInternal(conf, ZooKeeperDefaultHost, ZooKeeperDefaultPort,
                numBookies, true, BookieDefaultInitialPort, false, "test");
    }

    private static void usage() {
        System.err.println("Usage: LocalBookKeeper number-of-bookies");
    }

    public static boolean waitForServerUp(String hp, long timeout) {
        long start = MathUtils.now();
        String split[] = hp.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try {
                Socket sock = new Socket(host, port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes(UTF_8));
                    outstream.flush();

                    reader =
                        new BufferedReader(
                                new InputStreamReader(sock.getInputStream(), UTF_8));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        LOG.info("Server UP");
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                LOG.info("server " + hp + " not up " + e);
            }

            if (MathUtils.now() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    public void shutdownBookies() {
        for (BookieServer bookieServer: bs) {
            bookieServer.shutdown();
        }
    }

}
