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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import static com.google.common.base.Charsets.UTF_8;
import org.apache.zookeeper.data.ACL;

public class LocalBookKeeper {
    protected static final Logger LOG = LoggerFactory.getLogger(LocalBookKeeper.class);
    public static final int CONNECTION_TIMEOUT = 30000;

    int numberOfBookies;

    public LocalBookKeeper() {
        numberOfBookies = 3;
    }

    public LocalBookKeeper(int numberOfBookies) {
        this();
        this.numberOfBookies = numberOfBookies;
        LOG.info("Running " + this.numberOfBookies + " bookie(s).");
    }

    NIOServerCnxnFactory serverFactory;
    ZooKeeperServer zks;
    ZooKeeper zkc;
    int ZooKeeperDefaultPort = 2181;
    static int zkSessionTimeOut = 5000;
    File ZkTmpDir;

    //BookKeeper variables
    File tmpDirs[];
    BookieServer bs[];
    ServerConfiguration bsConfs[];
    Integer initialPort = 5000;

    /**
     * @param args
     */

    private void runZookeeper(int maxCC) throws IOException {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.info("Starting ZK server");
        //ServerStats.registerAsConcrete();
        //ClientBase.setupTestEnv();
        ZkTmpDir = IOUtils.createTempDir("zookeeper", "localbookkeeper");

        InetAddress loopbackIP = InetAddress.getLoopbackAddress();
        try {
            zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
            serverFactory =  new NIOServerCnxnFactory();
            LOG.info("Starting Zookeeper server at " + loopbackIP.getHostAddress() + " port:" + ZooKeeperDefaultPort);
            serverFactory.configure(new InetSocketAddress(loopbackIP, ZooKeeperDefaultPort), maxCC);
            serverFactory.startup(zks);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Exception while instantiating ZooKeeper", e);
        }

        boolean b = waitForServerUp(loopbackIP.getHostAddress() + ":" + ZooKeeperDefaultPort, CONNECTION_TIMEOUT);
        LOG.debug("ZooKeeper server up: {}", b);
    }

    private void initializeZookeeper() throws IOException {
        LOG.info("Instantiate ZK Client");
        //initialize the zk client with values
        try {
            zkc = ZooKeeperClient.newBuilder()
                    .connectString(InetAddress.getLoopbackAddress().getHostAddress() + ":" + ZooKeeperDefaultPort)
                    .sessionTimeoutMs(zkSessionTimeOut)
                    .build();
            zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // No need to create an entry for each requested bookie anymore as the
            // BookieServers will register themselves with ZooKeeper on startup.
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            LOG.error("Exception while creating znodes", e);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            LOG.error("Interrupted while creating znodes", e);
        }
    }

    private static void cleanupDirectories(List<File> dirs) throws IOException {
        for (File dir : dirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    private List<File> runBookies(ServerConfiguration baseConf, String dirSuffix)
            throws IOException, KeeperException, InterruptedException, BookieException,
            UnavailableException, CompatibilityException {
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

    private void runBookies(ServerConfiguration baseConf, List<File> tempDirs, String dirSuffix)
            throws IOException, KeeperException, InterruptedException, BookieException,
            UnavailableException, CompatibilityException {
        LOG.info("Starting Bookie(s)");
        // Create Bookie Servers (B1, B2, B3)

        tmpDirs = new File[numberOfBookies];
        bs = new BookieServer[numberOfBookies];
        bsConfs = new ServerConfiguration[numberOfBookies];

        String loopbackIPAddr = InetAddress.getLoopbackAddress().getHostAddress();
        for(int i = 0; i < numberOfBookies; i++) {
            tmpDirs[i] = File.createTempFile("bookie" + Integer.toString(i), "test");
            if (!tmpDirs[i].delete() || !tmpDirs[i].mkdir()) {
                throw new IOException("Couldn't create bookie dir " + tmpDirs[i]);
            }

            bsConfs[i] = new ServerConfiguration(baseConf);
            // override settings
            bsConfs[i].setBookiePort(initialPort + i);
            LOG.info("Connecting to Zookeeper at " + loopbackIPAddr + " port:" + ZooKeeperDefaultPort);
            bsConfs[i].setZkServers(loopbackIPAddr + ":" + ZooKeeperDefaultPort);
            bsConfs[i].setJournalDirName(tmpDirs[i].getPath());
            bsConfs[i].setLedgerDirNames(new String[] { tmpDirs[i].getPath() });
            bsConfs[i].setAllowLoopback(true);

            bs[i] = new BookieServer(bsConfs[i]);
            bs[i].start();
        }
    }

    public static void main(String[] args) throws IOException, KeeperException,
            InterruptedException, BookieException, UnavailableException,
            CompatibilityException {
        if(args.length < 1) {
            usage();
            System.exit(-1);
        }
        LocalBookKeeper lb = new LocalBookKeeper(Integer.parseInt(args[0]));

        ServerConfiguration conf = new ServerConfiguration();
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

        lb.runZookeeper(1000);
        lb.initializeZookeeper();
        List<File> tmpDirs = lb.runBookies(conf, "test");
        try {
            while (true) {
                Thread.sleep(5000);
            }
        } catch (InterruptedException ie) {
            cleanupDirectories(tmpDirs);
            throw ie;
        }
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

}
