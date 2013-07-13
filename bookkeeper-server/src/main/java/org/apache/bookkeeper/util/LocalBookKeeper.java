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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

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

    private final String HOSTPORT = "127.0.0.1:2181";
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
        ZkTmpDir = File.createTempFile("zookeeper", "test");
        if (!ZkTmpDir.delete() || !ZkTmpDir.mkdir()) {
            throw new IOException("Couldn't create zk directory " + ZkTmpDir);
        }

        try {
            zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
            serverFactory =  new NIOServerCnxnFactory();
            serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), maxCC);
            serverFactory.startup(zks);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Exception while instantiating ZooKeeper", e);
        }

        boolean b = waitForServerUp(HOSTPORT, CONNECTION_TIMEOUT);
        LOG.debug("ZooKeeper server up: {}", b);
    }

    private void initializeZookeper() throws IOException {
        LOG.info("Instantiate ZK Client");
        //initialize the zk client with values
        try {
            ZKConnectionWatcher zkConnectionWatcher = new ZKConnectionWatcher();
            zkc = new ZooKeeper(HOSTPORT, zkSessionTimeOut,
                    zkConnectionWatcher);
            zkConnectionWatcher.waitForConnection();
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

    private void runBookies(ServerConfiguration baseConf) throws IOException,
            KeeperException, InterruptedException, BookieException,
            UnavailableException, CompatibilityException {
        LOG.info("Starting Bookie(s)");
        // Create Bookie Servers (B1, B2, B3)

        tmpDirs = new File[numberOfBookies];
        bs = new BookieServer[numberOfBookies];
        bsConfs = new ServerConfiguration[numberOfBookies];

        for(int i = 0; i < numberOfBookies; i++) {
            tmpDirs[i] = File.createTempFile("bookie" + Integer.toString(i), "test");
            if (!tmpDirs[i].delete() || !tmpDirs[i].mkdir()) {
                throw new IOException("Couldn't create bookie dir " + tmpDirs[i]);
            }

            bsConfs[i] = new ServerConfiguration(baseConf);
            // override settings
            bsConfs[i].setBookiePort(initialPort + i);
            bsConfs[i].setZkServers(InetAddress.getLocalHost().getHostAddress() + ":"
                                  + ZooKeeperDefaultPort);
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
        lb.initializeZookeper();
        lb.runBookies(conf);
        while (true) {
            Thread.sleep(5000);
        }
    }

    private static void usage() {
        System.err.println("Usage: LocalBookKeeper number-of-bookies");
    }

    /* Watching SyncConnected event from ZooKeeper */
    static class ZKConnectionWatcher implements Watcher {
        private CountDownLatch clientConnectLatch = new CountDownLatch(1);

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
                clientConnectLatch.countDown();
            }
        }

        // Waiting for the SyncConnected event from the ZooKeeper server
        public void waitForConnection() throws IOException {
            try {
                if (!clientConnectLatch.await(zkSessionTimeOut,
                        TimeUnit.MILLISECONDS)) {
                    throw new IOException(
                            "Couldn't connect to zookeeper server");
                }
            } catch (InterruptedException e) {
                throw new IOException(
                        "Interrupted when connecting to zookeeper server", e);
            }
        }
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
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader =
                        new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
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
