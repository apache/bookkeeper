/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.test;

import java.io.IOException;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.bookie.BookieException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import junit.framework.TestCase;

@RunWith(Parameterized.class)
public abstract class BaseTestCase extends TestCase {
    static final Logger LOG = LoggerFactory.getLogger(BaseTestCase.class);
    // ZooKeeper related variables
    protected static final String HOSTPORT = "127.0.0.1:2181";
    protected static Integer ZooKeeperDefaultPort = 2181;
    protected ZooKeeperServer zks;
    protected ZooKeeper zkc; // zookeeper client
    protected NIOServerCnxnFactory serverFactory;
    protected File ZkTmpDir;

    // BookKeeper
    protected List<File> tmpDirs = new ArrayList<File>();
    protected List<BookieServer> bs = new ArrayList<BookieServer>();
    protected List<ServerConfiguration> bsConfs = new ArrayList<ServerConfiguration>();
    protected Integer initialPort = 5000;
    protected int numBookies;
    protected BookKeeperTestClient bkc;

    protected ServerConfiguration baseConf = new ServerConfiguration();
    protected ClientConfiguration baseClientConf = new ClientConfiguration();

    public BaseTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { {DigestType.MAC }, {DigestType.CRC32}});
    }

    protected ServerConfiguration newServerConfiguration(int port, String zkServers, File journalDir, File[] ledgerDirs) {
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        conf.setZkServers(zkServers);
        conf.setJournalDirName(journalDir.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i=0; i<ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        return conf;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        try {
            // create a ZooKeeper server(dataDir, dataLogDir, port)
            LOG.debug("Running ZK server");
            // ServerStats.registerAsConcrete();
            ClientBase.setupTestEnv();
            ZkTmpDir = File.createTempFile("zookeeper", "test");
            ZkTmpDir.delete();
            ZkTmpDir.mkdir();

            zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
            serverFactory = new NIOServerCnxnFactory();
            serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), 100);
            serverFactory.startup(zks);

            boolean b = ClientBase.waitForServerUp(HOSTPORT, ClientBase.CONNECTION_TIMEOUT);

            LOG.debug("Server up: " + b);

            // create a zookeeper client
            LOG.debug("Instantiate ZK Client");
            zkc = new ZooKeeper("127.0.0.1", ZooKeeperDefaultPort, new emptyWatcher());

            // initialize the zk client with values
            zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            baseClientConf.setZkServers("127.0.0.1");
            if (numBookies > 0) {
                bkc = new BookKeeperTestClient(baseClientConf);
            }

            // Create Bookie Servers (B1, B2, B3)
            for (int i = 0; i < numBookies; i++) {
                File f = File.createTempFile("bookie", "test");
                tmpDirs.add(f);
                f.delete();
                f.mkdir();

                ServerConfiguration conf = newServerConfiguration(
                    initialPort + i, HOSTPORT, f, new File[] { f });
                bsConfs.add(conf);

                bs.add(startBookie(conf));
            }
        } catch(Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }
    }

    public void killBookie(InetSocketAddress addr) throws InterruptedException {
        BookieServer toRemove = null;
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.shutdown();
                toRemove = server;
            }
        }
        if (toRemove != null) {
            bs.remove(toRemove);
        }
    }

    public void sleepBookie(InetSocketAddress addr, final int seconds,
                            final CountDownLatch l)
            throws InterruptedException, IOException {
        final String name = "Bookie-" + addr.getPort();
        Thread[] allthreads = new Thread[Thread.activeCount()];
        Thread.enumerate(allthreads);
        for (final Thread t : allthreads) {
            if (t.getName().equals(name)) {
                Thread sleeper = new Thread() {
                    public void run() {
                        try {
                            t.suspend();
                            l.countDown();
                            Thread.sleep(seconds*1000);
                            t.resume();
                        } catch (Exception e) {
                            LOG.error("Error suspending thread", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("Bookie thread not found");
    }

    /**
     * Restart bookie servers
     *
     * @throws InterruptedException
     * @throws IOException
     */
    protected void restartBookies() 
            throws InterruptedException, IOException, KeeperException, BookieException {
        restartBookies(null);
    }

    /**
     * Restart bookie servers add new configuration settings
     */
    protected void restartBookies(ServerConfiguration newConf)
            throws InterruptedException, IOException, KeeperException, BookieException {
        // shut down bookie server
        for (BookieServer server : bs) {
            server.shutdown();
        }
        bs.clear();
        Thread.sleep(1000);
        // restart them to ensure we can't 
        int j = 0;
        for (File f : tmpDirs) {
            ServerConfiguration conf = bsConfs.get(j);
            if (null != newConf) {
                conf.loadConf(newConf);
            }
            bs.add(startBookie(conf));
            j++;
        }
    }

    /**
     * Helper method to startup a new bookie server with the indicated port
     * number
     *
     * @param port
     *            Port to start the new bookie server on
     * @throws IOException
     */
    protected void startNewBookie(int port)
            throws IOException, InterruptedException, KeeperException, BookieException {
        File f = File.createTempFile("bookie", "test");
        tmpDirs.add(f);
        f.delete();
        f.mkdir();

        ServerConfiguration conf = newServerConfiguration(port, HOSTPORT, f, new File[] { f });

        bs.add(startBookie(conf));
    }

    /**
     * Helper method to startup a bookie server using a configuration object
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    private BookieServer startBookie(ServerConfiguration conf)
            throws IOException, InterruptedException, KeeperException, BookieException {
        BookieServer server = new BookieServer(conf);
        server.start();

        int port = conf.getBookiePort();
        while(bkc.getZkHandle().exists("/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port, false) == null) {
            Thread.sleep(500);
        }

        bkc.readBookiesBlocking();
        LOG.info("New bookie on port " + port + " has been created.");
        return server;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        LOG.info("TearDown");

        if (bkc != null) {
            bkc.close();;
        }

        for (BookieServer server : bs) {
            server.shutdown();
        }

        if (zkc != null) {
            zkc.close();
        }

        for (File f : tmpDirs) {
            cleanUpDir(f);
        }

        // shutdown ZK server
        if (serverFactory != null) {
            serverFactory.shutdown();
            assertTrue("waiting for server down", ClientBase.waitForServerDown(HOSTPORT, ClientBase.CONNECTION_TIMEOUT));
        }
        // ServerStats.unregister();
        cleanUpDir(ZkTmpDir);


    }

    /* Clean up a directory recursively */
    protected boolean cleanUpDir(File dir) {
        if (dir.isDirectory()) {
            LOG.info("Cleaning up " + dir.getName());
            String[] children = dir.list();
            for (String string : children) {
                boolean success = cleanUpDir(new File(dir, string));
                if (!success)
                    return false;
            }
        }
        // The directory is now empty so delete it
        return dir.delete();
    }

    /* User for testing purposes, void */
    class emptyWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }

}
